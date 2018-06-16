package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"

	"github.com/go-sql-driver/mysql"
)

var (
	concurrency    = flag.Int("concurrency", 0, "Maximum number of concurrent load operations.")
	dataSourceName = flag.String("data-source-name", "", "Data source name for MySQL server to load data into.")
	dumpFile       = flag.String("dump-file", "", "MySQL dump file to load.")
	lowPriority    = flag.Bool("low-priority", false, "Use LOW_PRIORITY when loading data.")
	replaceTable   = flag.Bool("replace-table", false, "Load data into a temporary table and replace the old table with it once load is complete.")
)

func init() {
	flag.Lookup("concurrency").DefValue = "Number of available CPUs"
}

func main() {
	flag.Parse()

	if *concurrency == 0 {
		*concurrency = runtime.NumCPU()
	}

	if *dataSourceName == "" {
		*dataSourceName = os.Getenv("DATA_SOURCE_NAME")
	}

	db, err := sql.Open("mysql", *dataSourceName)
	if err != nil {
		log.Fatal(err)
	}

	r := os.Stdin
	if *dumpFile != "" {
		if r, err = os.Open(*dumpFile); err != nil {
			log.Fatal(err)
		}
	}

	var replacer *replacer
	if *replaceTable {
		replacer = newReplacer(db)
	}

	loader := newLoader(db, *concurrency, *lowPriority)
	scanner := newScanner(r)

	executor := &executor{db: db, loader: loader, scanner: scanner, replacer: replacer}
	if err := executor.execute(); err != nil {
		log.Fatal(err)
	}
}

type executor struct {
	db       *sql.DB
	loader   *loader
	replacer *replacer
	scanner  *scanner
}

func (e *executor) execute() error {
	var table *table
	var charset, database string

	conn, err := e.db.Conn(context.Background())
	if err != nil {
		return err
	}
	defer conn.Close()

	e.loader.start()

	for e.scanner.scan() {
		q := e.scanner.query()
		if e.replacer != nil && q.isDropTableStatement() {
			continue
		} else if e.replacer != nil && q.isCreateTableStatement() {
			if table != nil {
				if err := e.replacer.execute(context.Background(), database, table); err != nil {
					return err
				}
			}

			table, err = parseCreateTableStatement(q)
			if err != nil {
				return err
			}

			if err := createTable(context.Background(), conn, database, table); err != nil {
				return err
			}
		} else if q.isAlterTableStatement() || q.isLockTablesStatement() || q.isUnlockTablesStatement() {
			continue
		} else if q.isInsertStatement() || q.isReplaceStatement() {
			if err := e.loader.execute(context.Background(), q, charset, database, table); err != nil {
				return err
			}
		} else {
			if _, err := conn.ExecContext(context.Background(), q.s); err != nil {
				return err
			}
			if q.isSetNamesStatement() {
				if charset, err = parseSetNamesStatement(q); err != nil {
					return err
				}
			}
			if q.isUseStatement() {
				if database, err = parseUseStatement(q); err != nil {
					return err
				}
			}
		}
	}

	if e.replacer != nil && table != nil {
		if err := e.replacer.execute(context.Background(), database, table); err != nil {
			return err
		}
	}

	if err := e.scanner.err(); err != nil {
		return err
	}

	if err := e.loader.wait(); err != nil {
		return err
	}

	if e.replacer != nil {
		if err := e.replacer.wait(); err != nil {
			return err
		}
	}

	return nil
}

func parseCreateTableStatement(q *query) (*table, error) {
	var buf bytes.Buffer
	var foreignKeys []string

	origName, i, err := parseIdentifier(q.s, len("CREATE TABLE "), " ")
	if err != nil {
		return nil, fmt.Errorf("failed to parse table name. err=%s, line=%d", err, q.line)
	}
	i++

	if !strings.HasPrefix(q.s[i:], "(\n") {
		return nil, fmt.Errorf("unsupported CREATE TABLE statement. line=%d", q.line)
	}
	i += 2

	name := "_" + origName + "_tmp"

	buf.WriteString("(\n")
	scanner := &tableScanner{s: q.s[i:]}
	for scanner.scan() {
		d := scanner.definition()
		if isConstraintClause(d) {
			foreignKeys = append(foreignKeys, d)
		} else {
			if buf.Len() != 2 {
				buf.WriteString(",\n")
			}
			buf.WriteString("  ")
			buf.WriteString(d)
		}
	}
	if err := scanner.err(); err != nil {
		return nil, fmt.Errorf("failed to parse a table definition. err=%s, line=%d", err, q.line)
	}
	i += scanner.pos()

	buf.WriteByte('\n')
	buf.WriteString(q.s[i:])

	return &table{body: buf.String(), foreignKeys: foreignKeys, name: name, origName: origName}, nil
}

type table struct {
	body        string
	foreignKeys []string
	name        string
	origName    string
	wg          sync.WaitGroup
}

type tableScanner struct {
	d             string
	e             error
	p             int
	quote         byte
	s             string
	stringLiteral bool
}

func (s *tableScanner) scan() bool {
	i := s.p

	if !strings.HasPrefix(s.s[i:], "  ") {
		return false
	}
	i += 2

	for {
		j := strings.IndexAny(s.s[i:], "`\"'\\\n")
		if j == -1 {
			return false
		} else if s.quote == 0 && strings.IndexByte("`\"'", s.s[i+j]) != -1 {
			s.quote = s.s[i+j]
			s.stringLiteral = s.s[i+j] == '\''
			i += j + 1
		} else if s.quote != 0 && s.s[i+j] == s.quote {
			if !s.stringLiteral && len(s.s) > i+j+1 && s.s[i+j+1] == s.quote {
				i += j + 2
			} else {
				s.quote = 0
				s.stringLiteral = false
				i += j + 1
			}
		} else if s.stringLiteral && s.s[i+j] == '\\' {
			i += j + 2
		} else if s.quote == 0 && s.s[i+j] == '\n' {
			if len(s.s) > 1 && s.s[i+j-1] == ',' {
				s.d = s.s[s.p+2 : i+j-1]
			} else {
				s.d = s.s[s.p+2 : i+j]
			}
			s.p = i + j + 1
			return true
		} else {
			i += j + 1
		}
	}
}

func (s *tableScanner) err() error {
	return s.e
}

func (s *tableScanner) definition() string {
	return s.d
}

func (s *tableScanner) pos() int {
	return s.p
}

func isConstraintClause(d string) bool {
	return strings.HasPrefix(d, "CONSTRAINT ")
}

func parseSetNamesStatement(q *query) (charset string, err error) {
	if strings.HasPrefix(q.s, "/*!40101 SET NAMES ") {
		charset, _, err = parseIdentifier(q.s, len("/*!40101 SET NAMES "), " ")
	} else {
		charset, _, err = parseIdentifier(q.s, len(" SET NAMES "), " ")
	}
	return
}

func parseUseStatement(q *query) (database string, err error) {
	database, _, err = parseIdentifier(q.s, len("USE "), ";")
	return
}

func parseIdentifier(s string, i int, terms string) (string, int, error) {
	var buf bytes.Buffer
	if s[i] == '`' || s[i] == '"' {
		quote := s[i]
		i++
		for {
			j := strings.IndexByte(s[i:], quote)
			if j == -1 {
				return "", 0, fmt.Errorf("name is not enclosed by '%c'", quote)
			}
			buf.WriteString(s[i : i+j])
			i += j + 1
			if strings.IndexByte(terms, s[i]) != -1 {
				break
			} else if s[i] == quote {
				buf.WriteByte(quote)
			} else {
				return "", 0, fmt.Errorf("unexpected character '%c'", s[i])
			}
		}
	} else {
		j := strings.IndexAny(s[i:], terms)
		if j == -1 {
			return "", 0, errors.New("name is not terminated")
		} else {
			buf.WriteString(s[i : i+j])
			i += j
		}
	}
	return buf.String(), i, nil
}

func createTable(ctx context.Context, conn *sql.Conn, database string, table *table) error {
	var query bytes.Buffer
	query.WriteString("CREATE TABLE ")
	if database != "" {
		query.Write(quoteName(database))
		query.WriteByte('.')
	}
	query.Write(quoteName(table.name))
	query.WriteByte(' ')
	query.WriteString(table.body)
	_, err := conn.ExecContext(ctx, query.String())
	return err
}

type query struct {
	line int
	s    string
}

func (q *query) isAlterTableStatement() bool {
	return strings.HasPrefix(q.s, "/*!40000 ALTER TABLE ")
}

func (q *query) isCreateTableStatement() bool {
	return strings.HasPrefix(q.s, "CREATE TABLE ")
}

func (q *query) isDropTableStatement() bool {
	return strings.HasPrefix(q.s, "DROP TABLE ")
}

func (q *query) isInsertStatement() bool {
	return strings.HasPrefix(q.s, "INSERT ")
}

func (q *query) isLockTablesStatement() bool {
	return strings.HasPrefix(q.s, "LOCK TABLES ")
}

func (q *query) isReplaceStatement() bool {
	return strings.HasPrefix(q.s, "REPLACE ")
}

func (q *query) isSetNamesStatement() bool {
	return strings.HasPrefix(q.s, " SET NAMES ") || strings.HasPrefix(q.s, "/*!40101 SET NAMES ")
}

func (q *query) isUnlockTablesStatement() bool {
	return strings.HasPrefix(q.s, "UNLOCK TABLES ")
}

func (q *query) isUseStatement() bool {
	return strings.HasPrefix(q.s, "USE ")
}

type loader struct {
	ch          chan request
	concurrency int
	db          *sql.DB
	errCh       chan error
	lowPriority bool
	wg          sync.WaitGroup
}

func newLoader(db *sql.DB, concurrency int, lowPriority bool) *loader {
	return &loader{
		concurrency: concurrency,
		db:          db,
		lowPriority: lowPriority,
	}
}

func (l *loader) start() {
	l.ch = make(chan request, l.concurrency*2)
	l.errCh = make(chan error, l.concurrency)

	l.wg.Add(l.concurrency)

	for i := 0; i < l.concurrency; i++ {
		go func() {
			defer l.wg.Done()
			l.loop()
		}()
	}
}

func (l *loader) loop() {
	for r := range l.ch {
		if err := l.load(r.ctx, r.q, r.charset, r.database, r.table); err != nil {
			l.errCh <- err
			break
		}

		if r.table != nil {
			r.table.wg.Done()
		}
	}
}

func (l *loader) execute(ctx context.Context, q *query, charset, database string, table *table) error {
	select {
	case err := <-l.errCh:
		return err
	default:
	}

	if table != nil {
		table.wg.Add(1)
	}

	l.ch <- request{ctx: ctx, q: q, charset: charset, database: database, table: table}

	return nil
}

func (l *loader) load(ctx context.Context, q *query, charset, database string, table *table) error {
	i, err := convert(q)
	if err != nil {
		return err
	}

	var query bytes.Buffer
	query.WriteString("LOAD DATA ")
	if l.lowPriority {
		query.WriteString("LOW_PRIORITY ")
	}
	query.WriteString(fmt.Sprintf("LOCAL INFILE 'Reader::%d' ", q.line))
	if i.replace {
		query.WriteString("REPLACE ")
	} else if i.ignore {
		query.WriteString("IGNORE ")
	}
	query.WriteString("INTO TABLE ")
	if database != "" {
		query.Write(quoteName(database))
		query.WriteByte('.')
	}
	if table != nil {
		query.Write(quoteName(table.name))
	} else {
		query.Write(quoteName(i.table))
	}
	if charset != "" {
		query.WriteString(" CHARACTER SET ")
		query.WriteString(charset)
	}

	mysql.RegisterReaderHandler(strconv.Itoa(q.line), func() io.Reader { return i.r })
	defer mysql.DeregisterReaderHandler(strconv.Itoa(q.line))

	conn, err := l.db.Conn(context.Background())
	if err != nil {
		return err
	}
	defer conn.Close()

	if charset != "" {
		if err := setCharacterSet(ctx, conn, charset); err != nil {
			return err
		}
	}
	if err := disableForeignKeyChecks(ctx, conn); err != nil {
		return err
	}
	if _, err := conn.ExecContext(ctx, query.String()); err != nil {
		return err
	}

	return nil
}

func convert(q *query) (*insertion, error) {
	var replace, ignore bool
	var i int
	if strings.HasPrefix(q.s, "INSERT ") {
		i = len("INSERT ")
	} else if strings.HasPrefix(q.s, "REPLACE ") {
		replace = true
		i = len("REPLACE ")
	} else {
		return nil, fmt.Errorf("unsupported statement. line=%d", q.line)
	}

	if strings.HasPrefix(q.s[i:], "IGNORE ") {
		ignore = true
		i += len("IGNORE ")
	}

	if strings.HasPrefix(q.s[i:], "INTO ") {
		i += len("INTO ")
	} else {
		return nil, fmt.Errorf("unsupported statement. line=%d", q.line)
	}

	table, i, err := parseIdentifier(q.s, i, " ")
	if err != nil {
		return nil, fmt.Errorf("failed to parse table name. err=%s, line=%d", err, q.line)
	}
	i++

	if q.s[i] == '(' {
		i++
		for {
			_, i, err = parseIdentifier(q.s, i, ",)")
			if err != nil {
				return nil, fmt.Errorf("failed to parse column name. err=%s, line=%d", err, q.line)
			}
			if q.s[i] == ')' {
				i++
				break
			} else if strings.HasPrefix(q.s[i:], ", ") {
				i += 2
			} else {
				return nil, fmt.Errorf("no space character after ',' in a list of column names. line=%d", q.line)
			}
		}
		if q.s[i] != ' ' {
			return nil, fmt.Errorf("no space character after a list of colunm names. line=%d", q.line)
		}
		i++
	}

	if strings.HasPrefix(q.s[i:], "VALUES ") {
		i += len("VALUES ")
	} else {
		return nil, fmt.Errorf("unsupported statement. line=%d", q.line)
	}

	var buf bytes.Buffer
	for {
		for {
			if q.s[i] == '(' {
				i++
			}
			if q.s[i] == '\'' {
				i++
				for {
					// TODO: NO_BACKSLASH_ESCAPES
					j := strings.IndexAny(q.s[i:], "\\\t'")
					if j == -1 {
						return nil, fmt.Errorf("column value is not enclosed. line=%d", q.line)
					}
					buf.WriteString(q.s[i : i+j])
					i += j
					if q.s[i] == '\\' {
						buf.WriteString(q.s[i : i+2])
						i += 2
					} else if q.s[i] == '\t' {
						buf.WriteString(`\t`)
						i++
					} else if strings.IndexByte(",)", q.s[i+1]) != -1 {
						i++
						break
					} else {
						return nil, fmt.Errorf("unescaped single quote. line=%d", q.line)
					}
				}
			} else if strings.HasPrefix(q.s[i:], "0x") {
				j := strings.IndexAny(q.s[i+2:], ",)")
				if j == -1 {
					return nil, fmt.Errorf("hex blob is not terminated. line=%d", q.line)
				}
				if _, err := buf.ReadFrom(hex.NewDecoder(strings.NewReader(q.s[i+2 : i+2+j]))); err != nil {
					return nil, fmt.Errorf("failed to decode hex blob. err=%s, line=%d", err, q.line)
				}
				i += 2 + j
			} else {
				j := strings.IndexAny(q.s[i:], ",)")
				if j == -1 {
					return nil, fmt.Errorf("column value is not terminated. line=%d", q.line)
				}
				s := q.s[i : i+j]
				if s == "NULL" {
					buf.WriteString(`\N`)
				} else {
					buf.WriteString(s)
				}
				i += j
			}
			if q.s[i] == ',' {
				buf.WriteByte('\t')
				i++
			} else {
				buf.WriteByte('\n')
				i++
				break
			}
		}
		if q.s[i] == ',' {
			i++
		} else if q.s[i] == ';' {
			i++
			break
		} else {
			return nil, fmt.Errorf("unexpected character '%c'. line=%d", q.s[i], q.line)
		}
	}

	return &insertion{ignore: ignore, r: &buf, replace: replace, table: table}, nil
}

func quoteName(name string) []byte {
	var i int
	buf := make([]byte, len(name)*2+2)

	buf[i] = '`'
	i++
	for j := 0; j < len(name); j++ {
		if name[j] == '`' {
			buf[i] = '`'
			i++
		}
		buf[i] = name[j]
		i++
	}
	buf[i] = '`'
	i++

	return buf[:i]
}

func disableForeignKeyChecks(ctx context.Context, conn *sql.Conn) error {
	_, err := conn.ExecContext(ctx, "SET FOREIGN_KEY_CHECKS=0")
	return err
}

func setCharacterSet(ctx context.Context, conn *sql.Conn, charset string) error {
	_, err := conn.ExecContext(ctx, fmt.Sprintf("SET NAMES %s", charset))
	return err
}

func (l *loader) wait() error {
	close(l.ch)

	waitCh := make(chan struct{})
	go func() {
		defer close(waitCh)
		l.wg.Wait()
	}()

	select {
	case err := <-l.errCh:
		return err
	case <-waitCh:
		return nil
	}
}

type request struct {
	charset  string
	ctx      context.Context
	database string
	q        *query
	table    *table
}

type insertion struct {
	ignore  bool
	r       io.Reader
	replace bool
	table   string
}

type replacer struct {
	db    *sql.DB
	errCh chan error
	wg    sync.WaitGroup
}

func newReplacer(db *sql.DB) *replacer {
	return &replacer{db: db, errCh: make(chan error, 1)}
}

func (s *replacer) execute(ctx context.Context, database string, table *table) error {
	select {
	case err := <-s.errCh:
		return err
	default:
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		table.wg.Wait()
		if err := s.replace(ctx, database, table); err != nil {
			s.errCh <- err
		}
	}()
	return nil
}

func (s *replacer) replace(ctx context.Context, database string, table *table) error {
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	if err := disableForeignKeyChecks(ctx, conn); err != nil {
		return err
	}
	if err := dropTableIfExists(ctx, conn, database, table.origName); err != nil {
		return err
	}
	if err := renameTable(ctx, conn, database, table.name, table.origName); err != nil {
		return err
	}
	if len(table.foreignKeys) > 0 {
		if err := addForeignKeys(ctx, conn, database, table.origName, table.foreignKeys); err != nil {
			return err
		}
	}
	return nil
}

func dropTableIfExists(ctx context.Context, conn *sql.Conn, database, table string) error {
	var query bytes.Buffer
	query.WriteString("DROP TABLE IF EXISTS ")
	if database != "" {
		query.Write(quoteName(database))
		query.WriteByte('.')
	}
	query.Write(quoteName(table))
	_, err := conn.ExecContext(ctx, query.String())
	return err
}

func renameTable(ctx context.Context, conn *sql.Conn, database, old, new string) error {
	var query bytes.Buffer
	query.WriteString("RENAME TABLE ")
	if database != "" {
		query.Write(quoteName(database))
		query.WriteByte('.')
	}
	query.Write(quoteName(old))
	query.WriteString(" TO ")
	if database != "" {
		query.Write(quoteName(database))
		query.WriteByte('.')
	}
	query.Write(quoteName(new))
	_, err := conn.ExecContext(ctx, query.String())
	return err
}

func addForeignKeys(ctx context.Context, conn *sql.Conn, database, table string, foreignKeys []string) error {
	var query bytes.Buffer
	query.WriteString("ALTER TABLE ")
	if database != "" {
		query.Write(quoteName(database))
		query.WriteByte('.')
	}
	query.Write(quoteName(table))
	for i, fk := range foreignKeys {
		if i != 0 {
			query.WriteByte(',')
		}
		query.WriteString(" ADD ")
		query.WriteString(fk)
	}
	_, err := conn.ExecContext(ctx, query.String())
	return err
}

func (s *replacer) wait() error {
	waitCh := make(chan struct{})
	go func() {
		defer close(waitCh)
		s.wg.Wait()
	}()

	select {
	case err := <-s.errCh:
		return err
	case <-waitCh:
		return nil
	}
}
