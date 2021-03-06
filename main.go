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
	verbose        = flag.Bool("verbose", false, "Verbose mode.")
	mysqlVariables = make(mysqlVariableValue)
)

type mysqlVariableValue map[string]string

func (v *mysqlVariableValue) String() string {
	var buf bytes.Buffer
	for name, value := range *v {
		if buf.Len() != 0 {
			buf.WriteByte(',')
		}
		buf.WriteString(name)
		buf.WriteString("=")
		buf.WriteString(value)
	}
	return buf.String()
}

func (v *mysqlVariableValue) Set(value string) error {
	parts := strings.SplitN(value, "=", 2)
	if len(parts) != 2 {
		return errors.New("must be a name=value pair")
	}
	(*v)[parts[0]] = parts[1]
	return nil
}

func init() {
	flag.Var(&mysqlVariables, "mysql-variable", "MySQL variable (format: <name>=<value>)")
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

	clientFactory := func(ctx context.Context) (*client, error) {
		conn, err := db.Conn(ctx)
		if err != nil {
			return nil, fmt.Errorf("error getting database connection: %v", err)
		}

		c := client{conn: conn}

		mysqlVariables["foreign_key_checks"] = "0"
		if err := c.setVariables(ctx, mysqlVariables); err != nil {
			return nil, fmt.Errorf("error setting MySQL variables: %v", err)
		}

		return &c, nil
	}

	client, err := clientFactory(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	defer client.close()

	var replacer *replacer
	if *replaceTable {
		replacer = newReplacer(client)
	}

	loader := newLoader(clientFactory, *concurrency, *lowPriority)

	scanner := newScanner(r)

	executor := &executor{client: client, loader: loader, scanner: scanner, replacer: replacer}
	if err := executor.execute(); err != nil {
		log.Fatal(err)
	}
}

type executor struct {
	client   *client
	loader   *loader
	replacer *replacer
	scanner  *scanner
}

func (e *executor) execute() error {
	var table *table
	var charset, database string
	var err error

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
				return fmt.Errorf("error parsing CREATE TABLE statement on line %d: %v", q.line, err)
			}

			if *verbose {
				log.Printf("Creating new table %s...", quoteName(table.name))
			}

			if err := e.client.createTable(context.Background(), database, table.name, table.body); err != nil {
				return fmt.Errorf("error creating table %s: %v", quoteName(table.name), err)
			}
		} else if q.isAlterTableStatement() || q.isLockTablesStatement() || q.isUnlockTablesStatement() {
			continue
		} else if q.isInsertStatement() || q.isReplaceStatement() {
			if err := e.loader.execute(context.Background(), q, charset, database, table); err != nil {
				return err
			}
		} else {
			if err := e.client.exec(context.Background(), q.s); err != nil {
				return fmt.Errorf("error executing query on line %d: %v", q.line, err)
			}
			if q.isSetNamesStatement() {
				if charset, err = parseSetNamesStatement(q); err != nil {
					return fmt.Errorf("error parsing SET NAMES statement on line %d: %v", q.line, err)
				}
			}
			if q.isUseStatement() {
				if database, err = parseUseStatement(q); err != nil {
					return fmt.Errorf("error parsing USE statement on line %d: %v", q.line, err)
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
		return fmt.Errorf("error reading dump file: %v", err)
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
		return nil, fmt.Errorf("error parsing table name: %v", err)
	}
	i++

	if !strings.HasPrefix(q.s[i:], "(\n") {
		return nil, errors.New("unsupported CREATE TABLE statement")
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
		return nil, fmt.Errorf("error parsing table definition: %v", err)
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
	if strings.HasPrefix(q.s, "/*!") {
		// A version number after the ! character consists of exactly
		// 5 digits. See https://github.com/mysql/mysql-server/blob/7d10c82196c8e45554f27c00681474a9fb86d137/sql/sql_lex.cc#L1728-L1735.
		charset, _, err = parseIdentifier(q.s, len("/*!00000 SET NAMES "), " ")
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
	return strings.HasPrefix(q.s, " SET NAMES ") ||
		strings.HasPrefix(q.s, "/*!40101 SET NAMES ") ||
		strings.HasPrefix(q.s, "/*!50503 SET NAMES ")
}

func (q *query) isUnlockTablesStatement() bool {
	return q.s == "UNLOCK TABLES;"
}

func (q *query) isUseStatement() bool {
	return strings.HasPrefix(q.s, "USE ")
}

type loader struct {
	ch            chan request
	concurrency   int
	clientFactory func(ctx context.Context) (*client, error)
	errCh         chan error
	lowPriority   bool
	wg            sync.WaitGroup
}

func newLoader(clientFactory func(ctx context.Context) (*client, error), concurrency int, lowPriority bool) *loader {
	return &loader{
		clientFactory: clientFactory,
		concurrency:   concurrency,
		lowPriority:   lowPriority,
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
	client, err := l.clientFactory(context.Background())
	if err != nil {
		l.errCh <- err
		return
	}
	defer client.close()

	for r := range l.ch {
		if err := l.load(client, r.ctx, r.q, r.charset, r.database, r.table); err != nil {
			l.errCh <- fmt.Errorf("error loading data on line %d: %v", r.q.line, err)
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

func (l *loader) load(client *client, ctx context.Context, q *query, charset, database string, table *table) error {
	i, err := convert(q)
	if err != nil {
		return fmt.Errorf("error converting query: %v", err)
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

	if charset != "" {
		if err := client.setCharacterSet(ctx, charset); err != nil {
			return fmt.Errorf("error setting character set: %v", err)
		}
	}

	if err := client.exec(ctx, query.String()); err != nil {
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
		return nil, errors.New("unsupported statement")
	}

	if strings.HasPrefix(q.s[i:], "IGNORE ") {
		ignore = true
		i += len("IGNORE ")
	}

	if strings.HasPrefix(q.s[i:], "INTO ") {
		i += len("INTO ")
	} else {
		return nil, errors.New("unsupported statement")
	}

	table, i, err := parseIdentifier(q.s, i, " ")
	if err != nil {
		return nil, fmt.Errorf("error parsing table name: %v", err)
	}
	i++

	if q.s[i] == '(' {
		i++
		for {
			_, i, err = parseIdentifier(q.s, i, ",)")
			if err != nil {
				return nil, fmt.Errorf("error parsing column name: %v", err)
			}
			if q.s[i] == ')' {
				i++
				break
			} else if strings.HasPrefix(q.s[i:], ", ") {
				i += 2
			} else {
				return nil, errors.New("no space character after ',' in a list of column names")
			}
		}
		if q.s[i] != ' ' {
			return nil, errors.New("no space character after a list of column names")
		}
		i++
	}

	if strings.HasPrefix(q.s[i:], "VALUES ") {
		i += len("VALUES ")
	} else {
		return nil, errors.New("unsupported statement")
	}

	var buf bytes.Buffer
	for {
		for {
			if q.s[i] == '(' {
				i++
			}
			if strings.HasPrefix(q.s[i:], "_binary ") {
				i += len("_binary ")
			}
			if q.s[i] == '\'' {
				i++
				for {
					// TODO: NO_BACKSLASH_ESCAPES
					j := strings.IndexAny(q.s[i:], "\\\t'")
					if j == -1 {
						return nil, errors.New("column value is not enclosed")
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
						return nil, errors.New("unescaped single quote")
					}
				}
			} else if strings.HasPrefix(q.s[i:], "0x") {
				j := strings.IndexAny(q.s[i+2:], ",)")
				if j == -1 {
					return nil, errors.New("hex blob is not terminated")
				}
				if _, err := buf.ReadFrom(hex.NewDecoder(strings.NewReader(q.s[i+2 : i+2+j]))); err != nil {
					return nil, fmt.Errorf("error decoding hex blob: %v", err)
				}
				i += 2 + j
			} else {
				j := strings.IndexAny(q.s[i:], ",)")
				if j == -1 {
					return nil, errors.New("column value is not terminated")
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
			return nil, fmt.Errorf("unexpected character '%c'", q.s[i])
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
	client *client
	errCh  chan error
	mutex  sync.Mutex
	wg     sync.WaitGroup
}

func newReplacer(client *client) *replacer {
	return &replacer{client: client, errCh: make(chan error, 1)}
}

func (r *replacer) execute(ctx context.Context, database string, table *table) error {
	select {
	case err := <-r.errCh:
		return err
	default:
	}
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		table.wg.Wait()
		if err := r.replace(ctx, database, table); err != nil {
			r.errCh <- fmt.Errorf("error replacing table %s with new table %s: %v", quoteName(table.origName), quoteName(table.name), err)
		}
	}()
	return nil
}

func (r *replacer) replace(ctx context.Context, database string, table *table) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if *verbose {
		log.Printf("Replacing table %s with new table %s...", quoteName(table.origName), quoteName(table.name))
	}

	if err := r.client.dropTableIfExists(ctx, database, table.origName); err != nil {
		return fmt.Errorf("error dropping table %s: %v", quoteName(table.origName), err)
	}

	if err := r.client.renameTable(ctx, database, table.name, table.origName); err != nil {
		return fmt.Errorf("error renaming table %s to %s: %v", quoteName(table.name), quoteName(table.origName), err)
	}

	if len(table.foreignKeys) > 0 {
		if err := r.client.addForeignKeys(ctx, database, table.origName, table.foreignKeys); err != nil {
			return fmt.Errorf("error restoring foreign keys in table %s: %v", quoteName(table.origName), err)
		}
	}

	return nil
}

func (r *replacer) wait() error {
	waitCh := make(chan struct{})
	go func() {
		defer close(waitCh)
		r.wg.Wait()
	}()

	select {
	case err := <-r.errCh:
		return err
	case <-waitCh:
		return nil
	}
}
