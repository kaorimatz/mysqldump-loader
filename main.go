package main

import (
	"bufio"
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

	loader := newLoader(db, *concurrency, *lowPriority)
	scanner := newScanner(r)

	executor := &executor{db: db, loader: loader, scanner: scanner}
	if err := executor.execute(); err != nil {
		log.Fatal(err)
	}
}

type executor struct {
	db      *sql.DB
	loader  *loader
	scanner *scanner
}

func (e *executor) execute() error {
	var charset, database string

	conn, err := e.db.Conn(context.Background())
	if err != nil {
		return err
	}
	defer conn.Close()

	for e.scanner.scan() {
		q := e.scanner.query()
		if q.isInsertStatement() || q.isReplaceStatement() {
			if err := e.loader.execute(context.Background(), q, charset, database); err != nil {
				return err
			}
		} else if q.isLockTablesStatement() || q.isUnlockTablesStatement() {
			continue
		} else {
			if _, err := conn.ExecContext(context.Background(), q.s); err != nil {
				return err
			}
			if q.isSetNamesStatement() {
				cs, err := parseSetNamesStatement(q)
				if err != nil {
					return err
				}
				charset = cs
			}
			if q.isUseStatement() {
				db, err := parseUseStatement(q)
				if err != nil {
					return err
				}
				database = db
			}
		}
	}
	if err := e.scanner.err(); err != nil {
		return err
	}

	if err := e.loader.wait(); err != nil {
		return err
	}

	return nil
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

type scanner struct {
	buf           bytes.Buffer
	comment       bool
	e             error
	line          int
	q             *query
	quote         byte
	r             *bufio.Reader
	stringLiteral bool
}

func newScanner(r io.Reader) *scanner {
	return &scanner{r: bufio.NewReader(r)}
}

func (s *scanner) scan() bool {
	if s.e != nil {
		return false
	}

	line := s.line

	for {
		str, err := s.r.ReadString('\n')
		if err != nil {
			s.e = err
			return false
		}
		s.line++

		if !s.comment && s.quote == 0 && (strings.HasPrefix(str, "--") || str == "\n") {
			continue
		}

		var i int
		for {
			j := strings.IndexAny(str[i:], "/*`\"'\\;")
			if j == -1 {
				s.buf.WriteString(str)
				break
			} else if !s.comment && s.quote == 0 && strings.HasPrefix(str[i+j:], "/*!") {
				s.comment = true
				i += j + 3
			} else if s.comment && strings.HasPrefix(str[i+j:], "*/") {
				s.comment = false
				i += j + 2
			} else if !s.comment && s.quote == 0 && strings.IndexByte("`\"'", str[i+j]) != -1 {
				s.quote = str[i+j]
				s.stringLiteral = str[i+j] == '\''
				i += j + 1
			} else if s.quote != 0 && str[i+j] == s.quote {
				if !s.stringLiteral && len(str) > i+j+1 && str[i+j+1] == s.quote {
					i += j + 2
				} else {
					s.quote = 0
					s.stringLiteral = false
					i += j + 1
				}
			} else if s.stringLiteral && str[i+j] == '\\' {
				i += j + 2
			} else if !s.comment && s.quote == 0 && str[i+j] == ';' {
				if len(str) != i+j+2 || str[i+j+1] != '\n' {
					s.e = fmt.Errorf("newline is expected after ';'. line=%d", s.line)
					return false
				}
				s.buf.WriteString(str[:i+j+1])
				s.q = &query{line: line + 1, s: s.buf.String()}
				s.buf.Reset()
				return true
			} else {
				i += j + 1
			}
		}
	}
}

func (s *scanner) err() error {
	if s.e == io.EOF {
		return nil
	}
	return s.e
}

func (s *scanner) query() *query {
	if s.e != nil {
		return nil
	}
	return s.q
}

type query struct {
	line int
	s    string
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
	converter   *converter
	db          *sql.DB
	errCh       chan error
	guardCh     chan struct{}
	lowPriority bool
	wg          sync.WaitGroup
}

func newLoader(db *sql.DB, concurrency int, lowPriority bool) *loader {
	return &loader{
		converter:   &converter{},
		db:          db,
		errCh:       make(chan error, concurrency),
		guardCh:     make(chan struct{}, concurrency),
		lowPriority: lowPriority,
	}
}

func (l *loader) execute(ctx context.Context, q *query, charset, database string) error {
	select {
	case err := <-l.errCh:
		return err
	case l.guardCh <- struct{}{}:
	}
	l.wg.Add(1)
	go func() {
		defer l.wg.Done()
		if err := l.load(ctx, q, charset, database); err != nil {
			l.errCh <- err
		}
		<-l.guardCh
	}()
	return nil
}

func (l *loader) load(ctx context.Context, q *query, charset, database string) error {
	i, err := l.converter.convert(q)
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
	query.Write(quoteName(i.table))
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

func setCharacterSet(ctx context.Context, conn *sql.Conn, charset string) error {
	_, err := conn.ExecContext(ctx, fmt.Sprintf("SET NAMES %s", charset))
	return err
}

func disableForeignKeyChecks(ctx context.Context, conn *sql.Conn) error {
	_, err := conn.ExecContext(ctx, "SET FOREIGN_KEY_CHECKS=0")
	return err
}

func (l *loader) wait() error {
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

type converter struct {
}

func (c *converter) convert(q *query) (*insertion, error) {
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
				buf.ReadFrom(hex.NewDecoder(strings.NewReader(q.s[i+2 : i+2+j])))
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

type insertion struct {
	ignore  bool
	r       io.Reader
	replace bool
	table   string
}
