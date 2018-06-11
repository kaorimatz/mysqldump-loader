package main

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"
)

type scanner struct {
	buf           bytes.Buffer
	comment       bool
	e             error
	line          int
	q             *query
	quote         byte
	reader        *bufio.Reader
	stringLiteral bool
}

func newScanner(r io.Reader) *scanner {
	return &scanner{reader: bufio.NewReader(r)}
}

func (s *scanner) scan() bool {
	if s.e != nil {
		return false
	}

	line := s.line

	for {
		str, err := s.reader.ReadString('\n')
		if err == io.EOF && s.buf.Len() != 0 {
			s.e = errors.New("unexpected EOF")
			return false
		}
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
			} else if s.comment && s.quote == 0 && strings.HasPrefix(str[i+j:], "*/") {
				s.comment = false
				i += j + 2
			} else if s.quote == 0 && strings.IndexByte("`\"'", str[i+j]) != -1 {
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
