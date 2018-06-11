package main

import (
	"bufio"
	"errors"
	"strings"
	"testing"
)

func TestQuery(t *testing.T) {
	cases := []struct {
		in   string
		want *query
	}{
		{"\n", nil},
		{"--\n", nil},
		{"/*!40000 ALTER TABLE `/*!` DISABLE KEYS */;\n", &query{line: 1, s: "/*!40000 ALTER TABLE `/*!` DISABLE KEYS */;"}},
		{"/*!40000 ALTER TABLE `*/` DISABLE KEYS */;\n", &query{line: 1, s: "/*!40000 ALTER TABLE `*/` DISABLE KEYS */;"}},
		{"/*!40000 ALTER TABLE `'\"` DISABLE KEYS */;\n", &query{line: 1, s: "/*!40000 ALTER TABLE `'\"` DISABLE KEYS */;"}},
		{"/*!40000 ALTER TABLE `;` DISABLE KEYS */;\n", &query{line: 1, s: "/*!40000 ALTER TABLE `;` DISABLE KEYS */;"}},
		{"/*!40000 ALTER TABLE `a\nb` DISABLE KEYS */;\n", &query{line: 1, s: "/*!40000 ALTER TABLE `a\nb` DISABLE KEYS */;"}},
	}
	for _, c := range cases {
		s := &scanner{reader: bufio.NewReader(strings.NewReader(c.in))}
		s.scan()
		got := s.query()
		if got == nil && c.want == nil {
			continue
		}
		if got == nil || *got != *c.want {
			t.Errorf("in: %q, got: %#v, want: %#v", c.in, got, c.want)
		}
	}
}

func TestErr(t *testing.T) {
	cases := []struct {
		in   string
		want error
	}{
		{"", nil},
		{"UNLOCK TABLES\n", errors.New("unexpected EOF")},
		{"LOCK TABLES `a` WRITE;UNLOCK TABLES;\n", errors.New("newline is expected after ';'. line=1")},
	}
	for _, c := range cases {
		s := &scanner{reader: bufio.NewReader(strings.NewReader(c.in))}
		s.scan()
		got := s.err()
		if got == nil && c.want == nil {
			continue
		}
		if got == nil || got.Error() != c.want.Error() {
			t.Errorf("in: %q, got: %#v, want: %#v", c.in, got, c.want)
		}
	}
}
