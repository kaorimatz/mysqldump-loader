package main

import (
	"bufio"
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
