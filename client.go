package main

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
)

type client struct {
	conn *sql.Conn
}

func (c *client) addForeignKeys(ctx context.Context, database, table string, foreignKeys []string) error {
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

	return c.exec(ctx, query.String())
}

func (c *client) createTable(ctx context.Context, database, table, body string) error {
	var query bytes.Buffer

	query.WriteString("CREATE TABLE ")
	if database != "" {
		query.Write(quoteName(database))
		query.WriteByte('.')
	}

	query.Write(quoteName(table))
	query.WriteByte(' ')

	query.WriteString(body)

	return c.exec(ctx, query.String())
}

func (c *client) dropTableIfExists(ctx context.Context, database, table string) error {
	var query bytes.Buffer

	query.WriteString("DROP TABLE IF EXISTS ")

	if database != "" {
		query.Write(quoteName(database))
		query.WriteByte('.')
	}
	query.Write(quoteName(table))

	return c.exec(ctx, query.String())
}

func (c *client) renameTable(ctx context.Context, database, oldTable, newTable string) error {
	var query bytes.Buffer

	query.WriteString("RENAME TABLE ")
	if database != "" {
		query.Write(quoteName(database))
		query.WriteByte('.')
	}
	query.Write(quoteName(oldTable))

	query.WriteString(" TO ")
	if database != "" {
		query.Write(quoteName(database))
		query.WriteByte('.')
	}
	query.Write(quoteName(newTable))

	return c.exec(ctx, query.String())
}

func (c *client) setCharacterSet(ctx context.Context, charset string) error {
	return c.exec(ctx, fmt.Sprintf("SET NAMES %s", charset))
}

func (c *client) setVariables(ctx context.Context, variables map[string]string) error {
	var query bytes.Buffer

	query.WriteString("SET ")
	for name, value := range variables {
		if query.Len() != 4 {
			query.WriteString(", ")
		}
		query.WriteString("SESSION ")
		query.WriteString(name)
		query.WriteString(" = ")
		query.WriteString(value)
	}

	return c.exec(ctx, query.String())
}

func (c *client) exec(ctx context.Context, query string) error {
	_, err := c.conn.ExecContext(ctx, query)
	return err
}

func (c *client) close() error {
	return c.conn.Close()
}
