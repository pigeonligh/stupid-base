package main

import (
	"strings"

	"github.com/c-bata/go-prompt"
)

var startSuggests = []prompt.Suggest{
	{
		Text:        "use",
		Description: "to entry a database",
	},
	{
		Text:        "create",
		Description: "to create a database, a table or an index",
	},
	{
		Text:        "drop",
		Description: "to drop a database, a table or an index",
	},
	{
		Text:        "show",
		Description: "to show databases or tables",
	},
	{
		Text:        "desc",
		Description: "to describe a table",
	},
	{
		Text:        "alter",
		Description: "to alter a table",
	},
	{
		Text:        "insert",
		Description: "to insert rows into tables",
	},
	{
		Text:        "delete",
		Description: "to delete rows from tables",
	},
	{
		Text:        "update",
		Description: "to update rows in tables",
	},
	{
		Text:        "select",
		Description: "to query data in tables",
	},
	{
		Text:        "exit",
		Description: "exit this program",
	},
}

var fieldSuggests = []prompt.Suggest{
	{Text: "int"},
	{Text: "varchar"},
	{Text: "date"},
	{Text: "float"},
	{Text: "not"},
	{Text: "null"},
	{Text: "default"},
	{Text: "primary"},
	{Text: "foreign"},
	{Text: "key"},
	{Text: "references"},
}

var multiStepSuggests = map[string][]prompt.Suggest{
	"create": {
		{Text: "database"},
		{Text: "table"},
		{Text: "index"},
	},
	"create.index": {
		{Text: "on"},
	},

	"drop": {
		{Text: "database"},
		{Text: "table"},
		{Text: "index"},
	},

	"show": {
		{Text: "databases"},
		{Text: "tables"},
	},

	"insert": {
		{Text: "into"},
	},
	"insert.into": {
		{Text: "values"},
	},

	"delete": {
		{Text: "from"},
	},
	"delete.from": {
		{Text: "where"},
	},

	"update": {
		{Text: "set"},
	},
	"update.set": {
		{Text: "where"},
	},

	"select": {
		{Text: "from"},
	},
	"select.from": {
		{Text: "where"},
	},

	"alter": {
		{Text: "table"},
	},
	"alter.table": {
		{Text: "add"},
		{Text: "drop"},
		{Text: "change"},
		{Text: "rename"},
	},
	"alter.table.rename": {
		{Text: "to"},
	},
	"alter.table.drop": {
		{Text: "index"},
		{Text: "primary"},
		{Text: "foreign"},
	},
	"alter.table.drop.primary": {
		{Text: "key"},
	},
	"alter.table.drop.foreign": {
		{Text: "key"},
	},
	"alter.table.add": {
		{Text: "index"},
		{Text: "primary"},
		{Text: "foreign"},
		{Text: "constraint"},
	},
	"alter.table.add.primary": {
		{Text: "key"},
	},
	"alter.table.add.foreign": {
		{Text: "key"},
	},
	"alter.table.add.foreign.key": {
		{Text: "references"},
	},
	"alter.table.add.constraint": {
		{Text: "primary"},
		{Text: "foreign"},
	},
	"alter.table.add.constraint.primary": {
		{Text: "key"},
	},
	"alter.table.add.constraint.foreign": {
		{Text: "key"},
	},
	"alter.table.add.constraint.foreign.key": {
		{Text: "references"},
	},

	"create.table":       nil,
	"alter.table.change": nil,
}

func solveMultiStepSuggests(
	in prompt.Document,
	now string,
	fields []string,
) []prompt.Suggest {
	suggests, found := multiStepSuggests[now]
	if !found {
		prompt.FilterHasPrefix([]prompt.Suggest{}, in.GetWordBeforeCursor(), true)
	}
	if suggests == nil {
		return prompt.FilterHasPrefix(fieldSuggests, in.GetWordBeforeCursor(), true)
	}

	for _, text := range fields {

		for _, suggest := range suggests {
			if text == suggest.Text {
				now = now + "." + text
				suggests, found = multiStepSuggests[now]
				if !found {
					prompt.FilterHasPrefix([]prompt.Suggest{}, in.GetWordBeforeCursor(), true)
				}
				if suggests == nil {
					return prompt.FilterHasPrefix(fieldSuggests, in.GetWordBeforeCursor(), true)
				}
			}
		}
	}

	// special
	if now == "alter.table.add" {
		if fields[len(fields)-1] != "add" {
			return prompt.FilterHasPrefix(fieldSuggests, in.GetWordBeforeCursor(), true)
		}
	}
	return prompt.FilterHasPrefix(suggests, in.GetWordBeforeCursor(), true)
}

func completer(in prompt.Document) []prompt.Suggest {
	check := func(c rune) bool {
		return c == ' ' || c == '\n' || c == ';'
	}

	suffix := strings.ToLower(query + in.CurrentLine())

	fields := strings.FieldsFunc(suffix, check)

	if len(fields) > 0 && !check(rune(suffix[len(suffix)-1])) {
		fields = fields[0 : len(fields)-1]
	}

	if len(fields) == 0 {
		return prompt.FilterHasPrefix(startSuggests, in.GetWordBeforeCursor(), true)
	}

	return solveMultiStepSuggests(in, fields[0], fields)
}
