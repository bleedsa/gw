package main

import (
	"os"
	"fmt"
	"database/sql"

	"codeberg.org/anaseto/goal"
	"codeberg.org/anaseto/goal/cmd"
	"codeberg.org/anaseto/goal/help"
	gos "codeberg.org/anaseto/goal/os"

	_ "github.com/mattn/go-sqlite3"
)

type gwSql struct {
	db *sql.DB
	s string
}

func (x *gwSql) Append(ctx *goal.Context, dst []byte, compact bool) []byte {
	return append(dst, x.s...)
}

func (x *gwSql) Matches(y goal.BV) bool {
	yv, ok := y.(*gwSql)
	return ok && x == yv
}

func (x *gwSql) Type() string {
	return "SQL"
}

func SQLClose(ctx *goal.Context, args []goal.V) goal.V {
	if len(args) > 1 {
		return goal.Panicf(
			"sql.cls s: bad type %q in s", args[0].Type(),
		)
	}

	db, ok := args[0].BV().(*gwSql); if !ok {
		return goal.Panicf(
			"sql.cls s: bad type %q in s", args[0].Type(),
		)
	}

	db.db.Close()
	return goal.NewAV([]goal.V{})
}

func SQLOpen(ctx *goal.Context, args []goal.V) goal.V {
	if len(args) > 1 {
		return goal.Panicf(
			"sql.open s: bad type %q in s", args[0].Type(),
		)
	}

	s, ok := args[0].BV().(goal.S); if !ok {
		return goal.Panicf(
			"sql.open s: bad type %q in s", args[0].Type(),
		)
	}

	db, err := sql.Open("sqlite3", string(s)); if err != nil {
		return goal.Panicf(
			"sql.open s: error opening database: %s", err,
		)
	}

	return goal.NewV(&gwSql {
		db,
		fmt.Sprintf("sql.open[%q]", s),
	})
}

func SQLExe(ctx *goal.Context, args []goal.V) goal.V {
	if len(args) != 2 {
		return goal.Panicf(
			"db sql.exe s: too many args", args[0].Type(),
		)
	}

	db, ok := args[1].BV().(*gwSql); if !ok {
		return goal.Panicf(
			"db sql.exe s: bad type %q in db", args[0].Type(),
		)
	}

	cmd, ok := args[0].BV().(goal.S); if !ok {
		return goal.Panicf(
			"db sql.exe s: bad type %q in s", args[0].Type(),
		)
	}

	db.db.Exec(string(cmd))

	return goal.NewAV([]goal.V{})
}

func main() {
	ctx := goal.NewContext()
	ctx.Log = os.Stderr
	gos.Import(ctx, "")

	ctx.AssignGlobal("sql.open", ctx.RegisterMonad(".sql.open", SQLOpen))
	ctx.AssignGlobal("sql.cls",  ctx.RegisterMonad(".sql.cls", SQLClose))
	ctx.AssignGlobal("sql.exe",  ctx.RegisterDyad(".sql.exe", SQLExe))

	cmd.Exit(cmd.Run(ctx, cmd.Config{
		Help: help.HelpFunc(),
		Man: "goal",
	}))
}
