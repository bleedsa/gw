package main

import (
	"os"
	"fmt"
	"time"
	"html"
	"net/http"
	"database/sql"
	"net/url"
	"mime"
	"mime/multipart"
	"io"
	"strings"

	"codeberg.org/anaseto/goal"
	"codeberg.org/anaseto/goal/cmd"
	"codeberg.org/anaseto/goal/help"
	gos "codeberg.org/anaseto/goal/os"

	_ "github.com/mattn/go-sqlite3"
)

func RowsToV(rs *sql.Rows) goal.V {
	rs.ColumnTypes()
	cols, _ := rs.Columns()

	count := len(cols)
	vals := make([]interface{}, count)
	args := make([]interface{}, count)
	for i := range vals {
		args[i] = &vals[i]
	}

	buf := make(map[int]map[string]goal.V)

	i := 0
	for ; rs.Next(); i++ {
		row := make(map[string]goal.V)

		err := rs.Scan(args...)
		if err != nil {
			return goal.Panicf("error scanning column: %q", err)
		}

		for i, v := range vals {
			switch v.(type) {
			case int:
			case int8: case int16:
			case int32: case int64:
				row[cols[i]] = goal.NewI(v.(int64))
				break

			case float32:
			case float64:
				row[cols[i]] = goal.NewF(v.(float64))
				break

			case bool:
				if v.(bool) {
					row[cols[i]] = goal.NewI(1)
				} else {
					row[cols[i]] = goal.NewI(0)
				}
				break

			case string:
				row[cols[i]] = goal.NewS(v.(string))
				break

			case nil:
				row[cols[i]] = goal.NewAV([]goal.V{})

			default:
				return goal.Panicf("invalid type")
			}
		}

		buf[i] = row
	}

	ret := make([]goal.V, i)

	for i, x := range buf {
		xs := make([]goal.V, count)
		ys := make([]goal.V, count)

		idx := 0
		for k, v := range x {
			xs[idx] = goal.NewS(k)
			ys[idx] = v
			idx++
		}

		ret[i] = goal.NewD(goal.NewAV(xs), goal.NewAV(ys))
	}

	return goal.NewAV(ret)
}

func Err(x string) goal.V {
	return goal.NewError(goal.NewS(x))
}

type gwD struct {
	t time.Time
	s string
}

func (x *gwD) Append(ctx *goal.Context, dst []byte, compact bool) []byte {
	return append(dst, x.s...)
}

func (x *gwD) Matches(y goal.BV) bool {
	yv, ok := y.(*gwD)
	return ok && x == yv
}

func (*gwD) Type() string {
	return "D"
}

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

func EscStr(sql string) string {
	dest := make([]byte, 0, 2*len(sql))
	var escape byte
	for i := 0; i < len(sql); i++ {
		c := sql[i]

		escape = 0

		switch c {
		case '\\':
			escape = '\\'
			break
		case '\'':
			dest = append(dest, '\'');
			break
		case '"':
			escape = '"'
			break
		}

		if escape != 0 {
			dest = append(dest, '\\', escape)
		} else {
			dest = append(dest, c)
		}
	}

	return string(dest)
}

func DNew(ctx *goal.Context, args []goal.V) goal.V {
	if len(args) != 1 {
		return goal.Panicf("D.new i: ~1=#args")
	}

	i := args[0].I()
	t := time.Unix(i, i)

	return goal.NewV(&gwD {
		t,
		fmt.Sprintf("D.new[%q]", t),
	})
}

func DFmt(ctx *goal.Context, args []goal.V) goal.V {
	if len(args) != 1 {
		return goal.Panicf("D.fmt DT: ~1=#args")
	}

	d, ok := args[0].BV().(*gwD); if !ok {
		return goal.Panicf("D.fmt DT: bad type %q in DT", args[0].Type())
	}

	return goal.NewS(fmt.Sprintf("%s", d.t))
}

func DWeekday(ctx *goal.Context, args []goal.V) goal.V {
	if len(args) != 1 {
		return goal.Panicf("D.weekday DT: ~1=#args")
	}

	d, ok := args[0].BV().(*gwD); if !ok {
		return goal.Panicf("D.weekday DT: bad type %q in DT", args[0].Type())
	}

	return goal.NewI(int64(d.t.Weekday()))
}

func SQLEsc(ctx *goal.Context, args []goal.V) goal.V {
	if len(args) != 1 {
		return goal.Panicf("sql.esc s: ~1=#args")
	}

	str, ok := args[0].BV().(goal.S); if !ok {
		return goal.Panicf(
			"sql.esc s: bad type %q in s", args[0].Type(),
		)
	}

	return goal.NewS(EscStr(string(str)))
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


func SQLQuery(ctx *goal.Context, args []goal.V) goal.V {
	/*
	 * boilerplate
	 */
	 if len(args) != 2 {
		 return goal.Panicf("sql.qry[db;s]: ~2=#args")
	 }

	 db, ok := args[1].BV().(*gwSql); if !ok {
		 return goal.Panicf(
			 "sql.qry[db;s]: bad type %q in db", args[1].Type(),
		 )
	 }

	 cmd, ok := args[0].BV().(goal.S); if !ok {
		 return goal.Panicf(
			 "sql.qry[db;s]: bad type %q in s", args[0].Type(),
		 )
	 }

	 rows, err := db.db.Query(string(cmd)); if err != nil {
		 return goal.Panicf(
			 "sql.qry[db;s]: err in query: %s", err,
		 )
	 }

	 return RowsToV(rows)
}

func SQLExe(ctx *goal.Context, args []goal.V) goal.V {
	/*
	 * boilerplate
	 */
	if len(args) != 2 {
		return goal.Panicf("sql.exe[db;s]: ~2=#args")
	}

	db, ok := args[1].BV().(*gwSql); if !ok {
		return goal.Panicf(
			"db sql.exe s: bad type %q in db", args[1].Type(),
		)
	}

	cmd, ok := args[0].BV().(goal.S); if !ok {
		return goal.Panicf(
			"db sql.exe s: bad type %q in s", args[0].Type(),
		)
	}

	/* actually exec */
	res, err := db.db.Exec(string(cmd)); if err != nil {
		return goal.Panicf(
			"db sql.exe s: err in exec: %s", err,
		)
	}

	/*
	 * unwrap the result data
	 */
	ret := []goal.V{goal.NewI(-1),goal.NewI(-1)}

	x, err := res.LastInsertId(); if err == nil {
		ret[0] = goal.NewI(x)
	}
	x, err = res.RowsAffected(); if err == nil {
		ret[1] = goal.NewI(x)
	}

	return goal.NewAV(ret)
}

func URLDec(ctx *goal.Context, args []goal.V) goal.V {
	if len(args) != 1 {
		return goal.Panicf("url.dec s: ~1=#args")
	}

	x, ok := args[0].BV().(goal.S); if !ok {
		return goal.Panicf("url.dec: bad type %q in s", args[0].Type())
	}

	ret, err := url.QueryUnescape(string(x)); if err != nil {
		return goal.Panicf("url.dec: cannot unescape %s", string(x))
	}

	return goal.NewS(ret)
}

func URLEnc(ctx *goal.Context, args []goal.V) goal.V {
	if len(args) != 1 {
		return goal.Panicf("url.enc s: ~1=#args")
	}

	x, ok := args[0].BV().(goal.S); if !ok {
		return goal.Panicf("url.enc: bad type %q in s", args[0].Type())
	}

	return goal.NewS(url.QueryEscape(string(x)))
}

func HTMLEsc(ctx *goal.Context, args []goal.V) goal.V {
	if len(args) != 1 {
		return goal.Panicf("html.esc s: ~1=#args")
	}

	x, ok := args[0].BV().(goal.S); if !ok {
		return goal.Panicf("html.esc s: bad type %q in s", args[0].Type())
	}

	return goal.NewS(html.EscapeString(string(x)))
}

func UtilNow(ctx *goal.Context, args []goal.V) goal.V {
	if len(args) != 1 {
		return goal.Panicf("util.now x: ~1=#args")
	}
	return goal.NewI(time.Now().Unix())
}

func UtilFileType(ctx *goal.Context, args []goal.V) goal.V {
	if len(args) != 1 {
		return goal.Panicf("util.filetype AB: ~1=#args")
	}

	p := args[0].BV(); if p == nil {
		return goal.Panicf("util.filetype AB: bad type %q in s", args[0].Type())
	}

	switch v := p.(type) {
	case *goal.AB:
		return goal.NewS(http.DetectContentType(v.Slice))
	default:
		return goal.Panicf("util.filetype AB: bad type %q in AB", args[0].Type())
	}
}

func UtilMultipart(ctx *goal.Context, args []goal.V) goal.V {
	if len(args) != 1 {
		return goal.Panicf("util.multipart s: ~1=#args")
	}

	form, ok := args[0].BV().(goal.S); if !ok {
		return goal.Panicf("util.multipart s: bad type %q in s", args[0].Type())
	}

	env := make(map[string]string)
	for _, e := range os.Environ() {
		if i := strings.Index(e, "="); i >= 0 {
			env[e[:i]] = e[i+1:]
		}
	}

	_, prms, _ := mime.ParseMediaType(env["CONTENT_TYPE"])

	sr := strings.NewReader(string(form))
	mr := multipart.NewReader(sr, prms["boundary"])

	/* resultant map */
	res := make(map[string]goal.V)

	for {
		p, err := mr.NextPart()
		if err == io.EOF {
			goto ret
		}
		if err != nil {
			return Err(fmt.Sprintf("util.multipart s: failed to get next part: %q", err))
		}

		fi := p.FileName()
		fo := p.FormName()

		buf, err := io.ReadAll(p)
		if err != nil {
			return Err(fmt.Sprintf("util.multipart s: failed to read part: %q", err))
		}

		if fi == "" {
			res[fo] = goal.NewS(string(buf))
		} else {
			head := goal.NewAS([]string{"name",        "content"})
			body := goal.NewAV([]goal.V{goal.NewS(fi), goal.NewAB(buf)})
			res[fo] = goal.NewD(head, body)
		}
	}

ret:
	i := 0
	wid := len(res)
	head := make([]goal.V, wid)
	body := make([]goal.V, wid)

	for k, v := range res {
		head[i] = goal.NewS(k)
		body[i] = v
		i++
	}

	return goal.NewD(goal.NewAV(head), goal.NewAV(body))
}

func UtilWrite(ctx *goal.Context, args []goal.V) goal.V {
	if len(args) != 2 {
		return goal.Panicf("util.write[s;B]: ~1=#args")
	}

	path, ok := args[1].BV().(goal.S); if !ok {
		return goal.Panicf("util.write[s;B]: bad type %q in s", args[1].Type())
	}

	switch v := args[0].BV().(type) {
	case *goal.AB:
		err := os.WriteFile(string(path), v.Slice, 0666); if err != nil {
			return Err(fmt.Sprintf("util.write[s;B]: failed to write file: %q", err))
		}
		return goal.NewI(1)
	default:
		return goal.Panicf("util.write[s;B]: bad type %q in B", args[0].Type())
	}
}

func main() {
	ctx := goal.NewContext()
	ctx.Log = os.Stderr
	gos.Import(ctx, "")

	ctx.AssignGlobal("sql.open",       ctx.RegisterMonad(".sql.open", SQLOpen))
	ctx.AssignGlobal("sql.cls",        ctx.RegisterMonad(".sql.cls", SQLClose))
	ctx.AssignGlobal("sql.esc",        ctx.RegisterMonad(".sql.esc", SQLEsc))
	ctx.AssignGlobal("sql.exe",        ctx.RegisterDyad(".sql.exe", SQLExe))
	ctx.AssignGlobal("sql.qry",        ctx.RegisterDyad(".sql.qry", SQLQuery))

	ctx.AssignGlobal("url.dec",        ctx.RegisterMonad(".url.dec", URLDec))
	ctx.AssignGlobal("url.enc",        ctx.RegisterMonad(".url.enc", URLEnc))

	ctx.AssignGlobal("html.esc",       ctx.RegisterMonad(".html.esc", HTMLEsc))

	ctx.AssignGlobal("util.now",       ctx.RegisterMonad(".util.now", UtilNow))
	ctx.AssignGlobal("util.filetype",  ctx.RegisterMonad(".util.filetype", UtilFileType))
	ctx.AssignGlobal("util.multipart", ctx.RegisterMonad(".util.multipart", UtilMultipart))
	ctx.AssignGlobal("util.write",     ctx.RegisterMonad(".util.write", UtilWrite))

	ctx.AssignGlobal("D.new",          ctx.RegisterMonad(".D.new", DNew))
	ctx.AssignGlobal("D.fmt",          ctx.RegisterMonad(".D.fmt", DFmt))
	ctx.AssignGlobal("D.weekday",      ctx.RegisterMonad(".D.weekday", DWeekday))

	cmd.Exit(cmd.Run(ctx, cmd.Config{
		Help: help.HelpFunc(),
		Man: "goal",
	}))
}
