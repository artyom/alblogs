// Command update-fields fetches the list of AWS Elastic Load Balancer access
// log fields as described at
// https://docs.aws.amazon.com/elasticloadbalancing/latest/application/load-balancer-access-logs.html#access-log-entry-syntax
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"golang.org/x/net/html"
	"golang.org/x/net/html/atom"
)

func main() {
	log.SetFlags(0)
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://docs.aws.amazon.com/elasticloadbalancing/latest/application/load-balancer-access-logs.html", nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status: %q", resp.Status)
	}

	doc, err := html.Parse(resp.Body)
	if err != nil {
		return err
	}

	var fn func(*html.Node)
	var columns []string
	var wantTable bool
	fn = func(n *html.Node) {
		if err != nil {
			return
		}
		if n.Type == html.ElementNode && isHeader(n) && hasId(n, "access-log-entry-syntax") {
			wantTable = true
		}
		if wantTable && n.Type == html.ElementNode && n.DataAtom == atom.Table {
			columns, err = processTable(n)
			wantTable = false
			return
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			fn(c)
		}
	}
	fn(doc)
	if err != nil {
		return err
	}

	if len(columns) == 0 {
		return errors.New("no columns found")
	}
	colsCount := make(map[string]int)
	for _, name := range columns {
		colsCount[name]++
	}
	if len(colsCount) != len(columns) {
		b := new(strings.Builder)
		b.WriteString("non-unique column list, columns seen more than once: ")
		var prependComma bool
		for name, cnt := range colsCount {
			if cnt == 1 {
				continue
			}
			if prependComma {
				b.WriteByte(',')
			}
			fmt.Fprintf(b, " %q (%d)", name, cnt)
			if !prependComma {
				prependComma = true
			}
		}
		return errors.New(b.String())
	}
	out := strings.Join(columns, "\n")
	return os.WriteFile("fields.txt", []byte(out), 0666)
}

func processTable(table *html.Node) ([]string, error) {
	var column int
	var fn func(*html.Node)
	var out []string
	var err error
	fn = func(n *html.Node) {
		if err != nil {
			return
		}
		if n.Type == html.ElementNode && n.DataAtom == atom.Tr {
			column = 0
		}
		if n.Type == html.ElementNode && n.DataAtom == atom.Td && column == 0 {
			text := strings.TrimSpace(nodeText(n))
			s := strings.Map(func(r rune) rune {
				switch {
				case 'A' <= r && r <= 'Z':
					return r
				case 'a' <= r && r <= 'z':
					return r
				case '0' <= r && r <= '9':
					return r
				case r == '_':
					return r
				case r == ':':
					return '_'
				}
				return -1
			}, text)
			if s == "" {
				err = fmt.Errorf("invalid field name %q", text)
				return
			}
			out = append(out, s)
			column++
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			fn(c)
		}
	}
	fn(table)
	return out, err
}

func isHeader(n *html.Node) bool {
	switch n.DataAtom {
	case atom.H1, atom.H2, atom.H3, atom.H4, atom.H5, atom.H6:
		return true
	}
	return false
}

func hasId(n *html.Node, idText string) bool {
	for _, a := range n.Attr {
		if a.Key == "id" && a.Val == idText {
			return true
		}
	}
	return false
}

// nodeText returns text extracted from node and all its descendants
func nodeText(n *html.Node) string {
	var b strings.Builder
	var fn func(*html.Node)
	fn = func(n *html.Node) {
		if n.Type == html.TextNode {
			b.WriteString(n.Data)
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			fn(c)
		}
	}
	fn(n)
	return b.String()
}
