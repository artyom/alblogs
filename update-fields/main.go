// Command update-fields fetches the list of AWS Elastic Load Balancer access
// log fields.
//
// Log fields are described at
// https://docs.aws.amazon.com/elasticloadbalancing/latest/application/load-balancer-access-logs.html#access-log-entry-syntax,
// this command do lightweight parsing of the markdown source of that page
// found at
// https://raw.githubusercontent.com/awsdocs/elb-application-load-balancers-user-guide/master/doc_source/load-balancer-access-logs.md
package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
)

const markdownURL = `https://raw.githubusercontent.com/awsdocs/elb-application-load-balancers-user-guide/master/doc_source/load-balancer-access-logs.md`

func main() {
	log.SetFlags(0)
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	if err := run(ctx); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, markdownURL, nil)
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

	var columns []string
	sc := bufio.NewScanner(resp.Body)
	var inTable bool
	for sc.Scan() {
		b := sc.Bytes()
		if !inTable && bytes.HasPrefix(b, []byte("| Field |")) {
			inTable = true
			continue
		}
		if inTable && len(b) != 0 && b[0] != '|' {
			break
		}
		if inTable && len(b) == 0 {
			break
		}
		if !inTable {
			continue
		}
		fields := bytes.SplitN(b, []byte{'|'}, 3)
		if len(fields) != 3 {
			return fmt.Errorf("wrong table line %q", sc.Text())
		}
		s := bytes.TrimSpace(fields[1])
		if len(bytes.Trim(s, "-")) == 0 {
			continue
		}
		s = bytes.Map(func(r rune) rune {
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
		}, s)
		if len(s) == 0 {
			return fmt.Errorf("invalid field name in a table line %q", sc.Text())
		}
		columns = append(columns, string(s))
	}
	if err := sc.Err(); err != nil {
		return err
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
