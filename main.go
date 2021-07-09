// Command alblogs fetches a sample of AWS Elastic Load Balancer logs and loads
// it into an sqlite database for ad-hoc analysis.
package main

import (
	"compress/gzip"
	"context"
	"database/sql"
	_ "embed"
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"path"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	alb "github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"golang.org/x/term"
	_ "modernc.org/sqlite"
)

const timeLayout = "2006-01-02T15:04"

func main() {
	log.SetFlags(0)
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	args := runArgs{MaxSamples: 1}
	flag.IntVar(&args.MaxSamples, "n", args.MaxSamples, "load at most this `number` of candidate log files")
	flag.StringVar(&args.Database, "db", "", "`path` to the database file; "+
		"if empty, use a file in a temporary directory.\n"+
		"The same database file may be reused between program runs.")
	flag.StringVar(&args.TimeString, "time", "", "take log sample around this `time`, format is either "+
		"hh:mm\nfor today, or yyyy-mm-ddThh:mm for an arbitrary date;\n"+
		"if empty, take reference time as few minutes to the past")
	flag.BoolVar(&args.UTC, "utc", false, "treat time as UTC instead of local time zone")

	var cleanup bool
	flag.BoolVar(&cleanup, "clean", false, "clean cache and temporary files and exit")
	flag.Parse()

	if cleanup {
		_ = os.RemoveAll(tempDir())
		_ = os.RemoveAll(cacheDir())
		return
	}

	if err := run(ctx, &args, flag.Arg(0)); err != nil {
		if err == errUsage {
			flag.Usage()
			os.Exit(2)
		}
		log.Fatal(err)
	}
}

type runArgs struct {
	MaxSamples int
	UTC        bool
	TimeString string
	Database   string
	time       time.Time
}

func (args *runArgs) populate() error {
	if args.MaxSamples < 1 {
		return errors.New("number of candidate log files must be a positive number")
	}
	if args.TimeString == "" {
		args.time = time.Now().Add(-5 * time.Minute)
	} else {
		loc := time.Local
		if args.UTC {
			loc = time.UTC
		}
		var t time.Time
		var err error
		if t, err = time.ParseInLocation("15:04", args.TimeString, loc); err == nil {
			h, m, _ := t.Clock()
			now := time.Now().In(loc)
			t = time.Date(now.Year(), now.Month(), now.Day(), h, m, 0, 0, loc)
		} else if t, err = time.ParseInLocation(timeLayout, args.TimeString, loc); err != nil {
			return err
		}
		args.time = t
	}
	return nil
}

func run(ctx context.Context, args *runArgs, albName string) error {
	if err := args.populate(); err != nil {
		return err
	}
	if albName == "" {
		return errUsage
	}

	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return err
	}

	s3Client := s3.NewFromConfig(cfg)

	meta, err := loadMetadata(ctx, alb.NewFromConfig(cfg), albName)
	if err != nil {
		return err
	}

	fullPrefix := fullS3prefix(args.time, meta.Prefix, meta.Account, meta.Region)
	log.Println("Fetching candidate log files list, this may take a while")
	keys, err := candidateKeys(ctx, s3Client, meta.Bucket, fullPrefix, args.time)
	if err != nil {
		return err
	}
	if len(keys) == 0 {
		return fmt.Errorf("no candidate log files found, bucket %q, prefix %q", meta.Bucket, fullPrefix)
	}

	dbName := args.Database
	if dbName == "" {
		dbName = filepath.Join(tempDir(), albName+".db")
		if err := os.MkdirAll(filepath.Dir(dbName), 0777); err != nil {
			return err
		}
	}
	cols := logFields()
	db, err := sql.Open("sqlite", dbName)
	if err != nil {
		return err
	}
	defer db.Close()
	for _, pragma := range []string{"PRAGMA journal_mode=WAL", "PRAGMA synchronous=off"} {
		if _, err := db.ExecContext(ctx, pragma); err != nil {
			return err
		}
	}
	for _, statement := range databaseSchema(cols) {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			return err
		}
	}

	for i, k := range keys {
		if i == args.MaxSamples {
			break
		}
		log.Printf("Processing s3://%s", path.Join(meta.Bucket, k))
		if err := ingestLogFile(ctx, s3Client, meta.Bucket, k, db, cols); err != nil {
			return fmt.Errorf("ingesting %q: %w", k, err)
		}
	}
	_, _ = db.ExecContext(ctx, "PRAGMA optimize")
	if err := db.Close(); err != nil {
		return err
	}
	log.Print("For details on field description see")
	log.Print("https://docs.aws.amazon.com/elasticloadbalancing/latest/application/load-balancer-access-logs.html#access-log-entry-syntax")
	log.Println("Database file:", dbName)
	if term.IsTerminal(0) && term.IsTerminal(1) {
		if sqlitePath, err := exec.LookPath("sqlite3"); err == nil {
			// cmd := exec.CommandContext(ctx, sqlitePath, dbName)
			// cmd.Stdin = os.Stdin
			// cmd.Stdout = os.Stdout
			// cmd.Stderr = os.Stderr
			// return cmd.Run()
			return syscall.Exec(sqlitePath, []string{"sqlite3", dbName}, os.Environ())
		}
	}
	return nil
}

func logFields() []string { return strings.Split(strings.TrimSpace(fieldsFile), "\n") }

func ingestLogFile(ctx context.Context, client *s3.Client, bucket, key string, db *sql.DB, cols []string) error {
	obj, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &bucket,
		Key:    &key,
	})
	if err != nil {
		return err
	}
	defer obj.Body.Close()
	gr, err := gzip.NewReader(obj.Body)
	if err != nil {
		return err
	}
	defer gr.Close()

	rd := csv.NewReader(gr)
	rd.FieldsPerRecord = len(cols)
	rd.Comma = ' '
	rd.ReuseRecord = true

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	st, err := tx.PrepareContext(ctx, insertStatement(cols))
	if err != nil {
		return err
	}
	defer st.Close()
	var insertArgs []interface{}
	for {
		fields, err := rd.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		insertArgs = insertArgs[:0]
		for i, v := range fields {
			insertArgs = append(insertArgs, sql.Named(cols[i], v))
		}
		if _, err := st.ExecContext(ctx, insertArgs...); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// databaseSchema returns SQL statements initializing database
func databaseSchema(cols []string) []string {
	var out []string

	b := new(strings.Builder)
	b.WriteString("create table if not exists logs(\n")
	for i, col := range cols {
		var colType string
		switch col {
		case "elb_status_code", "target_status_code",
			"received_bytes", "sent_bytes",
			"matched_rule_priority":
			colType = "INTEGER"
		case "request_processing_time", "target_processing_time", "response_processing_time":
			colType = "REAL"
		}
		b.WriteString("    '")
		b.WriteString(col)
		b.WriteByte('\'')
		if colType != "" {
			b.WriteByte(' ')
			b.WriteString(colType)
		}
		if i != len(cols)-1 {
			b.WriteByte(',')
		}
		b.WriteByte('\n')
	}
	b.WriteByte(')')
	out = append(out, b.String())

	fs := newFieldSet(cols)

	b.Reset()
	if fs.has("request_creation_time", "trace_id") {
		b.WriteString("create unique index if not exists idx0 on logs(request_creation_time, trace_id)")
	} else {
		b.WriteString("create unique index if not exists idx0 on logs(")
		for i, col := range cols {
			b.WriteByte('\'')
			b.WriteString(col)
			b.WriteByte('\'')
			if i != len(cols)-1 {
				b.WriteByte(',')
			}
		}
		b.WriteByte(')')
	}
	out = append(out, b.String())

	return out
}

// insertStatement returns an INSERT SQL statement, expecting to take sql.Named
// arguments named after columns.
func insertStatement(cols []string) string {
	b := new(strings.Builder)
	b.WriteString("insert or ignore into logs values(\n")
	for i, col := range cols {
		b.WriteString("    @")
		b.WriteString(col)
		if i != len(cols)-1 {
			b.WriteByte(',')
		}
		b.WriteByte('\n')
	}
	b.WriteByte(')')
	return b.String()
}

func accountAndRegion(arn string) (account, region string, err error) {
	fields := strings.SplitN(arn, ":", 6)
	if len(fields) != 6 {
		return "", "", fmt.Errorf("bad ARN format: %q", arn)
	}
	return fields[4], fields[3], nil
}

// https://docs.aws.amazon.com/elasticloadbalancing/latest/application/load-balancer-access-logs.html#access-log-file-format
func fullS3prefix(t time.Time, prefix, account, region string) string {
	return path.Join(prefix, "AWSLogs", account, "elasticloadbalancing", region, t.UTC().Format("2006/01/02"))
}

func candidateKeys(ctx context.Context, client *s3.Client, bucket, fullPrefix string, refTime time.Time) ([]string, error) {
	p := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{
		Bucket: &bucket,
		Prefix: &fullPrefix,
	})
	notAfter := refTime.Add(5 * time.Minute)
	var out []string
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, obj := range page.Contents {
			if obj.LastModified == nil || obj.Key == nil || !strings.HasSuffix(*obj.Key, ".log.gz") {
				continue
			}
			if t := *obj.LastModified; t.Before(refTime) || t.After(notAfter) {
				continue
			}
			out = append(out, *obj.Key)
		}
	}
	return out, nil
}

// loadMetadata either returns load balancer logs setup from the local cache,
// or discovers it over AWS API, saving results to persistent cache.
func loadMetadata(ctx context.Context, albClient *alb.Client, albName string) (*metadata, error) {
	cacheFile := filepath.Join(cacheDir(), "alblogs-cache.json")
	var fullCache map[string]metadata
	b, err := os.ReadFile(cacheFile)
	if err == nil {
		if err := json.Unmarshal(b, &fullCache); err == nil {
			if meta, ok := fullCache[albName]; ok {
				return &meta, nil
			}
		}
	}

	var meta metadata

	descResult, err := albClient.DescribeLoadBalancers(ctx, &alb.DescribeLoadBalancersInput{
		Names: []string{albName},
	})
	if err != nil {
		return nil, err
	}
	var albARN string
	for _, lb := range descResult.LoadBalancers {
		if lb.LoadBalancerName != nil && *lb.LoadBalancerName == albName {
			albARN = *lb.LoadBalancerArn
			break
		}
	}
	if albARN == "" {
		return nil, errors.New("cannot figure out load balancer ARN")
	}

	attrResult, err := albClient.DescribeLoadBalancerAttributes(ctx, &alb.DescribeLoadBalancerAttributesInput{
		LoadBalancerArn: &albARN,
	})
	if err != nil {
		return nil, err
	}
	for _, attr := range attrResult.Attributes {
		if attr.Key == nil || attr.Value == nil {
			continue
		}
		if *attr.Key == "access_logs.s3.enabled" && *attr.Value != "true" {
			return nil, errors.New("load balancer has S3 logging disabled")
		}
		switch *attr.Key {
		case "access_logs.s3.bucket":
			meta.Bucket = *attr.Value
		case "access_logs.s3.prefix":
			meta.Prefix = *attr.Value
		}
	}
	if meta.Bucket == "" {
		return nil, errors.New("cannot figure out which S3 bucket is used for logs")
	}
	if meta.Account, meta.Region, err = accountAndRegion(albARN); err != nil {
		return nil, err
	}
	if fullCache == nil {
		fullCache = make(map[string]metadata)
	}
	fullCache[albName] = meta
	if b, err := json.Marshal(fullCache); err == nil {
		_ = os.MkdirAll(filepath.Dir(cacheFile), 0777)
		_ = os.WriteFile(cacheFile, b, 0666)
	}
	return &meta, nil
}

type metadata struct {
	Account string
	Region  string
	Bucket  string
	Prefix  string
}

func cacheDir() string {
	dir, err := os.UserCacheDir()
	if err != nil {
		dir = os.TempDir()
	}
	return filepath.Join(dir, "alblogs")
}

func tempDir() string { return filepath.Join(os.TempDir(), "alblogs") }

func init() {
	flag.Usage = func() {
		fmt.Fprintln(flag.CommandLine.Output(), "Usage: alblogs [flags] load-balancer-name")
		flag.PrintDefaults()
	}
}

var errUsage = errors.New("invalid usage")

//go:embed fields.txt
var fieldsFile string

type fieldSet map[string]struct{}

func newFieldSet(ss []string) fieldSet {
	fs := make(fieldSet)
	for _, s := range ss {
		fs[s] = struct{}{}
	}
	return fs
}

func (fs fieldSet) has(fields ...string) bool {
	for _, s := range fields {
		if _, ok := fs[s]; !ok {
			return false
		}
	}
	return true
}

//go:generate go run ./update-fields
