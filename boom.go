// Copyright 2014 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"bufio"
	"crypto/md5"
	"flag"
	"fmt"
	"github.com/yuankui/boom/boomer"
	"log"
	"math"
	"net/http"
	_ "net/http/pprof"
	gourl "net/url"
	"os"
	"regexp"
	"runtime"
	"strings"
	"time"
)

const (
	headerRegexp = `^([\w-]+):\s*(.+)`
	authRegexp   = `^(.+):([^\s].+)`
)

var (
	m           = flag.String("m", "GET", "")
	headers     = flag.String("h", "", "")
	body        = flag.String("d", "", "")
	accept      = flag.String("A", "", "")
	contentType = flag.String("T", "text/html", "")
	authHeader  = flag.String("a", "", "")
	readAll     = flag.Bool("readall", false, "")

	output = flag.String("o", "", "")
	file   = flag.String("f", "", "")

	c    = flag.Int("c", 4, "")
	n    = flag.Int("n", 200, "")
	t    = flag.Int("t", 0, "")
	q    = flag.Int("q", 0, "")
	s    = flag.Int("s", 0, "")
	k    = flag.String("k", "", "")
	cpus = flag.Int("cpus", runtime.GOMAXPROCS(-1), "")
	port = flag.Int64("p", 6060, "")

	insecure           = flag.Bool("allow-insecure", false, "")
	disableCompression = flag.Bool("disable-compression", false, "")
	disableKeepAlives  = flag.Bool("disable-keepalive", false, "")
	proxyAddr          = flag.String("x", "", "")
)

var usage = `Usage: boom [options...] <url>

Options:
  -n  Number of requests to run(default 200).
  -c  Number of requests to run concurrently(default 4). Total number of requests cannot
      be smaller than the concurency level.
  -t  timelimit for the benchmark in seconds
  -q  Rate limit, in seconds (QPS).
  -o  Output type. If none provided, a summary is printed.
      "csv" is the only supported alternative. Dumps the response
      metrics in comma-seperated values format.

  -f  Query Files to get From query from
  -m  HTTP method, one of GET, POST, PUT, DELETE, HEAD, OPTIONS.
  -h  Custom HTTP headers, name1:value1;name2:value2.
  -s  Timeout in ms.
  -k  private key to generate 'X-Perf-Test' flag
  -A  HTTP Accept header.
  -d  HTTP request body.
  -T  Content-type, defaults to "text/html".
  -a  Basic authentication, username:password.
  -x  HTTP Proxy address as host:port.
  -p  Debug port for golang

  -readall              Consumes the entire request body.
  -allow-insecure       Allow bad/expired TLS/SSL certificates.
  -disable-compression  Disable compression.
  -disable-keepalive    Disable keep-alive, prevents re-use of TCP
                        connections between different HTTP requests.
  -cpus                 Number of used cpu cores.
                        (default for current machine is %d cores)
`

func main() {

	go func() {
		log.Println(http.ListenAndServe(fmt.Sprintf("localhost:%d", *port), nil))
	}()

	flag.Usage = func() {
		fmt.Fprint(os.Stderr, fmt.Sprintf(usage, runtime.NumCPU()))
	}

	flag.Parse()
	if flag.NArg() < 1 {
		usageAndExit("")
	}

	runtime.GOMAXPROCS(*cpus)
	num := *n
	conc := *c
	timelimit := *t
	q := *q

	// use timelimit if specified OR else use request number
	if timelimit == 0 {
		timelimit = math.MaxInt32
	} else {
		num = math.MaxInt32
	}

	if num <= 0 || conc <= 0 || timelimit < 0 {
		usageAndExit("n, c and t cannot be smaller than 1.")
	}

	var (
		url, method string
		// Username and password for basic auth
		username, password string
		// request headers
		header http.Header = make(http.Header)
	)

	url = flag.Args()[0]
	method = strings.ToUpper(*m)

	// set content-type
	header.Set("Content-Type", *contentType)
	if k != nil {
		flag := calcTestFlag(*k, time.Now())
		header.Set("X-Perf-Test", flag)
	}

	// set any other additional headers
	if *headers != "" {
		headers := strings.Split(*headers, ";")
		for _, h := range headers {
			match, err := parseInputWithRegexp(h, headerRegexp)
			if err != nil {
				usageAndExit(err.Error())
			}
			header.Set(match[1], match[2])
		}
	}

	if *accept != "" {
		header.Set("Accept", *accept)
	}

	// set basic auth if set
	if *authHeader != "" {
		match, err := parseInputWithRegexp(*authHeader, authRegexp)
		if err != nil {
			usageAndExit(err.Error())
		}
		username, password = match[1], match[2]
	}

	if *output != "csv" && *output != "" {
		usageAndExit("Invalid output type; only csv is supported.")
	}

	var proxyURL *gourl.URL
	if *proxyAddr != "" {
		var err error
		proxyURL, err = gourl.Parse(*proxyAddr)
		if err != nil {
			usageAndExit(err.Error())
		}
	}

	requestChan := make(chan *http.Request, 1000)

	// determine url from param or file
	if len(*file) == 0 {
		go func() {
			for {

				requestChan <- buildRequest(method, url, header, username, password)
			}
		}()
	} else {
		urlChan, err := buildUrlChan(*file)

		if err != nil {
			usageAndExit(err.Error())
		}
		go func() {
			for {
				url := <-urlChan
				req := buildRequest(method, url, header, username, password)
				requestChan <- req
			}
		}()
	}

	(&boomer.Boomer{
		RequestChan:        requestChan,
		RequestBody:        *body,
		N:                  num,
		T:                  timelimit,
		C:                  conc,
		Qps:                q,
		Timeout:            *s,
		AllowInsecure:      *insecure,
		DisableCompression: *disableCompression,
		DisableKeepAlives:  *disableKeepAlives,
		ProxyAddr:          proxyURL,
		Output:             *output,
		ReadAll:            *readAll,
	}).Run()
}

func calcTestFlag(key string, t time.Time) string {
	hash := md5.New()
	ts := t.Format("2006010215")
	hash.Write([]byte(ts))
	hash.Write([]byte(key))
	ret := hash.Sum(nil)
	return fmt.Sprintf("%x", ret)
}

func buildUrlChan(file string) (chan string, error) {
	urlChan := make(chan string, 1000)

	fd, err := os.Open(file)
	if err != nil {
		return nil, err
	}

	go func() {
		for {
			fd.Seek(0, 0)
			scanner := bufio.NewScanner(fd)
			for scanner.Scan() {
				urlChan <- scanner.Text()
			}
		}
		fd.Close()
	}()

	return urlChan, nil
}

func buildRequest(method, url string, header http.Header, username string, password string) *http.Request {
	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		usageAndExit(err.Error())
	}
	req.Header = header
	if username != "" || password != "" {
		req.SetBasicAuth(username, password)
	}
	return req
}

func usageAndExit(msg string) {
	if msg != "" {
		fmt.Fprintf(os.Stderr, msg)
		fmt.Fprintf(os.Stderr, "\n\n")
	}
	flag.Usage()
	fmt.Fprintf(os.Stderr, "\n")
	os.Exit(1)
}

func parseInputWithRegexp(input, regx string) ([]string, error) {
	re := regexp.MustCompile(regx)
	matches := re.FindStringSubmatch(input)
	if len(matches) < 1 {
		return nil, fmt.Errorf("could not parse the provided input; input = %v", input)
	}
	return matches, nil
}
