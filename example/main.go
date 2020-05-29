package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"time"

	"github.com/chenjiandongx/aura"
	"github.com/chenjiandongx/aura/reporter"
	"github.com/chenjiandongx/memcached-collector"
	"github.com/go-kit/kit/log"
)

// n9e 标准 Metric 格式
type Metric struct {
	Endpoint  string      `json:"endpoint"`
	Metric    string      `json:"metric"`
	Step      uint32      `json:"step"`
	Value     interface{} `json:"value"`
	Type      string      `json:"counterType"`
	Tags      string      `json:"tags"`
	Timestamp int64       `json:"timestamp"`
}

// mock 接收 metric HTTP 接口
func receive(_ http.ResponseWriter, req *http.Request) {
	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		panic(err)
	}
	defer req.Body.Close()

	var metrics []Metric
	if err := json.Unmarshal(body, &metrics); err != nil {
		panic(err)
	}

	for _, m := range metrics {
		fmt.Printf("metrics:%+v\n", m)
	}
}

// RunDirectly
func main1() {
	registry := aura.NewRegistry(nil)

	cot := collector.NewCollector("192.168.2.21:11211", 1*time.Second, log.NewJSONLogger(os.Stdout))
	registry.MustRegister(cot)

	n9eReporter := reporter.DefaultHTTPReporter
	n9eReporter.DropEndpoint = true
	n9eReporter.Urls = []string{"http://localhost:8099/api/push"}
	// 10s 上报周期
	n9eReporter.Ticker = time.Tick(30 * time.Second)
	registry.AddReporter(n9eReporter)

	go registry.Serve("localhost:9099")

	http.HandleFunc("/api/push", receive)
	go func() {
		http.ListenAndServe("localhost:8099", nil)
	}()

	registry.Run()
}

type Config struct {
	MemcachedServer string `json:"memcachedServer"`
	HttpURL         string `json:"httpURL"`
}

// RunWithConfig
func main() {
	cfgPath := flag.String("c", "config.json", "configuration file")
	flag.Parse()

	var cfg Config
	content, err := ioutil.ReadFile(*cfgPath)
	if err != nil {
		panic(err)
	}

	if err := json.Unmarshal(content, &cfg); err != nil {
		panic(err)
	}

	registry := aura.NewRegistry(nil)
	cot := collector.NewCollector(cfg.MemcachedServer, 1*time.Second, log.NewJSONLogger(os.Stdout))
	registry.MustRegister(cot)

	n9eReporter := reporter.DefaultHTTPReporter
	n9eReporter.DropEndpoint = true
	n9eReporter.Urls = []string{cfg.HttpURL}
	// 10s 上报周期
	n9eReporter.Ticker = time.Tick(10 * time.Second)
	registry.AddReporter(n9eReporter)

	go registry.Serve("localhost:9099")

	http.HandleFunc("/api/push", receive)
	go func() {
		http.ListenAndServe("localhost:8099", nil)
	}()

	registry.Run()
}
