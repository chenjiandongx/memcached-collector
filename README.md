# Memcached-Collector

> 📤 Collect metrics from memcached servers for consumption by aura.

Memcached-Collector **完全借鉴**了 [prometheus/memcached_exporter](https://github.com/prometheus/memcached_exporter) 的代码，感谢 Prometheus 官方维护了众多 exporter。本项目是利用 [aura](https://github.com/chenjiandongx/aura) 将其转换为 falcon/nightingale 支持的采集形式，并提供数据上报能力。

aura 是一套为 [falcon](https://github.com/open-falcon/falcon-plus)/[nightingale](https://github.com/didi/nightingale) 服务的 SDK，但其实 aura 支持任意后端，用户可以为其定制各类 Reporter，详细信息请参考 aura 文档。

## 📝 使用

测试用途可以使用 docker 先部署一个 memcached 服务。
```shell
$ docker run -p 11211:11211 -d memcached
```

**使用代码直接构建运行**
```golang
// main.go
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

// Run Directly
func main() {
	registry := aura.NewRegistry(nil)

	// 192.168.2.21:11211 是我部署的测试机的 memcached 服务的地址
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

// $go run main.go
```

**使用配置文件运行**

config.json
```json
{
  "memcachedServer": "192.168.2.21:11211",
  "httpUrl": "http://localhost:8099/api/push"
}
```

执行二进制构建 `go build -o collector .`
```golang
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
	n9eReporter.Ticker = time.Tick(30 * time.Second)
	registry.AddReporter(n9eReporter)

	go registry.Serve("localhost:9099")

	http.HandleFunc("/api/push", receive)
	go func() {
		http.ListenAndServe("localhost:8099", nil)
	}()

	registry.Run()
}

// $./collector -c config.json
```

**数据上报**

```shell
~/project/golang/src/github.com/chenjiandongx/memcached-collector/example 🤔 ./collector -c config.json
metrics:{Endpoint:192.168.2.21 Metric:memcached.version Step:20 Value:1 Type:Gauge Tags:port=11211,version=1.6.6 Timestamp:1590776904}
metrics:{Endpoint:192.168.2.21 Metric:memcached.commands_total Step:20 Value:0 Type:Counter Tags:command=delete,port=11211,status=miss Timestamp:1590776904}
metrics:{Endpoint:192.168.2.21 Metric:memcached.commands_total Step:20 Value:0 Type:Counter Tags:command=decr,port=11211,status=miss Timestamp:1590776904}
metrics:{Endpoint:192.168.2.21 Metric:memcached.commands_total Step:20 Value:0 Type:Counter Tags:command=cas,port=11211,status=hit Timestamp:1590776904}
metrics:{Endpoint:192.168.2.21 Metric:memcached.commands_total Step:20 Value:0 Type:Counter Tags:command=cas,port=11211,status=miss Timestamp:1590776904}
metrics:{Endpoint:192.168.2.21 Metric:memcached.commands_total Step:20 Value:0 Type:Counter Tags:command=touch,port=11211,status=hit Timestamp:1590776904}
metrics:{Endpoint:192.168.2.21 Metric:memcached.commands_total Step:20 Value:0 Type:Counter Tags:command=touch,port=11211,status=miss Timestamp:1590776904}
metrics:{Endpoint:192.168.2.21 Metric:memcached.uptime_seconds Step:20 Value:3370 Type:Counter Tags:port=11211 Timestamp:1590776904}
metrics:{Endpoint:192.168.2.21 Metric:memcached.time_seconds Step:20 Value:1.590776903e+09 Type:Gauge Tags:port=11211 Timestamp:1590776904}
metrics:{Endpoint:192.168.2.21 Metric:memcached.commands_total Step:20 Value:0 Type:Counter Tags:command=cas,port=11211,status=badval Timestamp:1590776904}
metrics:{Endpoint:192.168.2.21 Metric:memcached.commands_total Step:20 Value:0 Type:Counter Tags:command=flush,port=11211,status=hit Timestamp:1590776904}
metrics:{Endpoint:192.168.2.21 Metric:memcached.commands_total Step:20 Value:0 Type:Counter Tags:command=set,port=11211,status=hit Timestamp:1590776904}
metrics:{Endpoint:192.168.2.21 Metric:memcached.current_bytes Step:20 Value:0 Type:Gauge Tags:port=11211 Timestamp:1590776904}
metrics:{Endpoint:192.168.2.21 Metric:memcached.limit_bytes Step:20 Value:6.7108864e+07 Type:Gauge Tags:port=11211 Timestamp:1590776904}
metrics:{Endpoint:192.168.2.21 Metric:memcached.current_items Step:20 Value:0 Type:Gauge Tags:port=11211 Timestamp:1590776904}
metrics:{Endpoint:192.168.2.21 Metric:memcached.items_total Step:20 Value:0 Type:Counter Tags:port=11211 Timestamp:1590776904}
metrics:{Endpoint:192.168.2.21 Metric:memcached.read_bytes_total Step:20 Value:43106 Type:Counter Tags:port=11211 Timestamp:1590776904}
metrics:{Endpoint:192.168.2.21 Metric:memcached.connections_listener_disabled_total Step:20 Value:0 Type:Counter Tags:port=11211 Timestamp:1590776904}
metrics:{Endpoint:192.168.2.21 Metric:memcached.items_evicted_total Step:20 Value:0 Type:Counter Tags:port=11211 Timestamp:1590776904}
metrics:{Endpoint:192.168.2.21 Metric:memcached.items_reclaimed_total Step:20 Value:0 Type:Counter Tags:port=11211 Timestamp:1590776904}
metrics:{Endpoint:192.168.2.21 Metric:namespace.lru_crawler.starts_total Step:20 Value:2550 Type:Counter Tags:port=11211 Timestamp:1590776904}
metrics:{Endpoint:192.168.2.21 Metric:memcached.lru_crawler.items_checked_total Step:20 Value:0 Type:Counter Tags:port=11211 Timestamp:1590776904}
metrics:{Endpoint:192.168.2.21 Metric:memcached.lru_crawler.reclaimed_total Step:20 Value:0 Type:Counter Tags:port=11211 Timestamp:1590776904}
```

**指标及运行状态**

![image](https://user-images.githubusercontent.com/19553554/83293294-2a436580-a21e-11ea-93f6-0a94548fa549.png)

### 📃 License

Apache License [©chenjiandongx](https://github.com/chenjiandongx)
