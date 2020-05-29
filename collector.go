package collector

import (
	"errors"
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/chenjiandongx/aura"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/grobie/gomemcache/memcache"
)

const (
	namespace           = "memcached"
	subsystemLruCrawler = "lru_crawler"
	subsystemSlab       = "slab"
)

var errKeyNotFound = errors.New("key not found")

// Collector collects metrics from a memcached server.
type Collector struct {
	address  string
	endpoint string
	port     string

	timeout time.Duration
	logger  log.Logger

	up                       *aura.Desc
	uptime                   *aura.Desc
	time                     *aura.Desc
	version                  *aura.Desc
	bytesRead                *aura.Desc
	bytesWritten             *aura.Desc
	currentConnections       *aura.Desc
	maxConnections           *aura.Desc
	connectionsTotal         *aura.Desc
	connsYieldedTotal        *aura.Desc
	listenerDisabledTotal    *aura.Desc
	currentBytes             *aura.Desc
	limitBytes               *aura.Desc
	commands                 *aura.Desc
	items                    *aura.Desc
	itemsTotal               *aura.Desc
	evictions                *aura.Desc
	reclaimed                *aura.Desc
	lruCrawlerEnabled        *aura.Desc
	lruCrawlerSleep          *aura.Desc
	lruCrawlerMaxItems       *aura.Desc
	lruMaintainerThread      *aura.Desc
	lruHotPercent            *aura.Desc
	lruWarmPercent           *aura.Desc
	lruHotMaxAgeFactor       *aura.Desc
	lruWarmMaxAgeFactor      *aura.Desc
	lruCrawlerStarts         *aura.Desc
	lruCrawlerReclaimed      *aura.Desc
	lruCrawlerItemsChecked   *aura.Desc
	lruCrawlerMovesToCold    *aura.Desc
	lruCrawlerMovesToWarm    *aura.Desc
	lruCrawlerMovesWithinLru *aura.Desc
	malloced                 *aura.Desc
	itemsNumber              *aura.Desc
	itemsAge                 *aura.Desc
	itemsCrawlerReclaimed    *aura.Desc
	itemsEvicted             *aura.Desc
	itemsEvictedNonzero      *aura.Desc
	itemsEvictedTime         *aura.Desc
	itemsEvictedUnfetched    *aura.Desc
	itemsExpiredUnfetched    *aura.Desc
	itemsOutofmemory         *aura.Desc
	itemsReclaimed           *aura.Desc
	itemsTailrepairs         *aura.Desc
	itemsMovesToCold         *aura.Desc
	itemsMovesToWarm         *aura.Desc
	itemsMovesWithinLru      *aura.Desc
	itemsHot                 *aura.Desc
	itemsWarm                *aura.Desc
	itemsCold                *aura.Desc
	itemsTemporary           *aura.Desc
	itemsAgeOldestHot        *aura.Desc
	itemsAgeOldestWarm       *aura.Desc
	itemsLruHits             *aura.Desc
	slabsChunkSize           *aura.Desc
	slabsChunksPerPage       *aura.Desc
	slabsCurrentPages        *aura.Desc
	slabsCurrentChunks       *aura.Desc
	slabsChunksUsed          *aura.Desc
	slabsChunksFree          *aura.Desc
	slabsChunksFreeEnd       *aura.Desc
	slabsMemRequested        *aura.Desc
	slabsCommands            *aura.Desc
}

const step = 20

// NewCollector returns an initialized exporter.
func NewCollector(server string, timeout time.Duration, logger log.Logger) *Collector {
	addr, err := net.ResolveTCPAddr("tcp", server)
	if err != nil {
		panic(fmt.Sprintf("failed to resolve address: %s", server))
	}

	return &Collector{
		address:  server,
		endpoint: addr.IP.String(),
		port:     fmt.Sprintf("%d", addr.Port),
		timeout:  timeout,
		logger:   logger,
		up: aura.NewDesc(
			aura.BuildFQName(namespace, "", "up"),
			"Could the memcached server be reached.",
			step,
			[]string{"endpoint", "port"},
		),
		uptime: aura.NewDesc(
			aura.BuildFQName(namespace, "", "uptime_seconds"),
			"Number of seconds since the server started.",
			step,
			[]string{"endpoint", "port"},
		),
		time: aura.NewDesc(
			aura.BuildFQName(namespace, "", "time_seconds"),
			"current UNIX time according to the server.",
			step,
			[]string{"endpoint", "port"},
		),
		version: aura.NewDesc(
			aura.BuildFQName(namespace, "", "version"),
			"The version of this memcached server.",
			step,
			[]string{"endpoint", "port", "version"},
		),
		bytesRead: aura.NewDesc(
			aura.BuildFQName(namespace, "", "read_bytes_total"),
			"Total number of bytes read by this server from network.",
			step,
			[]string{"endpoint", "port"},
		),
		bytesWritten: aura.NewDesc(
			aura.BuildFQName(namespace, "", "written_bytes_total"),
			"Total number of bytes sent by this server to network.",
			step,
			[]string{"endpoint", "port"},
		),
		currentConnections: aura.NewDesc(
			aura.BuildFQName(namespace, "", "current_connections"),
			"Current number of open connections.",
			step,
			[]string{"endpoint", "port"},
		),
		maxConnections: aura.NewDesc(
			aura.BuildFQName(namespace, "", "max_connections"),
			"Maximum number of clients allowed.",
			step,
			[]string{"endpoint", "port"},
		),
		connectionsTotal: aura.NewDesc(
			aura.BuildFQName(namespace, "", "connections_total"),
			"Total number of connections opened since the server started running.",
			step,
			[]string{"endpoint", "port"},
		),
		connsYieldedTotal: aura.NewDesc(
			aura.BuildFQName(namespace, "", "connections_yielded_total"),
			"Total number of connections yielded running due to hitting the memcached's -R limit.",
			step,
			[]string{"endpoint", "port"},
		),
		listenerDisabledTotal: aura.NewDesc(
			aura.BuildFQName(namespace, "", "connections_listener_disabled_total"),
			"Number of times that memcached has hit its connections limit and disabled its listener.",
			step,
			[]string{"endpoint", "port"},
		),
		currentBytes: aura.NewDesc(
			aura.BuildFQName(namespace, "", "current_bytes"),
			"Current number of bytes used to store items.",
			step,
			[]string{"endpoint", "port"},
		),
		limitBytes: aura.NewDesc(
			aura.BuildFQName(namespace, "", "limit_bytes"),
			"Number of bytes this server is allowed to use for storage.",
			step,
			[]string{"endpoint", "port"},
		),
		commands: aura.NewDesc(
			aura.BuildFQName(namespace, "", "commands_total"),
			"Total number of all requests broken down by command (get, set, etc.) and status.",
			step,
			[]string{"endpoint", "port", "command", "status"},
		),
		items: aura.NewDesc(
			aura.BuildFQName(namespace, "", "current_items"),
			"Current number of items stored by this instance.",
			step,
			[]string{"endpoint", "port"},
		),
		itemsTotal: aura.NewDesc(
			aura.BuildFQName(namespace, "", "items_total"),
			"Total number of items stored during the life of this instance.",
			step,
			[]string{"endpoint", "port"},
		),
		evictions: aura.NewDesc(
			aura.BuildFQName(namespace, "", "items_evicted_total"),
			"Total number of valid items removed from cache to free memory for new items.",
			step,
			[]string{"endpoint", "port"},
		),
		reclaimed: aura.NewDesc(
			aura.BuildFQName(namespace, "", "items_reclaimed_total"),
			"Total number of times an entry was stored using memory from an expired entry.",
			step,
			[]string{"endpoint", "port"},
		),
		lruCrawlerEnabled: aura.NewDesc(
			aura.BuildFQName(namespace, subsystemLruCrawler, "enabled"),
			"Whether the LRU crawler is enabled.",
			step,
			[]string{"endpoint", "port"},
		),
		lruCrawlerSleep: aura.NewDesc(
			aura.BuildFQName(namespace, subsystemLruCrawler, "sleep"),
			"Microseconds to sleep between LRU crawls.",
			step,
			[]string{"endpoint", "port"},
		),
		lruCrawlerMaxItems: aura.NewDesc(
			aura.BuildFQName(namespace, subsystemLruCrawler, "to_crawl"),
			"Max items to crawl per slab per run.",
			step,
			[]string{"endpoint", "port"},
		),
		lruMaintainerThread: aura.NewDesc(
			aura.BuildFQName(namespace, subsystemLruCrawler, "maintainer_thread"),
			"Split LRU mode and background threads.",
			step,
			[]string{"endpoint", "port"},
		),
		lruHotPercent: aura.NewDesc(
			aura.BuildFQName(namespace, subsystemLruCrawler, "hot_percent"),
			"Percent of slab memory reserved for HOT LRU.",
			step,
			[]string{"endpoint", "port"},
		),
		lruWarmPercent: aura.NewDesc(
			aura.BuildFQName(namespace, subsystemLruCrawler, "warm_percent"),
			"Percent of slab memory reserved for WARM LRU.",
			step,
			[]string{"endpoint", "port"},
		),
		lruHotMaxAgeFactor: aura.NewDesc(
			aura.BuildFQName(namespace, subsystemLruCrawler, "hot_max_factor"),
			"Set idle age of HOT LRU to COLD age * this",
			step,
			[]string{"endpoint", "port"},
		),
		lruWarmMaxAgeFactor: aura.NewDesc(
			aura.BuildFQName(namespace, subsystemLruCrawler, "warm_max_factor"),
			"Set idle age of WARM LRU to COLD age * this",
			step,
			[]string{"endpoint", "port"},
		),
		lruCrawlerStarts: aura.NewDesc(
			aura.BuildFQName("namespace", subsystemLruCrawler, "starts_total"),
			"Times an LRU crawler was started.",
			step,
			[]string{"endpoint", "port"},
		),
		lruCrawlerReclaimed: aura.NewDesc(
			aura.BuildFQName(namespace, subsystemLruCrawler, "reclaimed_total"),
			"Total items freed by LRU Crawler.",
			step,
			[]string{"endpoint", "port"},
		),
		lruCrawlerItemsChecked: aura.NewDesc(
			aura.BuildFQName(namespace, subsystemLruCrawler, "items_checked_total"),
			"Total items examined by LRU Crawler.",
			step,
			[]string{"endpoint", "port"},
		),
		lruCrawlerMovesToCold: aura.NewDesc(
			aura.BuildFQName(namespace, subsystemLruCrawler, "moves_to_cold_total"),
			"Total number of items moved from HOT/WARM to COLD LRU's.",
			step,
			[]string{"endpoint", "port"},
		),
		lruCrawlerMovesToWarm: aura.NewDesc(
			aura.BuildFQName(namespace, subsystemLruCrawler, "moves_to_warm_total"),
			"Total number of items moved from COLD to WARM LRU.",
			step,
			[]string{"endpoint", "port"},
		),
		lruCrawlerMovesWithinLru: aura.NewDesc(
			aura.BuildFQName(namespace, subsystemLruCrawler, "moves_within_lru_total"),
			"Total number of items reshuffled within HOT or WARM LRU's.",
			step,
			[]string{"endpoint", "port"},
		),
		malloced: aura.NewDesc(
			aura.BuildFQName(namespace, "", "malloced_bytes"),
			"Number of bytes of memory allocated to slab pages.",
			step,
			[]string{"endpoint", "port"},
		),
		itemsNumber: aura.NewDesc(
			aura.BuildFQName(namespace, subsystemSlab, "current_items"),
			"Number of items currently stored in this slab class.",
			step,
			[]string{"endpoint", "port", "slab"},
		),
		itemsAge: aura.NewDesc(
			aura.BuildFQName(namespace, subsystemSlab, "items_age_seconds"),
			"Number of seconds the oldest item has been in the slab class.",
			step,
			[]string{"endpoint", "port", "slab"},
		),
		itemsCrawlerReclaimed: aura.NewDesc(
			aura.BuildFQName(namespace, subsystemSlab, "items_crawler_reclaimed_total"),
			"Number of items freed by the LRU Crawler.",
			step,
			[]string{"endpoint", "port", "slab"},
		),
		itemsEvicted: aura.NewDesc(
			aura.BuildFQName(namespace, subsystemSlab, "items_evicted_total"),
			"Total number of times an item had to be evicted from the LRU before it expired.",
			step,
			[]string{"endpoint", "port", "slab"},
		),
		itemsEvictedNonzero: aura.NewDesc(
			aura.BuildFQName(namespace, subsystemSlab, "items_evicted_nonzero_total"),
			"Total number of times an item which had an explicit expire time set had to be evicted from the LRU before it expired.",
			step,
			[]string{"endpoint", "port", "slab"},
		),
		itemsEvictedTime: aura.NewDesc(
			aura.BuildFQName(namespace, subsystemSlab, "items_evicted_time_seconds"),
			"Seconds since the last access for the most recent item evicted from this class.",
			step,
			[]string{"endpoint", "port", "slab"},
		),
		itemsEvictedUnfetched: aura.NewDesc(
			aura.BuildFQName(namespace, subsystemSlab, "items_evicted_unfetched_total"),
			"Total nmber of items evicted and never fetched.",
			step,
			[]string{"endpoint", "port", "slab"},
		),
		itemsExpiredUnfetched: aura.NewDesc(
			aura.BuildFQName(namespace, subsystemSlab, "items_expired_unfetched_total"),
			"Total number of valid items evicted from the LRU which were never touched after being set.",
			step,
			[]string{"endpoint", "port", "slab"},
		),
		itemsOutofmemory: aura.NewDesc(
			aura.BuildFQName(namespace, subsystemSlab, "items_outofmemory_total"),
			"Total number of items for this slab class that have triggered an out of memory error.",
			step,
			[]string{"endpoint", "port", "slab"},
		),
		itemsReclaimed: aura.NewDesc(
			aura.BuildFQName(namespace, subsystemSlab, "items_reclaimed_total"),
			"Total number of items reclaimed.",
			step,
			[]string{"endpoint", "port", "slab"},
		),
		itemsTailrepairs: aura.NewDesc(
			aura.BuildFQName(namespace, subsystemSlab, "items_tailrepairs_total"),
			"Total number of times the entries for a particular ID need repairing.",
			step,
			[]string{"endpoint", "port", "slab"},
		),
		itemsMovesToCold: aura.NewDesc(
			aura.BuildFQName(namespace, subsystemSlab, "items_moves_to_cold"),
			"Number of items moved from HOT or WARM into COLD.",
			step,
			[]string{"endpoint", "port", "slab"},
		),
		itemsMovesToWarm: aura.NewDesc(
			aura.BuildFQName(namespace, subsystemSlab, "items_moves_to_warm"),
			"Number of items moves from COLD into WARM.",
			step,
			[]string{"endpoint", "port", "slab"},
		),
		itemsMovesWithinLru: aura.NewDesc(
			aura.BuildFQName(namespace, subsystemSlab, "items_moves_within_lru"),
			"Number of times active items were bumped within HOT or WARM.",
			step,
			[]string{"endpoint", "port", "slab"},
		),
		itemsHot: aura.NewDesc(
			aura.BuildFQName(namespace, subsystemSlab, "hot_items"),
			"Number of items presently stored in the HOT LRU.",
			step,
			[]string{"endpoint", "port", "slab"},
		),
		itemsWarm: aura.NewDesc(
			aura.BuildFQName(namespace, subsystemSlab, "warm_items"),
			"Number of items presently stored in the WARM LRU.",
			step,
			[]string{"endpoint", "port", "slab"},
		),
		itemsCold: aura.NewDesc(
			aura.BuildFQName(namespace, subsystemSlab, "cold_items"),
			"Number of items presently stored in the COLD LRU.",
			step,
			[]string{"endpoint", "port", "slab"},
		),
		itemsTemporary: aura.NewDesc(
			aura.BuildFQName(namespace, subsystemSlab, "temporary_items"),
			"Number of items presently stored in the TEMPORARY LRU.",
			step,
			[]string{"endpoint", "port", "slab"},
		),
		itemsAgeOldestHot: aura.NewDesc(
			aura.BuildFQName(namespace, subsystemSlab, "hot_age_seconds"),
			"Age of the oldest item in HOT LRU.",
			step,
			[]string{"endpoint", "port", "slab"},
		),
		itemsAgeOldestWarm: aura.NewDesc(
			aura.BuildFQName(namespace, subsystemSlab, "warm_age_seconds"),
			"Age of the oldest item in HOT LRU.",
			step,
			[]string{"endpoint", "port", "slab"},
		),
		itemsLruHits: aura.NewDesc(
			aura.BuildFQName(namespace, subsystemSlab, "lru_hits_total"),
			"Number of get_hits to the LRU.",
			step,
			[]string{"endpoint", "port", "slab", "lru"},
		),
		slabsChunkSize: aura.NewDesc(
			aura.BuildFQName(namespace, subsystemSlab, "chunk_size_bytes"),
			"Number of bytes allocated to each chunk within this slab class.",
			step,
			[]string{"endpoint", "port", "slab"},
		),
		slabsChunksPerPage: aura.NewDesc(
			aura.BuildFQName(namespace, subsystemSlab, "chunks_per_page"),
			"Number of chunks within a single page for this slab class.",
			step,
			[]string{"endpoint", "port", "slab"},
		),
		slabsCurrentPages: aura.NewDesc(
			aura.BuildFQName(namespace, subsystemSlab, "current_pages"),
			"Number of pages allocated to this slab class.",
			step,
			[]string{"endpoint", "port", "slab"},
		),
		slabsCurrentChunks: aura.NewDesc(
			aura.BuildFQName(namespace, subsystemSlab, "current_chunks"),
			"Number of chunks allocated to this slab class.",
			step,
			[]string{"endpoint", "port", "slab"},
		),
		slabsChunksUsed: aura.NewDesc(
			aura.BuildFQName(namespace, subsystemSlab, "chunks_used"),
			"Number of chunks allocated to an item.",
			step,
			[]string{"endpoint", "port", "slab"},
		),
		slabsChunksFree: aura.NewDesc(
			aura.BuildFQName(namespace, subsystemSlab, "chunks_free"),
			"Number of chunks not yet allocated items.",
			step,
			[]string{"endpoint", "port", "slab"},
		),
		slabsChunksFreeEnd: aura.NewDesc(
			aura.BuildFQName(namespace, subsystemSlab, "chunks_free_end"),
			"Number of free chunks at the end of the last allocated page.",
			step,
			[]string{"endpoint", "port", "slab"},
		),
		slabsMemRequested: aura.NewDesc(
			aura.BuildFQName(namespace, subsystemSlab, "mem_requested_bytes"),
			"Number of bytes of memory actual items take up within a slab.",
			step,
			[]string{"endpoint", "port", "slab"},
		),
		slabsCommands: aura.NewDesc(
			aura.BuildFQName(namespace, subsystemSlab, "commands_total"),
			"Total number of all requests broken down by command (get, set, etc.) and status per slab.",
			step,
			[]string{"endpoint", "port", "slab", "command", "status"},
		),
	}
}

func (co *Collector) Interval() time.Duration {
	return 20 * time.Second
}

// Describe describes all the metrics exported by the memcached exporter. It
// implements aura.Collector.
func (co *Collector) Describe(ch chan<- *aura.Desc) {
	ch <- co.up
	ch <- co.uptime
	ch <- co.time
	ch <- co.version
	ch <- co.bytesRead
	ch <- co.bytesWritten
	ch <- co.currentConnections
	ch <- co.maxConnections
	ch <- co.connectionsTotal
	ch <- co.connsYieldedTotal
	ch <- co.listenerDisabledTotal
	ch <- co.currentBytes
	ch <- co.limitBytes
	ch <- co.commands
	ch <- co.items
	ch <- co.itemsTotal
	ch <- co.evictions
	ch <- co.reclaimed
	ch <- co.lruCrawlerEnabled
	ch <- co.lruCrawlerSleep
	ch <- co.lruCrawlerMaxItems
	ch <- co.lruMaintainerThread
	ch <- co.lruHotPercent
	ch <- co.lruWarmPercent
	ch <- co.lruHotMaxAgeFactor
	ch <- co.lruWarmMaxAgeFactor
	ch <- co.lruCrawlerStarts
	ch <- co.lruCrawlerReclaimed
	ch <- co.lruCrawlerItemsChecked
	ch <- co.lruCrawlerMovesToCold
	ch <- co.lruCrawlerMovesToWarm
	ch <- co.lruCrawlerMovesWithinLru
	ch <- co.itemsLruHits
	ch <- co.malloced
	ch <- co.itemsNumber
	ch <- co.itemsAge
	ch <- co.itemsCrawlerReclaimed
	ch <- co.itemsEvicted
	ch <- co.itemsEvictedNonzero
	ch <- co.itemsEvictedTime
	ch <- co.itemsEvictedUnfetched
	ch <- co.itemsExpiredUnfetched
	ch <- co.itemsOutofmemory
	ch <- co.itemsReclaimed
	ch <- co.itemsTailrepairs
	ch <- co.itemsMovesToCold
	ch <- co.itemsMovesToWarm
	ch <- co.itemsMovesWithinLru
	ch <- co.itemsHot
	ch <- co.itemsWarm
	ch <- co.itemsCold
	ch <- co.itemsTemporary
	ch <- co.itemsAgeOldestHot
	ch <- co.itemsAgeOldestWarm
	ch <- co.slabsChunkSize
	ch <- co.slabsChunksPerPage
	ch <- co.slabsCurrentPages
	ch <- co.slabsCurrentChunks
	ch <- co.slabsChunksUsed
	ch <- co.slabsChunksFree
	ch <- co.slabsChunksFreeEnd
	ch <- co.slabsMemRequested
	ch <- co.slabsCommands
}

// Collect fetches the statistics from the configured memcached server, and
// delivers them as Prometheus metrics. It implements aura.Collector.
func (co *Collector) Collect(ch chan<- aura.Metric) {
	c, err := memcache.New(co.address)
	if err != nil {
		ch <- aura.MustNewConstMetric(co.up, aura.GaugeValue, 0, co.endpoint, co.port)
		level.Error(co.logger).Log("msg", "Failed to connect to memcached", "err", err)
		return
	}
	c.Timeout = co.timeout

	up := float64(1)
	stats, err := c.Stats()
	if err != nil {
		level.Error(co.logger).Log("msg", "Failed to collect stats from memcached", "err", err)
		up = 0
	}
	statsSettings, err := c.StatsSettings()
	if err != nil {
		level.Error(co.logger).Log("msg", "Could not query stats settings", "err", err)
		up = 0
	}

	if err := co.parseStats(ch, stats); err != nil {
		up = 0
	}
	if err := co.parseStatsSettings(ch, statsSettings); err != nil {
		up = 0
	}

	ch <- aura.MustNewConstMetric(co.up, aura.GaugeValue, up, co.endpoint, co.port)
}

func (co *Collector) parseStats(ch chan<- aura.Metric, stats map[net.Addr]memcache.Stats) error {
	// TODO(ts): Clean up and consolidate metric mappings.
	itemsCounterMetrics := map[string]*aura.Desc{
		"crawler_reclaimed": co.itemsCrawlerReclaimed,
		"evicted":           co.itemsEvicted,
		"evicted_nonzero":   co.itemsEvictedNonzero,
		"evicted_time":      co.itemsEvictedTime,
		"evicted_unfetched": co.itemsEvictedUnfetched,
		"expired_unfetched": co.itemsExpiredUnfetched,
		"outofmemory":       co.itemsOutofmemory,
		"reclaimed":         co.itemsReclaimed,
		"tailrepairs":       co.itemsTailrepairs,
		"mem_requested":     co.slabsMemRequested,
		"moves_to_cold":     co.itemsMovesToCold,
		"moves_to_warm":     co.itemsMovesToWarm,
		"moves_within_lru":  co.itemsMovesWithinLru,
	}

	itemsGaugeMetrics := map[string]*aura.Desc{
		"number_hot":  co.itemsHot,
		"number_warm": co.itemsWarm,
		"number_cold": co.itemsCold,
		"number_temp": co.itemsTemporary,
		"age_hot":     co.itemsAgeOldestHot,
		"age_warm":    co.itemsAgeOldestWarm,
	}

	var parseError error
	for _, t := range stats {
		s := t.Stats
		ch <- aura.MustNewConstMetric(co.version, aura.GaugeValue, 1, co.endpoint, co.port, s["version"])

		for _, op := range []string{"get", "delete", "incr", "decr", "cas", "touch"} {
			err := firstError(
				co.parseAndNewMetric(ch, co.commands, aura.CounterValue, s, op+"_hits", op, "hit"),
				co.parseAndNewMetric(ch, co.commands, aura.CounterValue, s, op+"_misses", op, "miss"),
			)
			if err != nil {
				parseError = err
			}
		}
		err := firstError(
			co.parseAndNewMetric(ch, co.uptime, aura.CounterValue, s, "uptime"),
			co.parseAndNewMetric(ch, co.time, aura.GaugeValue, s, "time"),
			co.parseAndNewMetric(ch, co.commands, aura.CounterValue, s, "cas_badval", "cas", "badval"),
			co.parseAndNewMetric(ch, co.commands, aura.CounterValue, s, "cmd_flush", "flush", "hit"),
		)
		if err != nil {
			parseError = err
		}

		// memcached includes cas operations again in cmd_set.
		setCmd, err := parse(s, "cmd_set", co.logger)
		if err == nil {
			if cas, casErr := sum(s, "cas_misses", "cas_hits", "cas_badval"); casErr == nil {
				ch <- aura.MustNewConstMetric(co.commands, aura.CounterValue, setCmd-cas, co.endpoint, co.port, "set", "hit")
			} else {
				level.Error(co.logger).Log("msg", "Failed to parse cas", "err", casErr)
				parseError = casErr
			}
		} else {
			level.Error(co.logger).Log("msg", "Failed to parse set", "err", err)
			parseError = err
		}

		err = firstError(
			co.parseAndNewMetric(ch, co.currentBytes, aura.GaugeValue, s, "bytes"),
			co.parseAndNewMetric(ch, co.limitBytes, aura.GaugeValue, s, "limit_maxbytes"),
			co.parseAndNewMetric(ch, co.items, aura.GaugeValue, s, "curr_items"),
			co.parseAndNewMetric(ch, co.itemsTotal, aura.CounterValue, s, "total_items"),
			co.parseAndNewMetric(ch, co.bytesRead, aura.CounterValue, s, "bytes_read"),
			co.parseAndNewMetric(ch, co.bytesWritten, aura.CounterValue, s, "bytes_written"),
			co.parseAndNewMetric(ch, co.currentConnections, aura.GaugeValue, s, "curr_connections"),
			co.parseAndNewMetric(ch, co.connectionsTotal, aura.CounterValue, s, "total_connections"),
			co.parseAndNewMetric(ch, co.connsYieldedTotal, aura.CounterValue, s, "conn_yields"),
			co.parseAndNewMetric(ch, co.listenerDisabledTotal, aura.CounterValue, s, "listen_disabled_num"),
			co.parseAndNewMetric(ch, co.evictions, aura.CounterValue, s, "evictions"),
			co.parseAndNewMetric(ch, co.reclaimed, aura.CounterValue, s, "reclaimed"),
			co.parseAndNewMetric(ch, co.lruCrawlerStarts, aura.CounterValue, s, "lru_crawler_starts"),
			co.parseAndNewMetric(ch, co.lruCrawlerItemsChecked, aura.CounterValue, s, "crawler_items_checked"),
			co.parseAndNewMetric(ch, co.lruCrawlerReclaimed, aura.CounterValue, s, "crawler_reclaimed"),
			co.parseAndNewMetric(ch, co.lruCrawlerMovesToCold, aura.CounterValue, s, "moves_to_cold"),
			co.parseAndNewMetric(ch, co.lruCrawlerMovesToWarm, aura.CounterValue, s, "moves_to_warm"),
			co.parseAndNewMetric(ch, co.lruCrawlerMovesWithinLru, aura.CounterValue, s, "moves_within_lru"),
			co.parseAndNewMetric(ch, co.malloced, aura.GaugeValue, s, "total_malloced"),
		)
		if err != nil {
			parseError = err
		}

		for slab, u := range t.Items {
			slab := strconv.Itoa(slab)
			err := firstError(
				co.parseAndNewMetric(ch, co.itemsNumber, aura.GaugeValue, u, "number", slab),
				co.parseAndNewMetric(ch, co.itemsAge, aura.GaugeValue, u, "age", slab),
				co.parseAndNewMetric(ch, co.itemsLruHits, aura.CounterValue, u, "hits_to_hot", slab, "hot"),
				co.parseAndNewMetric(ch, co.itemsLruHits, aura.CounterValue, u, "hits_to_warm", slab, "warm"),
				co.parseAndNewMetric(ch, co.itemsLruHits, aura.CounterValue, u, "hits_to_cold", slab, "cold"),
				co.parseAndNewMetric(ch, co.itemsLruHits, aura.CounterValue, u, "hits_to_temp", slab, "temporary"),
			)
			if err != nil {
				parseError = err
			}
			for m, d := range itemsCounterMetrics {
				if _, ok := u[m]; !ok {
					continue
				}
				if err := co.parseAndNewMetric(ch, d, aura.CounterValue, u, m, slab); err != nil {
					parseError = err
				}
			}
			for m, d := range itemsGaugeMetrics {
				if _, ok := u[m]; !ok {
					continue
				}
				if err := co.parseAndNewMetric(ch, d, aura.GaugeValue, u, m, slab); err != nil {
					parseError = err
				}
			}
		}

		for slab, v := range t.Slabs {
			slab := strconv.Itoa(slab)

			for _, op := range []string{"get", "delete", "incr", "decr", "cas", "touch"} {
				if err := co.parseAndNewMetric(ch, co.slabsCommands, aura.CounterValue, v, op+"_hits", slab, op, "hit"); err != nil {
					parseError = err
				}
			}
			if err := co.parseAndNewMetric(ch, co.slabsCommands, aura.CounterValue, v, "cas_badval", slab, "cas", "badval"); err != nil {
				parseError = err
			}

			slabSetCmd, err := parse(v, "cmd_set", co.logger)
			if err == nil {
				if slabCas, slabCasErr := sum(v, "cas_hits", "cas_badval"); slabCasErr == nil {
					ch <- aura.MustNewConstMetric(
						co.slabsCommands, aura.CounterValue, slabSetCmd-slabCas, co.endpoint, co.port, slab, "set", "hit")
				} else {
					level.Error(co.logger).Log("msg", "Failed to parse cas", "err", slabCasErr)
					parseError = slabCasErr
				}
			} else {
				level.Error(co.logger).Log("msg", "Failed to parse set", "err", err)
				parseError = err
			}

			err = firstError(
				co.parseAndNewMetric(ch, co.slabsChunkSize, aura.GaugeValue, v, "chunk_size", slab),
				co.parseAndNewMetric(ch, co.slabsChunksPerPage, aura.GaugeValue, v, "chunks_per_page", slab),
				co.parseAndNewMetric(ch, co.slabsCurrentPages, aura.GaugeValue, v, "total_pages", slab),
				co.parseAndNewMetric(ch, co.slabsCurrentChunks, aura.GaugeValue, v, "total_chunks", slab),
				co.parseAndNewMetric(ch, co.slabsChunksUsed, aura.GaugeValue, v, "used_chunks", slab),
				co.parseAndNewMetric(ch, co.slabsChunksFree, aura.GaugeValue, v, "free_chunks", slab),
				co.parseAndNewMetric(ch, co.slabsChunksFreeEnd, aura.GaugeValue, v, "free_chunks_end", slab),
				co.parseAndNewMetric(ch, co.slabsMemRequested, aura.GaugeValue, v, "mem_requested", slab),
			)
			if err != nil {
				parseError = err
			}
		}
	}

	return parseError
}

func (co *Collector) parseStatsSettings(ch chan<- aura.Metric, statsSettings map[net.Addr]map[string]string) error {
	var parseError error
	for _, settings := range statsSettings {
		if err := co.parseAndNewMetric(ch, co.maxConnections, aura.GaugeValue, settings, "maxconns"); err != nil {
			parseError = err
		}

		if v, ok := settings["lru_crawler"]; ok && v == "yes" {
			err := firstError(
				co.parseBoolAndNewMetric(ch, co.lruCrawlerEnabled, aura.GaugeValue, settings, "lru_crawler"),
				co.parseAndNewMetric(ch, co.lruCrawlerSleep, aura.GaugeValue, settings, "lru_crawler_sleep"),
				co.parseAndNewMetric(ch, co.lruCrawlerMaxItems, aura.GaugeValue, settings, "lru_crawler_tocrawl"),
				co.parseBoolAndNewMetric(ch, co.lruMaintainerThread, aura.GaugeValue, settings, "lru_maintainer_thread"),
				co.parseAndNewMetric(ch, co.lruHotPercent, aura.GaugeValue, settings, "hot_lru_pct"),
				co.parseAndNewMetric(ch, co.lruWarmPercent, aura.GaugeValue, settings, "warm_lru_pct"),
				co.parseAndNewMetric(ch, co.lruHotMaxAgeFactor, aura.GaugeValue, settings, "hot_max_factor"),
				co.parseAndNewMetric(ch, co.lruWarmMaxAgeFactor, aura.GaugeValue, settings, "warm_max_factor"),
			)
			if err != nil {
				parseError = err
			}
		}
	}
	return parseError
}

func (co *Collector) parseAndNewMetric(
	ch chan<- aura.Metric, desc *aura.Desc, valueType aura.ValueType,
	stats map[string]string, key string, labelValues ...string,
) error {
	labelValues = append([]string{co.endpoint, co.port}, labelValues...)
	return co.extractValueAndNewMetric(ch, desc, valueType, parse, stats, key, labelValues...)
}

func (co *Collector) parseBoolAndNewMetric(
	ch chan<- aura.Metric, desc *aura.Desc, valueType aura.ValueType,
	stats map[string]string, key string, labelValues ...string,
) error {
	labelValues = append([]string{co.endpoint, co.port}, labelValues...)
	return co.extractValueAndNewMetric(ch, desc, valueType, parseBool, stats, key, labelValues...)
}

func (co *Collector) extractValueAndNewMetric(
	ch chan<- aura.Metric, desc *aura.Desc, valueType aura.ValueType,
	f func(map[string]string, string, log.Logger) (float64, error),
	stats map[string]string, key string, labelValues ...string,
) error {
	v, err := f(stats, key, co.logger)
	if err == errKeyNotFound {
		return nil
	}
	if err != nil {
		return err
	}

	ch <- aura.MustNewConstMetric(desc, valueType, v, labelValues...)
	return nil
}

func parse(stats map[string]string, key string, logger log.Logger) (float64, error) {
	value, ok := stats[key]
	if !ok {
		level.Debug(logger).Log("msg", "Key not found", "key", key)
		return 0, errKeyNotFound
	}

	v, err := strconv.ParseFloat(value, 64)
	if err != nil {
		level.Error(logger).Log("msg", "Failed to parse", "key", key, "value", value, "err", err)
		return 0, err
	}
	return v, nil
}

func parseBool(stats map[string]string, key string, logger log.Logger) (float64, error) {
	value, ok := stats[key]
	if !ok {
		level.Debug(logger).Log("msg", "Key not found", "key", key)
		return 0, errKeyNotFound
	}

	switch value {
	case "yes":
		return 1, nil
	case "no":
		return 0, nil
	default:
		level.Error(logger).Log("msg", "Failed to parse", "key", key, "value", value)
		return 0, errors.New("failed parse a bool value")
	}
}

func sum(stats map[string]string, keys ...string) (float64, error) {
	s := 0.
	for _, key := range keys {
		if _, ok := stats[key]; !ok {
			return 0, errKeyNotFound
		}
		v, err := strconv.ParseFloat(stats[key], 64)
		if err != nil {
			return 0, err
		}
		s += v
	}
	return s, nil
}

func firstError(errors ...error) error {
	for _, v := range errors {
		if v != nil {
			return v
		}
	}
	return nil
}
