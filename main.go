package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/antonlindstrom/mesos_stats"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/log"
)

const concurrentFetch = 100

// Commandline flags.
var (
	addr                 = flag.String("web.listen-address", ":9105", "Address to listen on for web interface and telemetry")
	autoDiscover         = flag.Bool("exporter.discovery", false, "Discover all Mesos slaves")
	localURL             = flag.String("exporter.local-url", "http://127.0.0.1:5051", "URL to the local Mesos slave")
	masterURL            = flag.String("exporter.discovery.master-url", "http://mesos-master.example.com:5050", "Mesos master URL")
	metricsPath          = flag.String("web.telemetry-path", "/metrics", "Path under which to expose metrics")
	scrapeInterval       = flag.Duration("exporter.interval", (60 * time.Second), "Scrape interval duration")
	scrapeMasterInterval = flag.Duration("exporter.master-interval", (10 * time.Minute), "Scrape master interval duration")
)

var (
	variableLabels = []string{"task", "slave", "framework_id", "framework_name"}

	cpuLimitDesc = prometheus.NewDesc(
		"mesos_task_cpu_limit",
		"Fractional CPU limit.",
		variableLabels, nil,
	)
	cpuSysDesc = prometheus.NewDesc(
		"mesos_task_cpu_system_seconds_total",
		"Cumulative system CPU time in seconds.",
		variableLabels, nil,
	)
	cpuUsrDesc = prometheus.NewDesc(
		"mesos_task_cpu_user_seconds_total",
		"Cumulative user CPU time in seconds.",
		variableLabels, nil,
	)
	memLimitDesc = prometheus.NewDesc(
		"mesos_task_memory_limit_bytes",
		"Task memory limit in bytes.",
		variableLabels, nil,
	)
	memRssDesc = prometheus.NewDesc(
		"mesos_task_memory_rss_bytes",
		"Task memory RSS usage in bytes.",
		variableLabels, nil,
	)

	frameworkVariableLabels = []string{"id", "name"}

	frameworkResourcesUsedCpusDesc = prometheus.NewDesc(
		"mesos_framework_resources_used_cpu",
		"CPUs used by all tasks of a framework",
		frameworkVariableLabels, nil,
	)

	frameworkResourcesUsedDiskDesc = prometheus.NewDesc(
		"mesos_framework_resources_used_disk_megabytes",
		"Disk space used by all tasks of a framework",
		frameworkVariableLabels, nil,
	)

	frameworkResourcesUsedMemDesc = prometheus.NewDesc(
		"mesos_framework_resources_used_memory_megabytes",
		"Memory used by all tasks of a framework",
		frameworkVariableLabels, nil,
	)

	slaveVariableLables = []string{"pid"}

	slaveResourcesCpusDesc = prometheus.NewDesc(
		"mesos_slave_resources_cpus",
		"CPUs advertised by a Mesos slave",
		slaveVariableLables, nil,
	)

	slaveResourcesDiskDesc = prometheus.NewDesc(
		"mesos_slave_resources_disk_megabytes",
		"Disk space advertised by a Mesos slave",
		slaveVariableLables, nil,
	)

	slaveResourcesMemDesc = prometheus.NewDesc(
		"mesos_slave_resources_memory_megabytes",
		"Memory advertised by a Mesos slave",
		slaveVariableLables, nil,
	)
)

var httpClient = http.Client{
	Timeout: 5 * time.Second,
}

type exporterOpts struct {
	autoDiscover   bool
	interval       time.Duration
	localURL       string
	masterInterval time.Duration
	masterURL      string
}

type framework struct {
	Active        bool
	Id            string
	Name          string
	UsedResources *resources `json:"used_resources"`
}

type periodicExporter struct {
	sync.RWMutex
	errors     *prometheus.CounterVec
	frameworks struct {
		sync.RWMutex
		mapping map[string]string
	}
	masterMetrics []prometheus.Metric
	masterURL     *url.URL
	metrics       []prometheus.Metric
	opts          *exporterOpts
	slaves        struct {
		sync.Mutex
		urls []string
	}
}

type resources struct {
	Cpus float64
	Disk float64
	Mem  float64
}

type slave struct {
	Active    bool   `json:"active"`
	Hostname  string `json:"hostname"`
	Pid       string `json:"pid"`
	Resources *resources
}

type state struct {
	Frameworks []*framework
	Slaves     []*slave
}

func newMesosExporter(opts *exporterOpts) *periodicExporter {
	e := &periodicExporter{
		errors: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "mesos_exporter",
				Name:      "slave_scrape_errors_total",
				Help:      "Current total scrape errors",
			},
			[]string{"slave"},
		),
		opts: opts,
	}
	e.slaves.urls = []string{e.opts.localURL}

	if e.opts.autoDiscover {
		log.Info("auto discovery enabled from command line flag.")

		parsedMasterURL, err := url.Parse(opts.masterURL)
		if err != nil {
			log.Fatalf("unable to parse master URL '%s': ", opts.masterURL, err)
		}
		if strings.HasPrefix(parsedMasterURL.Scheme, "http") == false {
			log.Fatalf("invalid scheme '%s' in master url - use 'http' or 'https'", parsedMasterURL.Scheme)
		}

		e.masterURL = parsedMasterURL

		// Update nr. of Mesos Slaves and metrics of the Mesos Master.
		e.scrapeMaster()
		go runEvery(e.scrapeMaster, e.opts.masterInterval)
	}

	// Fetch slave metrics every interval.
	go runEvery(e.scrapeSlaves, e.opts.interval)

	return e
}

func (e *periodicExporter) Describe(ch chan<- *prometheus.Desc) {
	e.rLockMetrics(func() {
		for _, m := range e.metrics {
			ch <- m.Desc()
		}

		for _, m := range e.masterMetrics {
			ch <- m.Desc()
		}
	})
	e.errors.MetricVec.Describe(ch)
}

func (e *periodicExporter) Collect(ch chan<- prometheus.Metric) {
	e.rLockMetrics(func() {
		for _, m := range e.metrics {
			ch <- m
		}

		for _, m := range e.masterMetrics {
			ch <- m
		}
	})
	e.errors.MetricVec.Collect(ch)
}

func (e *periodicExporter) fetch(urlChan <-chan string, metricsChan chan<- prometheus.Metric, wg *sync.WaitGroup) {
	defer wg.Done()

	for u := range urlChan {
		u, err := url.Parse(u)
		if err != nil {
			log.Warn("could not parse slave URL: ", err)
			continue
		}

		host, _, err := net.SplitHostPort(u.Host)
		if err != nil {
			log.Warn("could not parse network address: ", err)
			continue
		}

		monitorURL := fmt.Sprintf("%s/monitor/statistics.json", u)
		resp, err := httpClient.Get(monitorURL)
		if err != nil {
			log.Warn(err)
			e.errors.WithLabelValues(host).Inc()
			continue
		}
		defer resp.Body.Close()

		var stats []mesos_stats.Monitor
		if err = json.NewDecoder(resp.Body).Decode(&stats); err != nil {
			log.Warn("failed to deserialize response: ", err)
			e.errors.WithLabelValues(host).Inc()
			continue
		}

		frameworksMapping := make(map[string]string, len(e.frameworks.mapping))
		e.frameworks.RLock()
		for id, name := range e.frameworks.mapping {
			frameworksMapping[id] = name
		}
		e.frameworks.RUnlock()

		for _, stat := range stats {
			fwName, ok := frameworksMapping[stat.FrameworkId]
			// Do not record metrics of a task if its framework has not been discovered yet and the exporter is running in discovery mode.
			if ok == false && e.opts.autoDiscover {
				continue
			}

			metricsChan <- prometheus.MustNewConstMetric(
				cpuLimitDesc,
				prometheus.GaugeValue,
				float64(stat.Statistics.CpusLimit),
				stat.Source, host, stat.FrameworkId, fwName,
			)
			metricsChan <- prometheus.MustNewConstMetric(
				cpuSysDesc,
				prometheus.CounterValue,
				float64(stat.Statistics.CpusSystemTimeSecs),
				stat.Source, host, stat.FrameworkId, fwName,
			)
			metricsChan <- prometheus.MustNewConstMetric(
				cpuUsrDesc,
				prometheus.CounterValue,
				float64(stat.Statistics.CpusUserTimeSecs),
				stat.Source, host, stat.FrameworkId, fwName,
			)
			metricsChan <- prometheus.MustNewConstMetric(
				memLimitDesc,
				prometheus.GaugeValue,
				float64(stat.Statistics.MemLimitBytes),
				stat.Source, host, stat.FrameworkId, fwName,
			)
			metricsChan <- prometheus.MustNewConstMetric(
				memRssDesc,
				prometheus.GaugeValue,
				float64(stat.Statistics.MemRssBytes),
				stat.Source, host, stat.FrameworkId, fwName,
			)
		}
	}
}

func (e *periodicExporter) rLockMetrics(f func()) {
	e.RLock()
	defer e.RUnlock()
	f()
}

func (e *periodicExporter) setMetrics(ch chan prometheus.Metric) {
	metrics := make([]prometheus.Metric, 0)
	for metric := range ch {
		metrics = append(metrics, metric)
	}

	e.Lock()
	e.metrics = metrics
	e.Unlock()
}

func (e *periodicExporter) scrapeSlaves() {
	e.slaves.Lock()
	urls := make([]string, len(e.slaves.urls))
	copy(urls, e.slaves.urls)
	e.slaves.Unlock()

	urlCount := len(urls)
	log.Debugf("active slaves: %d", urlCount)

	urlChan := make(chan string)
	metricsChan := make(chan prometheus.Metric)
	go e.setMetrics(metricsChan)

	poolSize := concurrentFetch
	if urlCount < concurrentFetch {
		poolSize = urlCount
	}

	log.Debugf("creating fetch pool of size %d", poolSize)

	var wg sync.WaitGroup
	wg.Add(poolSize)
	for i := 0; i < poolSize; i++ {
		go e.fetch(urlChan, metricsChan, &wg)
	}

	for _, url := range urls {
		urlChan <- url
	}
	close(urlChan)

	wg.Wait()
	close(metricsChan)
}

func (e *periodicExporter) scrapeMaster() {
	masterMetrics := make([]prometheus.Metric, 0)
	log.Debug("scraping master...")

	// This will redirect us to the elected mesos master
	redirectURL := fmt.Sprintf("%s://%s/master/redirect", e.masterURL.Scheme, e.masterURL.Host)
	rReq, err := http.NewRequest("GET", redirectURL, nil)
	if err != nil {
		panic(err)
	}

	tr := http.Transport{
		DisableKeepAlives: true,
	}
	rresp, err := tr.RoundTrip(rReq)
	if err != nil {
		log.Warn(err)
		return
	}
	defer rresp.Body.Close()

	// This will/should return http://master.ip:5050
	masterLoc := rresp.Header.Get("Location")
	if masterLoc == "" {
		log.Warnf("%d response missing Location header", rresp.StatusCode)
		return
	}

	log.Debugf("current elected master at: %s", masterLoc)

	// Starting from 0.23.0, a Mesos Master does not set the scheme in the "Location" header.
	// Use the scheme from the master URL in this case.
	var stateURL string
	if strings.HasPrefix(masterLoc, "http") {
		stateURL = fmt.Sprintf("%s/master/state.json", masterLoc)
	} else {
		stateURL = fmt.Sprintf("%s:%s/master/state.json", e.masterURL.Scheme, masterLoc)
	}

	// Find all active slaves
	resp, err := http.Get(stateURL)
	if err != nil {
		log.Warn(err)
		return
	}
	defer resp.Body.Close()

	var req state

	if err := json.NewDecoder(resp.Body).Decode(&req); err != nil {
		log.Warnf("failed to deserialize request: %s", err)
		return
	}

	fwMapping := make(map[string]string, 0)
	for _, framework := range req.Frameworks {
		if framework.Active {
			fwMapping[framework.Id] = framework.Name

			frameworkMetrics := []prometheus.Metric{
				prometheus.MustNewConstMetric(
					frameworkResourcesUsedCpusDesc,
					prometheus.GaugeValue,
					framework.UsedResources.Cpus,
					framework.Id, framework.Name,
				),
				prometheus.MustNewConstMetric(
					frameworkResourcesUsedDiskDesc,
					prometheus.GaugeValue,
					framework.UsedResources.Disk,
					framework.Id, framework.Name,
				),
				prometheus.MustNewConstMetric(
					frameworkResourcesUsedMemDesc,
					prometheus.GaugeValue,
					framework.UsedResources.Mem,
					framework.Id, framework.Name,
				),
			}

			masterMetrics = append(masterMetrics, frameworkMetrics...)
		}
	}
	e.frameworks.Lock()
	e.frameworks.mapping = fwMapping
	e.frameworks.Unlock()

	var slaveURLs []string
	for _, slave := range req.Slaves {
		if slave.Active {
			// Extract slave port from pid.
			_, port, err := net.SplitHostPort(slave.Pid)
			if err != nil {
				port = "5051"
			}
			url := fmt.Sprintf("http://%s:%s", slave.Hostname, port)

			slaveURLs = append(slaveURLs, url)

			slaveMetrics := []prometheus.Metric{
				prometheus.MustNewConstMetric(
					slaveResourcesCpusDesc,
					prometheus.GaugeValue,
					slave.Resources.Cpus,
					slave.Pid,
				),
				prometheus.MustNewConstMetric(
					slaveResourcesDiskDesc,
					prometheus.GaugeValue,
					slave.Resources.Disk,
					slave.Pid,
				),
				prometheus.MustNewConstMetric(
					slaveResourcesMemDesc,
					prometheus.GaugeValue,
					slave.Resources.Mem,
					slave.Pid,
				),
			}

			masterMetrics = append(masterMetrics, slaveMetrics...)
		}
	}

	log.Debugf("%d slaves discovered", len(slaveURLs))

	e.Lock()
	e.masterMetrics = masterMetrics
	e.Unlock()

	e.slaves.Lock()
	e.slaves.urls = slaveURLs
	e.slaves.Unlock()
}

func runEvery(f func(), interval time.Duration) {
	for _ = range time.NewTicker(interval).C {
		f()
	}
}

func main() {
	flag.Parse()

	opts := &exporterOpts{
		autoDiscover:   *autoDiscover,
		interval:       *scrapeInterval,
		localURL:       strings.TrimRight(*localURL, "/"),
		masterInterval: *scrapeMasterInterval,
		masterURL:      strings.TrimRight(*masterURL, "/"),
	}
	exporter := newMesosExporter(opts)
	prometheus.MustRegister(exporter)

	http.Handle(*metricsPath, prometheus.Handler())
	http.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "OK")
	})
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, *metricsPath, http.StatusMovedPermanently)
	})

	log.Info("starting mesos_exporter on ", *addr)

	log.Fatal(http.ListenAndServe(*addr, nil))
}
