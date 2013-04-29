// A Go package to intelligently and flexibly pool among multiple hosts from your Go application.
// Host selection can operate in round robin or epsilon greedy mode, and unresponsive hosts are
// avoided. A good overview of Epsilon Greedy is here http://stevehanov.ca/blog/index.php?id=132
package hostpool

import (
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// Returns current version
func Version() string {
	return "0.1"
}

// This interface represents the response from HostPool. You can retrieve the
// hostname by calling Host(), and after making a request to the host you should
// call Mark with any error encountered, which will inform the HostPool issuing
// the HostPoolResponse of what happened to the request and allow it to update.
type HostPoolResponse interface {
	Host() string
	Mark(error)
}

type standardHostPoolResponse struct {
	replied int32
	host    string
	pool    *standardHostPool
}

func (r *standardHostPoolResponse) Host() string {
	return r.host
}

func (r *standardHostPoolResponse) Mark(err error) {
	if !atomic.CompareAndSwapInt32(&r.replied, 0, 1) {
		return
	}

	if err == nil {
		r.pool.markSuccess(r)
	} else {
		r.pool.markFailed(r)
	}
}

// This is the main HostPool interface. Structs implementing this interface
// allow you to Get a HostPoolResponse (which includes a hostname to use),
// get the list of all Hosts, and use ResetAll to reset state.
type HostPool interface {
	Get() HostPoolResponse
	ResetAll()
	Hosts() []string
}

type standardHostPool struct {
	sync.RWMutex
	hosts         map[string]*hostEntry
	hostList      []*hostEntry
	initialRetry  time.Duration
	maxRetry      time.Duration
	nextHostIndex int
}

// Construct a basic HostPool using the hostnames provided
func New(hosts []string, initialRetry time.Duration, maxRetry time.Duration) HostPool {
	p := &standardHostPool{
		hosts:        make(map[string]*hostEntry, len(hosts)),
		hostList:     make([]*hostEntry, len(hosts)),
		initialRetry: initialRetry,
		maxRetry:     maxRetry,
	}

	for i, h := range hosts {
		e := &hostEntry{
			host:       h,
			retryDelay: p.initialRetry,
		}
		p.hosts[h] = e
		p.hostList[i] = e
	}

	return p
}

func (p *standardHostPool) Hosts() []string {
	hosts := make([]string, len(p.hosts))
	for host, _ := range p.hosts {
		hosts = append(hosts, host)
	}
	return hosts
}

// return an entry from the HostPool
func (p *standardHostPool) Get() HostPoolResponse {
	p.Lock()
	defer p.Unlock()
	host := p.getRoundRobin()
	return &standardHostPoolResponse{host: host, pool: p}
}

func (p *standardHostPool) getRoundRobin() string {
	now := time.Now()
	hostCount := len(p.hostList)
	for i := 0; i < hostCount; i++ {
		currentIndex := (p.nextHostIndex + i) % hostCount
		h := p.hostList[currentIndex]
		if !h.dead {
			p.nextHostIndex = currentIndex + 1
			return h.host
		}
		if h.nextRetry.Before(now) {
			h.willRetryHost(p.maxRetry)
			p.nextHostIndex = currentIndex + 1
			return h.host
		}
	}

	// all hosts are down. re-add them
	p.doResetAll()
	p.nextHostIndex = 0
	return p.hostList[0].host
}

func (p *standardHostPool) ResetAll() {
	p.Lock()
	defer p.Unlock()
	p.doResetAll()
}

// this actually performs the logic to reset,
// and should only be called when the lock has
// already been acquired
func (p *standardHostPool) doResetAll() {
	for _, h := range p.hosts {
		h.dead = false
	}
}

func (p *standardHostPool) markSuccess(host string) {
	p.Lock()
	defer p.Unlock()

	h, ok := p.hosts[host]
	if !ok {
		log.Fatalf("host %s not in HostPool %v", host, p.Hosts())
	}
	h.dead = false
}

func (p *standardHostPool) markFailed(host string) {
	p.Lock()
	defer p.Unlock()

	h, ok := p.hosts[host]
	if !ok {
		log.Fatalf("host %s not in HostPool %v", host, p.Hosts())
	}
	if !h.dead {
		h.dead = true
		h.retryCount = 0
		h.retryDelay = p.initialRetry
		h.nextRetry = time.Now().Add(h.retryDelay)
	}
}
