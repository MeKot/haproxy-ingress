package haproxy

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	hatypes "github.com/jcmoraisjr/haproxy-ingress/pkg/haproxy/types"
	"github.com/jcmoraisjr/haproxy-ingress/pkg/haproxy/utils"
	"github.com/jcmoraisjr/haproxy-ingress/pkg/types"
	"strconv"
	"strings"
	"time"
)

type ControllerType string

const (
	PID      ControllerType = "PIDController"
	SimpleML ControllerType = "Simple ML"
)

type Controller interface {
	Update(backend *hatypes.Backend)
	NeedsReload() bool
	Reset()
}

type TargetConfig struct {
	Paths   []string         `json:"paths"`
	Targets map[string]int64 `json:"targets"`
}

type BrownoutConfig struct {
	Targets map[string]TargetConfig `json:"targets"`
}

func (i *instance) GetController(t ControllerType) Controller {
	var c BrownoutConfig
	i.logger.InfoV(2, "Trying to parse the config, which is: %q", i.curConfig.Brownout().Rules)
	_ = json.Unmarshal([]byte(i.curConfig.Brownout().Rules), &c)
	i.logger.InfoV(2, "%d configurations parsed\n", len(c.Targets))

	i.logger.Info("The map in config %p has the address of %p", &i.curConfig, &i.curConfig.Brownout().Rates)

	for _, value := range c.Targets {
		i.logger.Info("Initialising the global rates map")
		// Default rate limit is 1001 requests per 10s (or 100/s)
		for _, path := range value.Paths {
			r, ok := i.curConfig.Brownout().Rates[path]
			if !ok {
				i.logger.Info("Adding %q", path)
				i.curConfig.Brownout().Rates[path] = 1001
				continue
			}
			i.logger.Info("Found %q with set rate %d", path, r)
		}
	}
	switch t {
	case PID:
		return &PIDController{
			needsReload:    false,
			logger:         i.logger,
			lastUpdate:     time.Now(),
			reloadInterval: time.Second * 10,
			targets:        c.Targets,
			cmd:            utils.HAProxyCommandWithReturn,
			socket:         i.curConfig.Global().AdminSocket,
			metrics:        i.metrics,
			currConfig:     i.curConfig.(*config),
		}
	case SimpleML:
		return nil
	}
	return nil
}

// PIDController used to perform runtime updates
type PIDController struct {
	reloadInterval time.Duration
	lastUpdate     time.Time
	needsReload    bool
	logger         types.Logger
	targets        map[string]TargetConfig
	cmd            func(socket string, observer func(duration time.Duration), commands ...string) ([]string, error)
	socket         string
	metrics        types.Metrics
	currConfig     *config
}

// Coordinates metric collection and updates the config with any necessary action
func (c *PIDController) Update(backend *hatypes.Backend) {
	stats, err := c.readStats(backend.ID)
	if err != nil {
		c.logger.Error(err.Error())
		return
	}
	// Have to use ID here, as prometheus only exports backend IDs
	c.recordResponseTime(backend.ID, stats)

	if c.lastUpdate.Add(c.reloadInterval).After(time.Now()) {
		//c.logger.Info("Waiting before next update")
		return
	}

	c.execApplyACL(backend, c.getAdjustment(backend.Name, stats))
	if c.needsReload {
		c.logger.InfoV(2, "HAProxy will need to reload because of brownout")
	}
	c.lastUpdate = time.Now()
}

func (c *PIDController) NeedsReload() bool {
	return c.needsReload
}

func (c *PIDController) Reset() {
	c.needsReload = false
}

func (c *PIDController) execApplyACL(backend *hatypes.Backend, adjustment int) {
	for path := range c.currConfig.brownout.Rates {
		for _, p := range c.targets[backend.Name].Paths {
			if p == path {
				c.addRateLimitToConfig(path, adjustment)
			}
		}

	}
}

// Given the current error, returns the necessary adjustment for brownout ACL and rate limiting
func (c *PIDController) getAdjustment(backend string, stats map[string]string) int {
	// The PID controller
	response := 0

	//c.logger.InfoV(2, "Targets for backend are: %v", c.targets[backend].Targets)
	//c.logger.InfoV(2, "About to go into the loop for %d iterations", len(c.targets[backend].Targets))
	for metric, target := range c.targets[backend].Targets {
		//c.logger.InfoV(2, "Found a target for %q, which is %d", metric, target)
		if current, ok := stats[metric]; ok {
			//c.logger.InfoV(2, "Stats have the metric with value %q", current)
			cur, err := strconv.ParseInt(current, 10, 64)
			if err != nil {
				c.logger.Error("Failed to parse an int from %q", current)
				continue
			}

			//c.logger.InfoV(2, "Parsed the value to %d", cur)

			if cur < target {
				response--
				continue
			}
			if cur > target {
				response++
				continue
			}
		}
	}
	//c.logger.InfoV(2, "Response before conversion %d", response)
	// f_c(i) = 500(1 - i) + 1-> max control response gives us the limit of 0.1/s, min = 100/s
	response = (1-response)*500 + 1
	//c.logger.InfoV(2, "Calculated response to be %d", response)

	return response
}

// Sends the stats query to HAProxy for a given backend and returns a map of key-value stats
func (c *PIDController) readStats(id string) (map[string]string, error) {
	cmd := fmt.Sprintf("show stat %s 2 -1", id)
	msg, err := c.cmd(c.socket, c.metrics.HAProxySetServerResponseTime, cmd)
	if err != nil {
		c.logger.Error("Error collecting stats for backend %q, %s", id, err)
		return map[string]string{}, err
	}
	msg = strings.Split(msg[0], "\n")
	if len(msg) != 2 {
		return map[string]string{}, errors.New(fmt.Sprintf("Unexpected return from HAProxy %v", msg))
	}

	r := csv.NewReader(strings.NewReader(strings.Join(msg, "\n")))
	keys, err := r.Read()
	if err != nil {
		c.logger.Error("Failed to parse the keys")
		return map[string]string{}, err
	}
	//c.logger.InfoV(2, "Read %d keys from the csv", len(keys))

	values, err := r.Read()
	if err != nil {
		c.logger.Error("Failed to parse the values")
		return map[string]string{}, err
	}
	//c.logger.InfoV(2, "Read %d values from the csv", len(values))

	if len(values) != len(keys) {
		return map[string]string{}, errors.New("number of keys does not match the number of values")
	}

	var m = make(map[string]string, len(keys))
	for i, key := range keys {
		m[key] = values[i]
	}
	return m, nil
}

func (c *PIDController) recordResponseTime(backend string, stats map[string]string) {
	curr, ok := stats["rtime"]
	if !ok {
		c.logger.Error("rtime was not returned by HAProxy")
		return
	}
	rtime, err := time.ParseDuration(fmt.Sprintf("%s%s", stats["rtime"], "ms"))
	if err != nil {
		c.logger.Error("failed to parse rtime from %s", curr)
		return
	}

	c.metrics.SetBackendResponseTime(backend, rtime)

}

// Adds Rate Limiting ACL to the config, enforcing brownout at the LB level
func (c *PIDController) addRateLimitToConfig(path string, rate int) {
	curr := c.currConfig.brownout.Rates[path]
	if curr != rate {
		c.logger.InfoV(2, "Updated the path %q from %d to %d in the rates map %p", path, curr, rate,
			&c.currConfig.brownout.Rates)
		c.currConfig.brownout.Rates[path] = rate
		c.metrics.SetBrownOutFeatureStatus(path, float64(rate))
		c.needsReload = true
	}
}
