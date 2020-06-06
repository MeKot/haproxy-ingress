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
	logger := i.logger
	logger.InfoV(2, "Trying to parse the config, which is: %q", i.curConfig.Brownout().Rules)
	_ = json.Unmarshal([]byte(i.curConfig.Brownout().Rules), &c)
	logger.InfoV(2, "The config was parsed:\n")
	logger.InfoV(2, "%d configurations parsed\n", len(c.Targets))
	for key, value := range c.Targets {
		logger.InfoV(2, "%q : %+v\n\n", key, value)
	}
	switch t {
	case PID:
		return &PIDController{
			needsReload:   false,
			logger:        logger,
			targets:       c.Targets,
			disabledPaths: nil,
			cmd:           utils.HAProxyCommandWithReturn,
			socket:        i.curConfig.Global().AdminSocket,
			metrics:       i.metrics,
			currConfig:    i.curConfig.(*config),
		}
	case SimpleML:
		return nil
	}
	return nil
}

// PIDController used to perform runtime updates
type PIDController struct {
	needsReload   bool
	logger        types.Logger
	targets       map[string]TargetConfig
	disabledPaths map[string]map[string]float32
	cmd           func(socket string, observer func(duration time.Duration), commands ...string) ([]string, error)
	socket        string
	metrics       types.Metrics
	currConfig    *config
}

// Coordinates metric collection and updates the config with any necessary action
func (c *PIDController) Update(backend *hatypes.Backend) {
	c.logger.InfoV(2, "The backend has the ID of: %q", backend.ID)
	c.logger.InfoV(2, "Current config is:")
	c.logger.InfoV(2, "%+v", c.targets)

	stats, err := c.readStats(backend.ID)
	if err != nil {
		c.logger.Error(err.Error())
		return
	}

	// Have to use ID here, as prometheus only exports backend IDs
	c.recordResponseTime(backend.ID, stats)

	c.needsReload = c.execApplyACL(backend, c.getAdjustment(backend.Name, stats))
}

func (c *PIDController) NeedsReload() bool {
	return c.needsReload
}

func (c *PIDController) execApplyACL(backend *hatypes.Backend, adjustment int) bool {
	nonEssentialPaths := c.targets[backend.Name].Paths

	c.logger.InfoV(2, "There are still some paths that can be disabled for %q", backend.Name)

	if adjustment > 501 {
		for _, path := range nonEssentialPaths {
			if _, found := c.disabledPaths[backend.Name][path]; !found {
				c.logger.InfoV(2, "Setting the rate %d for path %q on %q", adjustment, path, backend.Name)
				c.addRateLimitToConfig(path, adjustment)
				return true
			}
		}
	}
	if adjustment < 501 {
		for path := range c.disabledPaths[backend.Name] {
			c.logger.InfoV(2, "Setting the rate %d for path %q on %q", adjustment, path, backend.Name)
			c.addRateLimitToConfig(path, adjustment)
			return true
		}
	}
	return false
}

// Given the current error, returns the necessary adjustment for brownout ACL and rate limiting
func (c *PIDController) getAdjustment(backend string, stats map[string]string) int {
	// The PID controller
	response := 0

	c.logger.InfoV(2, "Targets for backend are: %v", c.targets[backend].Targets)
	c.logger.InfoV(2, "About to go into the loop for %d iterations", len(c.targets[backend].Targets))
	for metric, target := range c.targets[backend].Targets {
		c.logger.InfoV(2, "Found a target for %q, which is %d", metric, target)
		if current, ok := stats[metric]; ok {
			c.logger.InfoV(2, "Stats have the metric with value %q", current)
			cur, err := strconv.ParseInt(current, 10, 64)
			if err != nil {
				c.logger.Error("Failed to parse an int from %q", current)
				continue
			}

			c.logger.InfoV(2, "Parsed the value to %d", cur)

			if cur < target {
				c.logger.InfoV(2, "Decrementing response")
				response--
				continue
			}
			if cur > target {
				c.logger.InfoV(2, "Incrementing response")
				response++
				continue
			}
		}
	}
	c.logger.InfoV(2, "Response before conversion %d", response)
	// f_c(i) = 500(1 - i) + 1-> max control response gives us the limit of 0.1/s, min = 100/s
	response = (1-response)*500 + 1
	c.logger.InfoV(2, "Calculated response to be %d", response)

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
	c.logger.InfoV(2, "Read %d keys from the csv", len(keys))

	values, err := r.Read()
	if err != nil {
		c.logger.Error("Failed to parse the values")
		return map[string]string{}, err
	}
	c.logger.InfoV(2, "Read %d values from the csv", len(values))

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
	c.currConfig.brownout.Rates[path] = rate
}
