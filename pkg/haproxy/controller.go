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
	Update(backend *hatypes.Backend) bool
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
	logger.InfoV(2, "Trying to parse the config, which is: %q", i.curConfig.Global().BrownoutRules)
	_ = json.Unmarshal([]byte(i.curConfig.Global().BrownoutRules), &c)
	logger.InfoV(2, "The config was parsed:\n")
	logger.InfoV(2, "%d configurations parsed\n", len(c.Targets))
	for key, value := range c.Targets {
		logger.InfoV(2, "%q : %+v\n\n", key, value)
	}
	switch t {
	case PID:
		return &PIDController{
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
	logger        types.Logger
	targets       map[string]TargetConfig
	disabledPaths map[string]map[string]float32
	cmd           func(socket string, observer func(duration time.Duration), commands ...string) ([]string, error)
	socket        string
	metrics       types.Metrics
	currConfig    *config
}

// Coordinates metric collection and updates the config with any necessary action
func (c *PIDController) Update(backend *hatypes.Backend) bool {
	c.logger.InfoV(2, "Updating the backend %q", backend.Name)
	c.logger.InfoV(2, "The backend has the ID of: %q", backend.ID)
	c.logger.InfoV(2, "Current config is:")
	c.logger.InfoV(2, "%+v", c.targets)

	stats, err := c.readStats(backend.ID)
	if err != nil {
		c.logger.Error(err.Error())
		return false
	}

	c.recordResponseTime(backend.Name, stats)

	return c.execApplyACL(backend, c.getAdjustment(backend.ID, stats))
}

func (c *PIDController) execApplyACL(backend *hatypes.Backend, adjustment float64) bool {
	nonEssentialPaths := c.targets[backend.Name].Paths

	if len(nonEssentialPaths) < len(c.disabledPaths[backend.Name]) {
		c.logger.Warn("no more paths to disable for %q", backend.Name)
		return false
	}
	c.logger.InfoV(2, "There are still some paths that can be disabled for %q", backend.Name)

	if adjustment > 0 {
		for _, path := range nonEssentialPaths {
			if _, found := c.disabledPaths[backend.Name][path]; !found {
				c.logger.InfoV(2, "Disabling the path: %q for backend", path, backend.Name)
				//cmd := fmt.Sprintf("show stat %s 2 -1", backend.Name)
				return true
			}
		}
	}
	return false
}

// Given the current error, returns the necessary adjustment for brownout ACL and rate limiting
func (c *PIDController) getAdjustment(backend string, stats map[string]string) float64 {
	// The PID controller
	var response float64 = 0

	for metric, target := range c.targets[backend].Targets {
		if current, ok := stats[metric]; ok {
			t, err := strconv.ParseInt(current, 10, 64)
			if err != nil {
				c.logger.Error("Failed to parse an int from %q", current)
				continue
			}

			if t < target {
				response++
				continue
			}
			if t > target {
				response--
				continue
			}
		}
	}

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
	c.logger.InfoV(2, "HAProxy response for keys: %v", msg[0])
	c.logger.InfoV(2, "HAProxy response for values: %v", msg[1])

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
