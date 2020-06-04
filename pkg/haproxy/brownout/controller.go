package brownout

import (
	"encoding/csv"
	"encoding/json"
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
}

type TargetConfig struct {
	Paths  []string       `json:"paths"`
	Limits map[string]int `json:"limits"`
}

type Config struct {
	Targets map[string]TargetConfig `json:"targets"`
}

func GetController(t ControllerType, socket string, config string, logger types.Logger, metrics types.Metrics) Controller {
	var c Config
	logger.InfoV(2, "Trying to parse the config, which is: %q", config)
	_ = json.Unmarshal([]byte(config), &c)
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
			socket:        socket,
			metrics:       metrics,
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
	disabledPaths map[string][]string
	cmd           func(socket string, observer func(duration time.Duration), commands ...string) ([]string, error)
	socket        string
	metrics       types.Metrics
}

func (c *PIDController) Update(backend *hatypes.Backend) {
	c.logger.InfoV(2, "Updating the backend %q", backend.Name)
	c.logger.InfoV(2, "The backend has the ID of: %q", backend.ID)
	c.logger.InfoV(2, "Current config is:")
	c.logger.InfoV(2, "%+v", c.targets)
	c.execApplyACL(backend)
}

func (c *PIDController) execApplyACL(backend *hatypes.Backend) bool {
	cmd := fmt.Sprintf("show stat %s 2 -1", backend.ID)
	c.logger.InfoV(2, "Querying HAProxy for the backend response time")
	c.logger.InfoV(2, "Query used is %q", cmd)
	msg, err := c.cmd(c.socket, c.metrics.HAProxySetServerResponseTime, cmd)
	if err != nil {
		c.logger.Error("error collecting response time metrics for backend %q", backend.Name)
		return false
	}
	c.logger.InfoV(2, "This is the response from the socket: %v", msg)
	msg = strings.Split(msg[0], "\n")
	for _, i := range msg {
		c.logger.InfoV(2, "%q", i)
	}

	if len(msg) != 2 {
		c.logger.Warn("Unexpected return from HAProxy")
		return false
	}

	r := csv.NewReader(strings.NewReader(strings.Join(msg, "\n")))
	keys, err := r.Read()
	if err != nil {
		return false
	}
	c.logger.InfoV(2, "Read %d keys from the csv", len(keys))
	c.logger.InfoV(2, "%v", keys)
	values, err := r.Read()
	if err != nil {
		return false
	}
	c.logger.InfoV(2, "Read %d values from the csv", len(values))
	c.logger.InfoV(2, "%v", values)

	for i, key := range keys {
		if limit, found := c.targets[backend.Name].Limits[key]; found {
			currentValue, err := strconv.Atoi(values[i])
			if err != nil {
				c.logger.Warn("Failed to parse the limit %q with the value %q as an integer", key, values[i])
				currentValue = 0
			}
			c.logger.InfoV(2, "Limit of %d for %q configured, the current value is %d", limit, key, currentValue)
			nonEssentialPaths := c.targets[backend.Name].Paths
			if limit < currentValue && len(nonEssentialPaths) > len(c.disabledPaths[backend.Name]) {
				c.logger.InfoV(2, "There are still some paths that can be disabled")
				diffMap := make(map[string]int, len(c.disabledPaths[backend.Name]))
				for _, x := range c.disabledPaths[backend.Name] {
					diffMap[x]++
				}
				for _, path := range nonEssentialPaths {
					if _, found := diffMap[path]; !found {
						//cmd := fmt.Sprintf("show stat %s 2 -1", backend.Name)
						c.logger.InfoV(2, "Disabling the path: %q", path)
						return true
					}
				}
			}
		}
	}

	return false
}
