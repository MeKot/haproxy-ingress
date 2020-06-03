package brownout

import (
	"encoding/json"
	"github.com/jcmoraisjr/haproxy-ingress/pkg/haproxy/utils"
	"github.com/jcmoraisjr/haproxy-ingress/pkg/types"
	"time"
)

type ControllerType string

const (
	PID      ControllerType = "PIDController"
	SimpleML ControllerType = "Simple ML"
)

type Controller interface {
	Update(backend string)
}

type TargetConfig struct {
	Paths  []string       `json:"paths"`
	Limits map[string]int `json:"limits"`
}

type Config struct {
	Targets map[string]TargetConfig `json:"targets"`
}

func GetController(t ControllerType, socket string, config string, logger types.Logger) Controller {
	var c Config
	logger.Info("Trying to parse the config, which is: %q", config)
	_ = json.Unmarshal([]byte(config), &c)
	logger.Info("The config was parsed:\n")
	for key, value := range c.Targets {
		logger.Info("%q : %+v", key, value)
	}
	switch t {
	case PID:
		return &PIDController{
			logger:        logger,
			targets:       c.Targets,
			disabledPaths: nil,
			cmd:           utils.HAProxyCommand,
			socket:        socket,
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
}

func (c *PIDController) Update(backend string) {
	c.logger.Info("Updating the backend %q", backend)
	c.logger.Info("Current config is:")
	c.logger.Info("%+v", c.targets)
}
