package haproxy

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/jcmoraisjr/haproxy-ingress/pkg/brownout"
	hatypes "github.com/jcmoraisjr/haproxy-ingress/pkg/haproxy/types"
	"github.com/jcmoraisjr/haproxy-ingress/pkg/haproxy/utils"
	"github.com/jcmoraisjr/haproxy-ingress/pkg/types"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"strconv"
	"strings"
	"time"
)

type ControllerType string

type Controller interface {
	Update(backend *hatypes.Backend)
}

// Represents the per-target configuration, where target is the deployment that
// is scaled browned out
type TargetConfig struct {
	Paths               []string     `json:"paths"`
	RequestLimit        int          `josn:"request_limit"`
	Target              string       `json:"target"`
	TargetValue         int          `json:"target_value"`
	TargetReplicas      int          `json:"target_replicas"`
	MaxReplicas         int          `json:"max_replicas"`
	DeploymentNamespace string       `json:"deployment_namespace"`
	DeploymentName      string       `json:"deployment_name"`
	ScalerPID           brownout.PID `json:"scaler_pid"`
	DimmerPID           brownout.PID `json:"dimmer_pid"`
}

type BrownoutConfig struct {
	Targets map[string]TargetConfig `json:"targets"`
}

// controller used to perform runtime updates
type controller struct {
	reloadInterval    time.Duration
	lastUpdate        time.Time
	lastScalingUpdate time.Time
	scalingInterval   time.Duration
	scalingHysteris   time.Duration
	needsReload       bool
	logger            types.Logger
	targets           map[string]TargetConfig
	cmd               func(socket string, observer func(duration time.Duration), commands ...string) ([]string, error)
	socket            string
	metrics           types.Metrics
	currConfig        *config
	dimmers           map[string]brownout.Controller
	scalers           map[string]brownout.Controller
}

func (i *instance) GetController() Controller {
	var c BrownoutConfig
	err := json.Unmarshal([]byte(i.curConfig.Brownout().Rules), &c)

	if err != nil {
		i.logger.Error("Failed to unmarshal!!")
		i.logger.Error(err.Error())
	}

	i.logger.Info("Unmarshalled to %+v", c)

	for _, configuration := range c.Targets {
		i.logger.Info("Initialising the global rates map")
		// Default rate limit is 100000 requests per minute
		for _, path := range configuration.Paths {
			r, ok := i.curConfig.Brownout().Rates[path]
			if !ok {
				i.logger.Info("Adding %q", path)
				i.curConfig.Brownout().Rates[path] = 500
				continue
			}
			i.logger.Info("Found %q with set rate %d", path, r)
		}

		// Update the scaling parameters
		v, ok := i.curConfig.Brownout().UpdateDeployments[configuration.DeploymentName]

		if ok {
			i.logger.Info("Deployment %q has %f replicas", configuration.DeploymentName, v)
		}

		if !ok {
			// Set to target replicas, so we update them on the next scalability action
			i.curConfig.Brownout().UpdateDeployments[configuration.DeploymentName] = float64(configuration.TargetReplicas)
			i.logger.Info("Det the number of replicas for %q at %d", configuration.DeploymentName, configuration.TargetReplicas)
		}
	}

	if i.brownout != nil {
		// Reusing the controller that has already been initialised, just updating some values
		i.brownout.(*controller).currConfig = i.curConfig.(*config)
		i.brownout.(*controller).targets = c.Targets
		return i.brownout
	}

	out := &controller{
		needsReload:       false,
		logger:            i.logger,
		lastUpdate:        time.Now(),
		lastScalingUpdate: time.Now(),
		scalingInterval:   time.Minute * 1,
		scalingHysteris:   time.Minute * 2,
		reloadInterval:    time.Second * 10,
		targets:           c.Targets,
		cmd:               utils.HAProxyCommandWithReturn,
		socket:            i.curConfig.Global().AdminSocket,
		metrics:           i.metrics,
		currConfig:        i.curConfig.(*config),
		dimmers:           make(map[string]brownout.Controller),
		scalers:           make(map[string]brownout.Controller),
	}
	for deployment, conf := range c.Targets {
		out.createScalerController(conf, deployment)
		out.createDimmerController(conf, deployment)
	}
	return out
}

func (c *controller) createDimmerController(conf TargetConfig, deployment string) {
	conf.DimmerPID.Initialise(float64(conf.RequestLimit), float64(conf.TargetValue))
	c.dimmers[deployment] = &brownout.PIDController{
		OutLimits:         brownout.CreateLimits(float64(conf.RequestLimit), 0),
		OxLimits:          brownout.CreateLimits(1e6, -1e6),
		IntervalBased:     false,
		AutoTuningEnabled: false,
		Metrics:           c.metrics,
		MetricLabel:       fmt.Sprintf("dimmer-%q", deployment),
		DeploymentName:    deployment,
	}
	c.dimmers[deployment].SetController(conf.DimmerPID)
	c.logger.Info("Created Dimmer Controller, the result is %+v", c.dimmers[deployment])
}

func (c *controller) createScalerController(conf TargetConfig, deployment string) {
	conf.ScalerPID.Initialise(float64(conf.TargetReplicas), float64(conf.TargetReplicas))
	c.scalers[deployment] = &brownout.PIDController{
		OutLimits:         brownout.CreateLimits(float64(conf.MaxReplicas), float64(conf.TargetReplicas)),
		OxLimits:          brownout.CreateLimits(1, float64(conf.RequestLimit)),
		IntervalBased:     true,
		AutoTuningEnabled: false,
		Metrics:           c.metrics,
		MetricLabel:       fmt.Sprintf("scaler-%q", deployment),
		DeploymentName:    deployment,
	}
	c.scalers[deployment].SetController(conf.ScalerPID)
	c.logger.Info("Created Scaler Controller, the result is %+v", c.scalers[deployment])
}

// Coordinates metric collection and updates the config with any necessary action
func (c *controller) Update(backend *hatypes.Backend) {
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

	c.execApplyACL(backend, c.getDimmerAdjustment(backend.Name, stats))
	if c.needsReload {
		c.logger.InfoV(2, "Queued updates to be written to disks on next reload")
		c.needsReload = false
	}
	c.lastUpdate = time.Now()

	// If we have scaled recently, we need to wait before scaling again
	if c.lastScalingUpdate.Add(c.scalingHysteris).After(time.Now()) || c.lastScalingUpdate.Add(c.scalingInterval).
		After(time.Now()) {
		return
	}

	c.logger.Info("In the Update, the targets are %+v", c.targets)

	for deployment, config := range c.targets {
		c.logger.Info("Considering deployment %q for scaling", config.DeploymentName)
		c.currConfig.brownout.UpdateDeployments[config.DeploymentName] =
			c.getScalerAdjustment(c.currConfig.brownout.Rates[config.Paths[0]], deployment)

		c.logger.Info("Set deployment %q to %f replicas", config.DeploymentName,
			c.currConfig.brownout.UpdateDeployments[config.DeploymentName])
	}

	c.updateDeployments()
}

func (c *controller) execApplyACL(backend *hatypes.Backend, adjustment int) {
	for path := range c.currConfig.brownout.Rates {
		for _, p := range c.targets[backend.Name].Paths {
			if p == path {
				c.addRateLimitToConfig(path, adjustment, backend.Name)
			}
		}
	}
}

// Given the current error, returns the necessary number of replicas
func (c *controller) getScalerAdjustment(current int, deployment string) float64 {
	c.logger.Info("Scaler goal is %f, current is %d", c.dimmers[deployment].GetTargetValue(), current)
	c.scalers[deployment].SetGoal(c.dimmers[deployment].GetTargetValue())
	return c.scalers[deployment].Next(float64(current), time.Now().Sub(c.lastScalingUpdate))
}

// Given the current error, returns the necessary adjustment for brownout ACL and rate limiting
func (c *controller) getDimmerAdjustment(backend string, stats map[string]string) int {
	// The PID controller
	response := 0

	if current, ok := stats[c.targets[backend].Target]; ok {
		cur, err := strconv.ParseFloat(current, 64)
		if err != nil {
			c.logger.Error("Failed to parse an int from %q", current)
		}
		c.dimmers[backend].SetGoal(float64(c.targets[backend].TargetValue))

		// Casting to int, as this directly corresponds to the rate for all non-essential endpoints
		response = int(c.dimmers[backend].Next(cur, time.Now().Sub(c.lastUpdate)))
	}

	return response
}

// Sends the stats query to HAProxy for a given backend and returns a map of key-value stats
func (c *controller) readStats(id string) (map[string]string, error) {
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

func (c *controller) recordResponseTime(backend string, stats map[string]string) {
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
func (c *controller) addRateLimitToConfig(path string, rate int, deployment string) {
	curr := c.currConfig.brownout.Rates[path]
	if curr != rate {
		c.logger.InfoV(2, "Updated the path %q from %d to %d in the rates map %p", path, curr, rate,
			&c.currConfig.brownout.Rates)
		c.currConfig.brownout.Rates[path] = rate
		c.updateBrownoutMap(path, rate)
		c.needsReload = true
	}
	c.metrics.SetBrownOutFeatureStatus(path, float64(rate), deployment)
}

func (c *controller) updateBrownoutMap(path string, adjustment int) {
	cmd := fmt.Sprintf("set map /etc/haproxy/maps/_brownout_rates.map %s %d", path, adjustment)
	_, err := c.cmd(c.socket, c.metrics.HAProxySetServerResponseTime, cmd)
	if err != nil {
		c.logger.Error("error setting the map value for path %q dynamically", path, err)
	}
}

func (c *controller) updateDeployments() {
	c.logger.Info("Updating Deployments to %+v", c.currConfig.brownout.UpdateDeployments)
	for depl, repl := range c.currConfig.brownout.UpdateDeployments {
		d, err := c.currConfig.brownout.Client.AppsV1().Deployments("default").Get(depl, metav1.GetOptions{})

		if err != nil {
			c.logger.Error("could not get the deployement for %q", depl)
			c.logger.Error(err.Error())
			continue
		}

		desired := c.getReplicaCount(int(*d.Spec.Replicas), repl)
		c.logger.Info("getReplicaCount returned %d", desired)

		if int(*d.Spec.Replicas) != desired {
			*d.Spec.Replicas = int32(desired)
			_, err = c.currConfig.brownout.Client.AppsV1().Deployments("default").Update(d)

			if err != nil {
				c.logger.Error("error updating the deployment %q", depl)
				c.logger.Error(err.Error())
			}

			c.lastScalingUpdate = time.Now()
		}
		c.metrics.SetBackendNumberOfPods(depl, *d.Spec.Replicas)

	}
}

func (c *controller) getReplicaCount(current int, scaler float64) int {
	diff := float64(current) - scaler
	c.logger.Info("Current is %d, scaler is %f", current, scaler)
	if diff >= 0.6 {
		return int(scaler)
	} else if diff <= -0.6 {
		return int(scaler + 0.5)
	} else {
		return current
	}
}
