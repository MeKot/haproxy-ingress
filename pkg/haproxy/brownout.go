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

type ControllerInput string

const (
	DimmerOutput ControllerInput = "dimmer_out"
	ScalerOutput ControllerInput = "scaler_out"
	Metric       ControllerInput = "metric"
)

type Controller interface {
	Update(backend *hatypes.Backend)
}

// Represents the per-target configuration, where target is the deployment that
// is scaled browned out
type TargetConfig struct {
	Paths                       []string              `json:"paths"`
	RequestLimit                int                   `json:"request_limit"`
	Target                      string                `json:"target"`
	DimmerTargetValue           int                   `json:"dimmer_target_value"`
	ScalerTargetValue           float64               `json:"scaler_target_value"`
	ScalingThreshold            float64               `json:"scaler_threshold"`
	ScalerHysteresys            int                   `json:"scaler_hysteresis"`
	TargetReplicas              int                   `json:"target_replicas"`
	MaxReplicas                 int                   `json:"max_replicas"`
	DeploymentNamespace         string                `json:"deployment_namespace"`
	DeploymentName              string                `json:"deployment_name"`
	ScalerMeasurementWindowSize int                   `json:"scaler_measurement_window_size"`
	DimmerMeasurementWindowSize int                   `json:"dimmer_measurement_window_size"`
	ScalerInput                 ControllerInput       `json:"scaler_in"`
	DimmerInput                 ControllerInput       `json:"dimmer_in"`
	ScalerPID                   brownout.PIController `json:"scaler_pi"`
	DimmerPID                   brownout.PIController `json:"dimmer_pi"`
}

type BrownoutConfig struct {
	ReloadInterval int                     `json:"reload_interval"`
	Targets        map[string]TargetConfig `json:"targets"`
}

type ScalerParams struct {
	Threshold  float64
	Hysteresis time.Duration
}

// controller used to perform runtime updates
type controller struct {
	reloadInterval    time.Duration
	lastUpdates       map[string]time.Time
	lastScalingUpdate time.Time
	needsReload       bool
	logger            types.Logger
	targets           map[string]TargetConfig
	cmd               func(socket string, observer func(duration time.Duration), commands ...string) ([]string, error)
	socket            string
	metrics           types.Metrics
	currConfig        *config
	scalingParams     map[string]ScalerParams
	dimmers           map[string]brownout.Controller
	scalers           map[string]brownout.Controller
	rtimes            map[string]*brownout.StatsKeeper
}

func (i *instance) GetController() Controller {
	var c BrownoutConfig
	err := json.Unmarshal([]byte(i.curConfig.Brownout().Rules), &c)

	if err != nil {
		i.logger.Error("Failed to unmarshal!!")
		i.logger.Error(err.Error())
	}

	i.logger.Info("Unmarshalled to %+v", c)

	for depl, configuration := range c.Targets {
		if configuration.ScalingThreshold == 0 {
			configuration.ScalingThreshold = 0.6
		}
		i.logger.Info("Initialising the global rates map")
		// Default rate limit is 100000 requests per minute
		for _, path := range configuration.Paths {
			r, ok := i.curConfig.Brownout().Rates[path]
			if !ok {
				i.logger.Info("Adding %q", path)
				i.curConfig.Brownout().Rates[path] = configuration.RequestLimit
				continue
			}
			i.logger.Info("Found %q with set rate %d", path, r)
		}

		// Update the scaling parameters
		v, ok := i.curConfig.Brownout().Deployments[depl]

		if ok {
			i.logger.Info("Deployment %q has %f replicas", depl, v)
		}

		if !ok {
			// Set to target replicas, so we update them on the next scalability action
			i.curConfig.Brownout().Deployments[depl] = hatypes.DeploymentData{
				Name:      configuration.DeploymentName,
				Namespace: configuration.DeploymentNamespace,
				Replicas:  float64(configuration.TargetReplicas),
			}
			i.logger.Info("Set the number of replicas for %q at %d", depl, configuration.TargetReplicas)
		}
	}

	if i.brownout != nil {
		// Reusing the controller that has already been initialised, just updating some values
		i.brownout.(*controller).currConfig = i.curConfig.(*config)
		i.brownout.(*controller).targets = c.Targets
		i.brownout.(*controller).updateControllers()
		return i.brownout
	}

	reloadInterval, _ := time.ParseDuration(fmt.Sprintf("%ds", c.ReloadInterval))
	out := &controller{
		needsReload:       false,
		logger:            i.logger,
		lastUpdates:       make(map[string]time.Time),
		lastScalingUpdate: time.Now(),
		reloadInterval:    reloadInterval,
		targets:           c.Targets,
		cmd:               utils.HAProxyCommandWithReturn,
		socket:            i.curConfig.Global().AdminSocket,
		metrics:           i.metrics,
		currConfig:        i.curConfig.(*config),
		scalingParams:     make(map[string]ScalerParams),
		dimmers:           make(map[string]brownout.Controller),
		scalers:           make(map[string]brownout.Controller),
		rtimes:            make(map[string]*brownout.StatsKeeper),
	}
	for deployment, conf := range c.Targets {
		out.createDimmerController(conf, deployment)
		out.createScalerController(conf, deployment)
		dur, e := time.ParseDuration(fmt.Sprintf("%ds", conf.ScalerHysteresys))
		if e != nil {
			i.logger.Info(fmt.Sprintf("Failed to parse duration, setting to 60s"))
			dur = time.Second * 60
		}
		out.scalingParams[deployment] = ScalerParams{
			Hysteresis: dur,
			Threshold:  conf.ScalingThreshold,
		}
		out.rtimes[deployment] = brownout.CreateStatsKeeper(10)
	}
	return out
}

func (c *controller) updateControllers() {
	c.logger.Info("UPDATING CONTROLLER PARAMS FROM CONFIG MAP")
	for depl, conf := range c.targets {
		c.dimmers[depl].UpdateControllerParams(conf.DimmerPID)
		c.scalers[depl].UpdateControllerParams(conf.ScalerPID)
		c.scalers[depl].UpdateOutLimits(brownout.CreateLimits(float64(c.targets[depl].MaxReplicas),
			float64(c.targets[depl].TargetReplicas)))
	}
}

func (c *controller) createDimmerController(conf TargetConfig, deployment string) {
	conf.DimmerPID.Initialise(1.0, float64(conf.DimmerTargetValue))
	c.dimmers[deployment] = &brownout.PIDController{
		OutLimits:           brownout.CreateLimits(1, 0),
		ProcessOutputLimits: brownout.CreateLimits(1e6, -1e6),
		Stats:               brownout.CreateStatsKeeper(conf.DimmerMeasurementWindowSize),
		AutoTuningEnabled:   false,
		Metrics:             c.metrics,
		MetricLabel:         "dimmer",
		DeploymentName:      deployment,
	}
	c.dimmers[deployment].SetController(conf.DimmerPID)
	c.logger.Info("Created Dimmer Controller, the result is %+v", c.dimmers[deployment])
}

func (c *controller) createScalerController(conf TargetConfig, deployment string) {
	conf.ScalerPID.Initialise(float64(conf.TargetReplicas), float64(conf.RequestLimit))
	c.scalers[deployment] = &brownout.PIDController{
		OutLimits:           brownout.CreateLimits(float64(conf.MaxReplicas), float64(conf.TargetReplicas)),
		ProcessOutputLimits: brownout.CreateLimits(1, float64(conf.RequestLimit)),
		Stats:               brownout.CreateStatsKeeper(conf.ScalerMeasurementWindowSize),
		AutoTuningEnabled:   false,
		Metrics:             c.metrics,
		MetricLabel:         "scaler",
		DeploymentName:      deployment,
	}
	c.scalers[deployment].SetController(conf.ScalerPID)
	c.logger.Info("Created Scaler Controller, the result is %+v", c.scalers[deployment])
}

// Coordinates metric collection and updates the config with any necessary action
func (c *controller) Update(backend *hatypes.Backend) {
	if _, ok := c.rtimes[backend.Name]; !ok {
		return
	}
	stats, err := c.readStats(backend.ID)
	if err != nil {
		c.logger.Error(err.Error())
		return
	}
	// Have to use ID here, as prometheus only exports backend IDs
	rtime := c.recordResponseTime(backend.Name, stats)
	c.rtimes[backend.Name].AddMeasurement(rtime)

	lastUpdate, ok := c.lastUpdates[backend.Name]

	if ok && lastUpdate.Add(c.reloadInterval).After(time.Now()) {
		c.logger.Info("Waiting before next update for %q", backend.Name)
		return
	}

	dimmerAdjustment := c.getDimmerAdjustment(backend.Name, c.rtimes[backend.Name].GetAverage())
	c.execApplyACL(backend, int(dimmerAdjustment*float64(c.targets[backend.Name].RequestLimit)))
	if c.needsReload {
		c.logger.InfoV(2, "Queued updates to be written to disks on next reload")
		c.needsReload = false
	}
	c.lastUpdates[backend.Name] = time.Now()

	depl, ok := c.targets[backend.Name]
	if !ok {
		return
	}
	c.logger.Info("Considering deployment %q for scaling", depl.DeploymentName)
	scalerAction := c.getScalerAdjustment(c.getSclaerInput(stats, backend.Name, dimmerAdjustment), backend.Name)
	if c.lastScalingUpdate.Add(c.scalingParams[depl.DeploymentName].Hysteresis).After(time.Now()) {
		c.logger.Info("Scaling cancelled as we are still in the hysteresis time")
		return
	}

	c.logger.Info("Last scaling at %q time now is %q and hysteresis is %s", c.lastScalingUpdate.String(),
		time.Now().String(), c.scalingParams[backend.Name].Hysteresis)
	deploymentConf := c.currConfig.brownout.Deployments[backend.Name]
	deploymentConf.Replicas = scalerAction
	c.currConfig.brownout.Deployments[backend.Name] = deploymentConf

	c.logger.Info("Set deployment %q to %f replicas", backend.Name,
		c.currConfig.brownout.Deployments[backend.Name].Replicas)

	c.updateDeployments()
}

func (c *controller) getSclaerInput(stats map[string]string, backend string, dimmerAdj float64) float64 {
	switch c.targets[backend].ScalerInput {
	case DimmerOutput:
		return dimmerAdj
	case Metric:
		res, err := strconv.ParseFloat(stats[c.targets[backend].Target], 64)
		if err != nil {
			c.logger.Error("Failed to parse metric value when passing to scaler")
		}
		return res
	}
	c.logger.Error("Did not match any valid input type for scaler")
	return 0
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

// Given the current dimmer status, returns the necessary number of replicas
func (c *controller) getScalerAdjustment(current float64, deployment string) float64 {
	c.logger.Info("Scaler goal is %f, current is %f", c.targets[deployment].ScalerTargetValue, current)
	c.scalers[deployment].SetGoal(c.targets[deployment].ScalerTargetValue)
	return c.scalers[deployment].Next(current, time.Now().Sub(c.lastScalingUpdate))
}

// Given the current error, returns the necessary adjustment for brownout ACL and rate limiting
func (c *controller) getDimmerAdjustment(backend string, rtime float64) float64 {
	// The PIController controller
	response := 0.0

	if _, ok := c.dimmers[backend]; !ok {
		return 0
	}

	if rtime < 0 {
		return c.dimmers[backend].GetPrevious()
	}

	c.dimmers[backend].SetGoal(float64(c.targets[backend].DimmerTargetValue))

	// Casting to int, as this directly corresponds to the rate for all non-essential endpoints
	response = c.dimmers[backend].Next(rtime, time.Now().Sub(c.lastUpdates[backend]))
	c.logger.Info(fmt.Sprintf("Dimmer response for %q is %f", backend, response))

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

	values, err := r.Read()
	if err != nil {
		c.logger.Error("Failed to parse the values")
		return map[string]string{}, err
	}

	if len(values) != len(keys) {
		return map[string]string{}, errors.New("number of keys does not match the number of values")
	}

	var m = make(map[string]string, len(keys))
	for i, key := range keys {
		m[key] = values[i]
	}
	return m, nil
}

func (c *controller) recordResponseTime(backend string, stats map[string]string) float64 {
	curr, ok := stats["rtime"]
	if !ok {
		c.logger.Error("rtime was not returned by HAProxy")
		return -1
	}
	rtime, err := time.ParseDuration(fmt.Sprintf("%s%s", stats["rtime"], "ms"))
	if err != nil {
		c.logger.Error("failed to parse rtime from %s", curr)
		return -1
	}

	c.metrics.SetBackendResponseTime(backend, rtime)
	rtimeValue, conversion_err := strconv.ParseFloat(curr, 64)
	if conversion_err != nil {
		return -1
	}
	return rtimeValue
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
	c.logger.Info("Updating Deployments to %+v", c.currConfig.brownout.Deployments)
	for depl, repl := range c.currConfig.brownout.Deployments {
		d, err := c.currConfig.brownout.Client.AppsV1().Deployments(repl.Namespace).Get(repl.Name, metav1.GetOptions{})

		if err != nil {
			c.logger.Error("could not get the deployement for %q", depl)
			c.logger.Error(err.Error())
			continue
		}

		c.logger.Info(fmt.Sprintf("The scaler params are %+v for deployment %q", c.scalingParams[depl], depl))
		desired := c.getReplicaCount(int(*d.Spec.Replicas), repl.Replicas, c.scalingParams[depl].Threshold)
		c.logger.Info("getReplicaCount returned %d", desired)

		if int(*d.Spec.Replicas) != desired {
			*d.Spec.Replicas = int32(desired)
			_, err = c.currConfig.brownout.Client.AppsV1().Deployments(repl.Namespace).Update(d)

			if err != nil {
				c.logger.Error("error updating the deployment %q", depl)
				c.logger.Error(err.Error())
			}

			c.lastScalingUpdate = time.Now()
		}
		c.metrics.SetBackendNumberOfPods(depl, *d.Spec.Replicas)

	}
}

func (c *controller) getReplicaCount(current int, scaler float64, threshold float64) int {
	diff := float64(current) - scaler
	c.logger.Info("Current is %d, scaler is %f and scaling threshold is %f", current, scaler, threshold)
	if diff >= threshold {
		return int(scaler)
	} else if diff <= -threshold {
		return int(scaler + 0.5)
	} else {
		return current
	}
}
