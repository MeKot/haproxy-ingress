package brownout

import (
	"fmt"
	"github.com/golang/glog"
	"github.com/jcmoraisjr/haproxy-ingress/pkg/types"
	"math"
	"time"
)

type Controller interface {
	Next(current float64, lastUpdate time.Duration) float64
	SetGoal(newGoal float64)
	GetTargetValue() float64
	SetController(controller PID)
	UpdateControllerParams(newParams PID)
	SetAutoTuner(tuner Autotuner)
}

type Autotuner struct {
	stepCounter         int
	AutoTuningActive    bool
	AutoTuningThreshold float64
	atRelaysOld         int
	numUpToggles        int
	periodStepOld       float64
	lastUpToggle        int
	PmAutotuning        float64
	DuAutotuning        float64
}

type PID struct {
	current     float64
	P           float64 `json:"p"`
	integralSum float64
	Ti          float64 `json:"ti"`
	goal        float64
}

type Limits struct {
	min float64
	max float64
}

type PIDController struct {
	OutLimits         *Limits
	OxLimits          *Limits
	AutoTuningEnabled bool
	IntervalBased     bool

	Metrics        types.Metrics
	MetricLabel    string
	DeploymentName string

	autoTuner Autotuner
	pid       PID
}

func CreateLimits(max float64, min float64) *Limits {
	return &Limits{
		max: max,
		min: min,
	}
}

func (pid *PID) Initialise(current float64, goal float64) {
	pid.current = current
	pid.goal = goal
}

func (c *PIDController) SetController(newPID PID) {
	c.pid = newPID
}

func (c *PIDController) UpdateControllerParams(newParams PID) {
	c.pid.P = newParams.P
	c.pid.Ti = newParams.Ti
}

func (c *PIDController) SetAutoTuner(tuner Autotuner) {
	c.autoTuner = tuner
}

// Activates autotuning and cleans the autotuner object from previous tuning cycle
func (c *PIDController) activateAutoTuning() {
	glog.Info("Activating and resetting auto-tuning")
	c.autoTuner.AutoTuningActive = true
	c.autoTuner.numUpToggles = 0
	c.autoTuner.periodStepOld = 0
	c.autoTuner.lastUpToggle = 0
}

// Runs a simple plain PID-only control loop
func (c *PIDController) normalControlLoop(e float64, lastUpdate time.Duration) float64 {
	// Fixing the sign of the PI action
	glog.Info("Normal control loop, autotuning disabled")
	//c.P = (e / math.Abs(e)) * math.Abs(c.P)
	proportionalAction := 0.0

	if c.IntervalBased && math.Abs(e) < math.Abs(c.OxLimits.max-c.OxLimits.min)/4 {
		glog.Info("Skipping this iteration because we are within the target range")
	} else {
		proportionalAction = c.pid.P * e

		// Calculating the PI action
		c.pid.current = proportionalAction + c.pid.integralSum + (c.pid.P*(lastUpdate.Seconds()/c.pid.Ti))*e
		glog.Info(
			fmt.Sprintf(
				"Poportional action is %f and controller response is %f", proportionalAction, c.pid.current,
			),
		)
	}
	return proportionalAction
}

// Push control loop metrics and controller actions to Prometheus
func (c *PIDController) pushMetrics(error float64, deployment string) {
	if c.Metrics != nil {
		c.Metrics.SetControllerParameterValue(c.pid.Ti, "Ti", c.MetricLabel, deployment)
		c.Metrics.SetControllerParameterValue(c.pid.P, "K", c.MetricLabel, deployment)
		c.Metrics.SetControllerActionValue(c.pid.P*error, "proportional", c.MetricLabel, deployment)
		c.Metrics.SetControllerActionValue(c.pid.integralSum, "integral_sum", c.MetricLabel, deployment)
		c.Metrics.SetControllerResponse(error, c.MetricLabel, deployment)
	} else {
		glog.Warning("Metrics are null inside the controller")
	}
}

func (c *PIDController) Next(current float64, lastUpdate time.Duration) float64 {
	c.autoTuner.stepCounter++
	e := c.pid.goal - current

	if c.AutoTuningEnabled && !c.autoTuner.AutoTuningActive && math.Abs(e) > c.autoTuner.AutoTuningThreshold {
		c.activateAutoTuning()
	}

	proportionalAction := 0.0
	if c.AutoTuningEnabled && c.autoTuner.AutoTuningActive {
		c.autoTune(e, lastUpdate, current)
	} else {
		proportionalAction = c.normalControlLoop(e, lastUpdate)
	}

	c.clampOutput()
	c.pushMetrics(e, c.DeploymentName)

	// Anti-windup
	c.pid.integralSum = c.pid.current - proportionalAction

	return c.pid.current * c.OutLimits.max
}

func (c *PIDController) autoTune(e float64, lastUpdate time.Duration, current float64) {
	glog.Info("AUTOTUNING!!!")
	atRelays := -1

	if e > 0 {
		c.pid.current += c.autoTuner.DuAutotuning
		atRelays = 1
	} else {
		c.pid.current -= c.autoTuner.DuAutotuning
	}

	c.OxLimits.max = math.Max(c.OxLimits.max, current)
	c.OxLimits.min = math.Min(c.OxLimits.min, current)
	glog.Info(fmt.Sprintf("OxMax is %f and OxMin is %f", c.OxLimits.max, c.OxLimits.min))

	if atRelays == 1 && c.autoTuner.atRelaysOld == -1 {
		// There has been a change in error sign
		glog.Info("Relay Toggle")
		c.autoTuner.numUpToggles++

		if c.autoTuner.numUpToggles >= 2 {
			glog.Info("More than two relay toggles, we are oscillating!!")
			periodStep := float64(c.autoTuner.stepCounter - c.autoTuner.lastUpToggle)

			// The proportion of the number of steps in the current period vs in the previous period
			periodDifference := math.Abs((periodStep - c.autoTuner.periodStepOld) / c.autoTuner.periodStepOld)
			if c.autoTuner.periodStepOld > 0 && periodDifference < 0.05 {
				// There was a flip upwards already and the relative interval between the flips is small
				amplitude := c.OxLimits.max - c.OxLimits.min
				wox := 2 * math.Pi / (periodStep * lastUpdate.Seconds())
				c.pid.Ti = math.Tan((c.autoTuner.PmAutotuning/180.0)*math.Pi) / wox
				c.pid.P = 4 * c.autoTuner.DuAutotuning / math.Pi / (amplitude / 2) /
					math.Sqrt(1+math.Pow(wox*c.pid.Ti, 2))

				c.autoTuner.AutoTuningActive = false
				glog.Info(fmt.Sprintf("Autotuned. new P: %f, new ti: %f", c.pid.P, c.pid.Ti))
			}
			c.autoTuner.periodStepOld = periodStep
			c.autoTuner.lastUpToggle = c.autoTuner.stepCounter
		}
	}
	c.autoTuner.atRelaysOld = atRelays
}

func (c *PIDController) SetGoal(newGoal float64) {
	c.pid.goal = newGoal
}

func (c *PIDController) GetTargetValue() float64 {
	// Something in the ballpark of 600, which is the target value for the dimmer
	return ((c.OutLimits.max-c.OutLimits.min)*3)/5 + c.OutLimits.min
}

func (c *PIDController) clampOutput() {
	c.pid.current = math.Min(math.Max(0, c.pid.current), 1)
}
