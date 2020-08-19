package brownout

import (
	"container/ring"
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
	SetController(controller PIController)
	UpdateControllerParams(newParams PIController)
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

type PIController struct {
	ZeroedErrorLimits *Limits `json:"zeroed_error_range"`
	current           float64
	P                 float64 `json:"p"`
	integralSum       float64
	Ti                float64 `json:"ti"`
	goal              float64
	D                 float64 `json:"d"`
	previousMeasure   float64
	ControlLoopPeriod int `json:"period"`
	currentLoopCount  int
	isAdaptivePI      bool
	AdaptivePI        *AdaptivePI `json:"adaptivePI"`
}

type AdaptivePI struct {
	oldPole    float64
	oldRlsPole float64
	Pole       float64 `json:"pole"`
	RlsPole    float64 `json:"rls_pole"`
	slope      float64
}

type Limits struct {
	Min float64 `json:"min"`
	Max float64 `json:"max"`
}

type PIDController struct {
	OutLimits           *Limits
	ProcessOutputLimits *Limits
	AutoTuningEnabled   bool
	Stats               *StatsKeeper

	Metrics        types.Metrics
	MetricLabel    string
	DeploymentName string

	autoTuner Autotuner
	pid       PIController
}

func CreateLimits(max float64, min float64) *Limits {
	return &Limits{
		Max: max,
		Min: min,
	}
}

func CreateStatsKeeper(windowSize int) *StatsKeeper {
	return &StatsKeeper{
		statsSlidingWindow: ring.New(windowSize),
		Maxlen:             windowSize,
		runningSum:         0,
		numElems:           0,
	}
}

func (pid *PIController) Initialise(current float64, goal float64) {
	pid.current = current
	pid.goal = goal
	if pid.AdaptivePI != nil {
		pid.isAdaptivePI = true
		pid.AdaptivePI.slope = 0
		pid.AdaptivePI.oldPole = pid.AdaptivePI.Pole
		pid.AdaptivePI.oldRlsPole = pid.AdaptivePI.RlsPole
	}
}

func (pid *PIController) piControlLoop(measure float64, e float64) {
	glog.Info("Adaptive PI control loop")
	estimationError := pid.current*pid.AdaptivePI.slope - measure
	pid.P = pid.AdaptivePI.RlsPole * pid.current /
		(1 + pid.AdaptivePI.RlsPole*math.Pow(pid.current, 2))
	pid.AdaptivePI.slope -= pid.P * estimationError
	pid.AdaptivePI.RlsPole -= math.Pow(pid.AdaptivePI.RlsPole, 2) * math.Pow(pid.current, 2) /
		(1 + pid.AdaptivePI.RlsPole*math.Pow(pid.current, 2))

	coeffError := (pid.AdaptivePI.Pole - 1) / pid.AdaptivePI.slope
	pid.current += coeffError * e

}

func (c *PIDController) SetController(newPID PIController) {
	c.pid = newPID
}

func (c *PIDController) UpdateControllerParams(newParams PIController) {
	c.pid.P = newParams.P
	c.pid.Ti = newParams.Ti
	if newParams.AdaptivePI != nil {
		c.pid.isAdaptivePI = true
		if c.pid.AdaptivePI.oldPole != newParams.AdaptivePI.Pole {
			c.pid.AdaptivePI.Pole = newParams.AdaptivePI.Pole
			c.pid.AdaptivePI.RlsPole = newParams.AdaptivePI.RlsPole
		}
	}
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

// Runs a simple plain PIController-only control loop
func (c *PIDController) pidControlLoop(measure float64, e float64, lastUpdate time.Duration) float64 {
	/*
	   # Control
	   d_measure = measure - \
	       (self._last_measure if self._last_measure is not None else measure)
	   self._derivative = self._Kd * d_measure / delta_time
	   self._signal = self._proportional + self._integral + self._derivative
	   self._last_measure = measure
	*/
	// Fixing the sign of the PI action
	glog.Info("Normal control loop, autotuning disabled")
	//c.P = (e / math.Abs(e)) * math.Abs(c.P)
	proportionalAction := c.pid.P * e
	c.pid.integralSum += c.pid.Ti * e * lastUpdate.Seconds()
	prev := c.pid.previousMeasure
	if prev == 0 {
		prev = measure
	}
	c.pid.previousMeasure = measure
	dMeasure := measure - prev
	derivativeAction := c.pid.D * dMeasure / lastUpdate.Seconds()
	c.Metrics.SetControllerActionValue(derivativeAction, "derivative", c.MetricLabel, c.DeploymentName)
	c.pid.current = proportionalAction + c.pid.integralSum + derivativeAction

	// Calculating the PI action
	//c.pid.current = proportionalAction + c.pid.integralSum + (c.pid.P*(lastUpdate.Seconds()/c.pid.Ti))*e
	glog.Info(
		fmt.Sprintf(
			"Poportional action is %f and controller response is %f", proportionalAction, c.pid.current,
		),
	)
	return proportionalAction
}

// Push control loop metrics and controller actions to Prometheus
func (c *PIDController) pushMetrics(error float64, deployment string) {
	if c.Metrics != nil {
		if c.pid.isAdaptivePI {
			c.Metrics.SetControllerParameterValue(c.pid.P, "P", c.MetricLabel, deployment)
			c.Metrics.SetControllerParameterValue(c.pid.AdaptivePI.slope, "Slope", c.MetricLabel, deployment)
			c.Metrics.SetControllerParameterValue(c.pid.AdaptivePI.Pole, "Pole", c.MetricLabel, deployment)
			c.Metrics.SetControllerParameterValue(c.pid.AdaptivePI.RlsPole, "RlsPole", c.MetricLabel, deployment)
		} else {
			c.Metrics.SetControllerParameterValue(c.pid.Ti, "Ti", c.MetricLabel, deployment)
			c.Metrics.SetControllerParameterValue(c.pid.P, "K", c.MetricLabel, deployment)
			c.Metrics.SetControllerActionValue(c.pid.P*error, "proportional", c.MetricLabel, deployment)
			c.Metrics.SetControllerActionValue(c.pid.integralSum, "integral_sum", c.MetricLabel, deployment)
		}
		c.Metrics.SetControlError(error, c.MetricLabel, deployment)
	} else {
		glog.Warning("Metrics are null inside the controller")
	}
}

func (c *PIDController) getError(current float64) float64 {
	if current > c.pid.ZeroedErrorLimits.Min && current < c.pid.ZeroedErrorLimits.Max {
		glog.Info(
			fmt.Sprintf("Error is zero as current metric value %f is in range [%f, %f]",
				current, c.pid.ZeroedErrorLimits.Min, c.pid.ZeroedErrorLimits.Max),
		)
		return 0
	}
	return c.pid.goal - current
}

func (c *PIDController) Next(current float64, lastUpdate time.Duration) float64 {
	if c.pid.currentLoopCount != c.pid.ControlLoopPeriod {
		c.pid.currentLoopCount++
		return c.pid.current
	}

	c.Stats.AddMeasurement(current)
	e := c.getError(c.Stats.GetAverage())

	if c.AutoTuningEnabled {
		c.autoTuner.stepCounter++
		if !c.autoTuner.AutoTuningActive && math.Abs(e) > c.autoTuner.AutoTuningThreshold {
			c.activateAutoTuning()
		}
	}

	proportionalAction := 0.0
	if c.AutoTuningEnabled && c.autoTuner.AutoTuningActive {
		c.autoTune(e, lastUpdate, c.Stats.GetAverage())
	} else {
		if c.pid.isAdaptivePI {
			c.pid.piControlLoop(current, e)
		}
		proportionalAction = c.pidControlLoop(current, e, lastUpdate)
	}

	c.clampOutput()
	c.pushMetrics(e, c.DeploymentName)

	// Anti-windup
	c.pid.integralSum = c.pid.current - proportionalAction

	return c.pid.current * c.OutLimits.Max
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

	c.ProcessOutputLimits.Max = math.Max(c.ProcessOutputLimits.Max, current)
	c.ProcessOutputLimits.Min = math.Min(c.ProcessOutputLimits.Min, current)
	glog.Info(fmt.Sprintf("OxMax is %f and OxMin is %f", c.ProcessOutputLimits.Max, c.ProcessOutputLimits.Min))

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
				amplitude := c.ProcessOutputLimits.Max - c.ProcessOutputLimits.Min
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
	return ((c.OutLimits.Max-c.OutLimits.Min)*3)/5 + c.OutLimits.Min
}

func (c *PIDController) clampOutput() {
	c.pid.current = math.Min(math.Max(c.OutLimits.Min/c.OutLimits.Max, c.pid.current), 1)
}
