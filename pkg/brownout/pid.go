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
	NextAutoTuned(current float64, lastUpdate time.Duration) float64
	SetGoal(newGoal float64)
}

type PIDController struct {
	P                 float64
	I                 float64
	goal              float64
	integralSum       float64
	MinOut            float64
	MaxOut            float64
	AutoTuningEnabled bool

	Metrics types.Metrics

	// Auto-Tuning things
	current             float64
	stepCounter         int
	AutoTuningActive    bool
	AutoTuningThreshold float64
	atRelaysOld         int
	numUpToggles        int
	periodStepOld       float64
	lastUpToggle        int
	OxMax               float64
	OxMin               float64

	// Strange variables, that I need help in figuring out
	Ti           float64 // Integral time
	PmAutotuning float64
	DuAutotuning float64
}

func (c *PIDController) NextAutoTuned(current float64, lastUpdate time.Duration) float64 {
	c.stepCounter++
	e := c.goal - current

	if !c.AutoTuningActive && math.Abs(e) > c.AutoTuningThreshold && c.AutoTuningEnabled {
		glog.Info("Activating and resetting auto-tuning")
		c.AutoTuningActive = true
		c.numUpToggles = 0
		c.periodStepOld = 0
		c.lastUpToggle = 0
	}

	proportionalAction := 0.0

	if !c.AutoTuningActive {
		// Fixing the sign of the PI action
		glog.Info("Normal control loop, autotuning disabled")
		c.P = (e / math.Abs(e)) * math.Abs(c.P)

		proportionalAction = c.P * e

		// Calculating the PI action
		c.current = proportionalAction + c.integralSum + (c.P*(lastUpdate.Seconds()/c.Ti))*e
		glog.Info(fmt.Sprintf("Poportional action is %f and controller response is %f", proportionalAction, c.current))
	} else {
		c.autoTune(e, lastUpdate, current)
	}

	c.clampOutput()
	c.integralSum = c.current - proportionalAction

	c.Metrics.SetControllerIValue(c.integralSum)
	c.Metrics.SetControllerPValue(c.P)
	c.Metrics.SetControllerResponse(e)

	return c.current
}

func (c *PIDController) autoTune(e float64, lastUpdate time.Duration, current float64) {
	glog.Info("AUTOTUNING!!!")
	atRelays := -1
	if e > 0 {
		c.current += c.DuAutotuning
		atRelays = 1
	} else {
		c.current -= c.DuAutotuning
	}

	c.OxMax = math.Max(c.OxMax, current)
	c.OxMin = math.Min(c.OxMin, current)
	glog.Info(fmt.Sprintf("OxMax is %f and OxMin is %f", c.OxMax, c.OxMin))

	if atRelays == 1 && c.atRelaysOld == -1 {
		// There has been a change in error sign
		glog.Info("Relay Toggle")
		c.numUpToggles++

		if c.numUpToggles >= 2 {
			glog.Info("More than two relay toggles, we are oscillating!!")
			periodStep := float64(c.stepCounter - c.lastUpToggle)

			if c.periodStepOld > 0 && math.Abs((periodStep-c.periodStepOld)/c.periodStepOld) < 0.05 {
				// There was a flip upwards already and the relative interval between the flips is small
				glog.Info("-------INNER LOOP OF AUTOTUNING------")
				amplitude := c.OxMax - c.OxMin
				wox := 2 * math.Pi / (periodStep * lastUpdate.Seconds())
				c.Ti = math.Tan((c.PmAutotuning/180.0)*math.Pi) / wox
				c.P = 4 * c.DuAutotuning / math.Pi / (amplitude / 2) / math.Sqrt(1+math.Pow(wox*c.Ti, 2))

				c.AutoTuningActive = false
				glog.Info(fmt.Sprintf("Autotuned. new P: %f, new ti: %f", c.P, c.Ti))
			}
			c.periodStepOld = periodStep
			c.lastUpToggle = c.stepCounter
		}
	}
	c.atRelaysOld = atRelays
}

func (c *PIDController) Next(current float64, lastUpdate time.Duration) float64 {
	e := c.goal - current
	dt := lastUpdate.Seconds()

	c.integralSum += e * dt * c.I
	if c.integralSum > c.MaxOut {
		glog.Info("Integral sum is greater than max out")
		c.integralSum = c.MaxOut
	} else if c.integralSum < c.MinOut {
		glog.Info("Integral sum is smaller than min out")
		c.integralSum = c.MinOut
	}
	out := c.P*e + c.integralSum

	if out > c.MaxOut {
		glog.Info("Result is capped")
		return c.MaxOut
	} else if out < c.MinOut {
		glog.Info("Result is floored")
		return c.MinOut
	}

	c.Metrics.SetControllerIValue(c.integralSum)
	c.Metrics.SetControllerPValue(c.P)
	c.Metrics.SetControllerResponse(e)
	return out
}

func (c *PIDController) SetGoal(newGoal float64) {
	c.goal = newGoal
	if c.current == 0 {
		c.current = c.MinOut + (c.MaxOut-c.MinOut)/2
	}
}

func (c *PIDController) clampOutput() {
	if c.current > c.MaxOut {
		c.current = c.MaxOut
	} else if c.current < c.MinOut {
		c.current = c.MinOut
	}
}
