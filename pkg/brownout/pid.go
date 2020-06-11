package brownout

import (
	"fmt"
	"github.com/golang/glog"
	"math"
	"time"
)

type Controller interface {
	Next(current float64, lastUpdate time.Duration) float64
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

	// Auto-Tuning things
	current             float64
	stepCounter         int
	autoTuningActive    bool
	autoTuningThreshold float64
	atRelaysOld         int
	numUpToggles        int
	periodStepOld       float64
	lastUpToggle        int

	// Strange variables, that I need help in figuring out
	ti           float64
	pmAutotuning float64
	duAutotuning float64
	amplitude    float64 // I get that this is the amplitude of oscillations, but how do I set it?
}

func (c *PIDController) NextAutoTuned(current float64, lastUpdate time.Duration) float64 {
	c.stepCounter++
	e := c.goal - current

	if !c.autoTuningActive && math.Abs(e) > c.autoTuningThreshold && c.AutoTuningEnabled {
		c.autoTuningActive = true
		c.numUpToggles = 0
		c.periodStepOld = 0
		c.lastUpToggle = 0
	}

	proportionalAction := 0.0

	if !c.autoTuningActive {
		// Fixing the sign of the PI action
		c.P = (e / math.Abs(e)) * c.P

		proportionalAction = c.P * e

		// Calculating the PI action
		c.current = proportionalAction + c.integralSum + (c.P*(lastUpdate.Seconds()/c.ti))*e
	} else {
		c.autoTune(e, lastUpdate)
	}

	c.clampOutput()
	c.integralSum = c.current - proportionalAction
	return c.current
}

func (c *PIDController) autoTune(e float64, lastUpdate time.Duration) {
	atRelays := -1
	if e > 0 {
		c.current += c.duAutotuning
		atRelays = 1
	} else {
		c.current -= c.duAutotuning
	}

	if atRelays == 1 && c.atRelaysOld == -1 {
		// There has been a change in error sign
		c.numUpToggles++

		if c.numUpToggles >= 2 {
			periodStep := float64(c.stepCounter - c.lastUpToggle)

			if c.periodStepOld > 0 && math.Abs((periodStep-c.periodStepOld)/c.periodStepOld) < 0.05 {
				// There was a flip upwards already and the relative interval between the flips is small
				wox := 2 * math.Pi / (periodStep * lastUpdate.Seconds())
				c.ti = math.Tan((c.pmAutotuning/180.0)*math.Pi) / wox
				c.P = 4 * c.duAutotuning / math.Pi / (c.amplitude / 2) / math.Sqrt(1+math.Pow(wox*c.ti, 2))

				c.autoTuningActive = false
				glog.Info(fmt.Sprintf("Autotuned. new P: %f, new ti: %f", c.P, c.ti))
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
	return out
}

func (c *PIDController) SetGoal(newGoal float64) {
	c.goal = newGoal
}

func (c *PIDController) clampOutput() {
	if c.current > c.MaxOut {
		c.current = c.MaxOut
	} else if c.current < c.MinOut {
		c.current = c.MinOut
	}
}
