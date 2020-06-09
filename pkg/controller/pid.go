package controller

import (
	"github.com/golang/glog"
	"time"
)

type Controller interface {
	Next(current float64, lastUpdate time.Duration) float64
}

type PIDController struct {
	P           float64
	I           float64
	D           float64
	Target      float64
	Previous    float64
	IntegralSum float64
	MinOut      float64
	MaxOut      float64
}

func (c *PIDController) Next(current float64, lastUpdate time.Duration) float64 {
	e := c.Target - current
	dt := lastUpdate.Seconds()
	var derSum float64

	c.IntegralSum += e * dt * c.I
	if c.IntegralSum > c.MaxOut {
		glog.Info("Integral sum is greater than max out")
		c.IntegralSum = c.MaxOut
	} else if c.IntegralSum < c.MinOut {
		glog.Info("Integral sum is smaller than min out")
		c.IntegralSum = c.MinOut
	}
	if dt > 0 {
		derSum = (e - (c.Target - c.Previous)) / dt
	}
	c.Previous = current
	out := c.P*e + c.IntegralSum + c.D*derSum

	if c.IntegralSum > c.MaxOut {
		glog.Info("Result is capped")
		return c.MaxOut
	} else if c.IntegralSum < c.MinOut {
		glog.Info("Result is floored")
		return c.MinOut
	}
	return out
}
