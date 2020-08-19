package brownout

import (
	"container/ring"
)

type StatsKeeper struct {
	statsSlidingWindow *ring.Ring
	Maxlen             int
	runningSum         float64
	numElems           int
}

func (store *StatsKeeper) AddMeasurement(stat float64) {
	if store.statsSlidingWindow.Value == nil {
		store.runningSum = store.runningSum + stat
		store.numElems++
	} else {
		store.runningSum = store.runningSum - store.statsSlidingWindow.Value.(float64) + stat
	}
	store.statsSlidingWindow.Value = stat
	store.statsSlidingWindow = store.statsSlidingWindow.Next()
}

func (store *StatsKeeper) GetAverage() float64 {
	return store.runningSum / float64(store.numElems)
}

func (store *StatsKeeper) Reset() {
	store.numElems = 0
	store.runningSum = 0
}
