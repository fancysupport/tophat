package tophat

import (
	"sort"
	"time"
)

type Time int

const (
	Minute Time = iota
	Hour
	Day
	Month
	Year
)

// so we can sort int64 slice
type int64arr []int64

func (a int64arr) Len() int           { return len(a) }
func (a int64arr) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a int64arr) Less(i, j int) bool { return a[i] < a[j] }

type Timestep struct {
	Name     string
	Key      string
	Period   Time
	Keep     int
	NumSteps int
}

func (t *Timestep) StartOfPeriod(ts time.Time) int64 {
	return t.start_of_period(ts, false)
}

func (t *Timestep) StartOfPreviousPeriod(ts time.Time) int64 {
	return t.start_of_period(ts, true)
}

func (t *Timestep) start_of_period(ts time.Time, previous bool) int64 {
	// get a unix timestamp for the start of hour / day / month
	now := ts.UTC()

	switch t.Period {
	case Minute:
		if previous {
			return time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), now.Minute()-1, 0, 0, time.UTC).Unix()
		}
		return time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), now.Minute(), 0, 0, time.UTC).Unix()

	case Hour:
		if previous {
			return time.Date(now.Year(), now.Month(), now.Day(), now.Hour()-1, 0, 0, 0, time.UTC).Unix()
		}
		return time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), 0, 0, 0, time.UTC).Unix()

	case Day:
		if previous {
			return time.Date(now.Year(), now.Month(), now.Day()-1, 0, 0, 0, 0, time.UTC).Unix()
		}
		return time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC).Unix()

	case Month:
		// remember days start at 1 not 0
		if previous {
			return time.Date(now.Year(), now.Month()-1, 1, 0, 0, 0, 0, time.UTC).Unix()
		}
		return time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC).Unix()

	case Year:
		// remember months start at 1 not 0
		if previous {
			return time.Date(now.Year()-1, 1, 1, 0, 0, 0, 0, time.UTC).Unix()
		}
		return time.Date(now.Year(), 1, 1, 0, 0, 0, 0, time.UTC).Unix()
	}

	return 0
}

func (t *Timestep) last_step_of_period(ts time.Time) time.Time {
	// get a time for the end of hour / day / month
	// eg 59th minute of hour, 23rd hour of day etc
	now := ts.UTC()

	switch t.Period {
	case Minute:
		return time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), now.Minute(), 59, 0, time.UTC)

	case Hour:
		return time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), 59, 0, 0, time.UTC)

	case Day:
		return time.Date(now.Year(), now.Month(), now.Day(), 23, 0, 0, 0, time.UTC)

	case Month:
		// remember days start at 1 not 0
		// 0 means go backwards one
		return time.Date(now.Year(), now.Month()+1, 0, 0, 0, 0, 0, time.UTC)

	case Year:
		// remember months start at 1 not 0
		// 0 means go backwards one
		return time.Date(now.Year(), 0, 1, 0, 0, 0, 0, time.UTC)
	}

	return time.Unix(0, 0)
}

func (t *Timestep) current_step_of_period(ts time.Time) time.Time {
	// get a time for the step that the time given falls into
	// eg current top of the hour if the period is a day
	now := ts.UTC()

	switch t.Period {
	case Minute:
		return time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), now.Minute(), now.Second(), 0, time.UTC)

	case Hour:
		return time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), now.Minute(), 0, 0, time.UTC)

	case Day:
		return time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), 0, 0, 0, time.UTC)

	case Month:
		// remember days start at 1 not 0
		// 0 means go backwards one
		return time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)

	case Year:
		// remember months start at 1 not 0
		// 0 means go backwards one
		return time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC)
	}

	return time.Unix(0, 0)
}

func (t *Timestep) previous_step_of_period(ts time.Time) time.Time {
	// get a unix timestamp for the previous step based on what period we are using
	// go backwards an increment from ts
	now := ts.UTC()

	switch t.Period {
	case Minute:
		return time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), now.Minute(), now.Second()-1, 0, time.UTC)

	case Hour:
		return time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), now.Minute()-1, 0, 0, time.UTC)

	case Day:
		return time.Date(now.Year(), now.Month(), now.Day(), now.Hour()-1, 0, 0, 0, time.UTC)

	case Month:
		// remember days start at 1 not 0
		// 0 means go backwards one
		return time.Date(now.Year(), now.Month(), now.Day()-1, 0, 0, 0, 0, time.UTC)

	case Year:
		// remember months start at 1 not 0
		// 0 means go backwards one
		return time.Date(now.Year(), now.Month()-1, 1, 0, 0, 0, 0, time.UTC)
	}

	return time.Unix(0, 0)
}

func (t *Timestep) PeriodStep(ts time.Time) int {
	// if our period is Hour, the step is the minute of the hour
	switch t.Period {
	case Minute:
		return ts.UTC().Second()

	case Hour:
		return ts.UTC().Minute()

	case Day:
		return ts.UTC().Hour()

	case Month:
		return ts.UTC().Day()

	case Year:
		return int(ts.UTC().Month())
	}

	return -1
}

func (t *Timestep) PeriodExpireAt(ts time.Time) int64 {
	// return a unix timestamp for time of expiry to set in redis
	// we use a value 2 * period because we need to query 2 keys to get the last period from now()
	switch t.Period {
	case Minute:
		return time.Unix(t.StartOfPeriod(ts), 0).Add(time.Duration(t.Keep) * time.Minute).Unix()

	case Hour:
		return time.Unix(t.StartOfPeriod(ts), 0).Add(time.Duration(t.Keep) * time.Hour).Unix()

	case Day:
		return time.Unix(t.StartOfPeriod(ts), 0).AddDate(0, 0, t.Keep).Unix()

	case Month:
		return time.Unix(t.StartOfPeriod(ts), 0).AddDate(0, t.Keep, 0).Unix()

	case Year:
		return time.Unix(t.StartOfPeriod(ts), 0).AddDate(t.Keep, 0, 0).Unix()
	}

	return 1
}

func (t *Timestep) PeriodStepList(ts time.Time, override int) []int64 {
	// we start by getting the timestamp for the current step period, e.g. top of current hour of the day
	// build a list backwards in step increments to match what the subkeys would be
	// use NumSteps or the override amount of steps
	num_steps := t.NumSteps
	if override > 0 {
		num_steps = override
	}

	last := t.current_step_of_period(ts)
	list := make([]int64, 0, num_steps)
	list = append(list, last.Unix())

	for x := 0; x < num_steps-1; x++ {
		last = t.previous_step_of_period(last)
		list = append(list, last.Unix())
	}

	sort.Stable(int64arr(list))

	return list
}

// define some default normal steps
// hourly data with minute steps
var TimestepHour = &Timestep{
	Name:     "hour",
	Key:      "h",
	Period:   Hour,
	Keep:     2,
	NumSteps: 60,
}

// hourly for a day
var TimestepDay = &Timestep{
	Name:     "day",
	Key:      "d",
	Period:   Day,
	Keep:     2,
	NumSteps: 24,
}

// daily for a month
var TimestepMonth = &Timestep{
	Name:     "month",
	Key:      "m",
	Period:   Month,
	Keep:     2,
	NumSteps: 30,
}

// monthly for a year
var TimestepYear = &Timestep{
	Name:     "year",
	Key:      "y",
	Period:   Year,
	Keep:     2,
	NumSteps: 12,
}

var DefaultTimesteps = []*Timestep{
	TimestepHour,
	TimestepDay,
	TimestepMonth,
	TimestepYear,
}
