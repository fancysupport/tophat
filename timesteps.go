package tophat

import "time"

type Time int

const (
	Second Time = iota
	Minute
	Hour
	Day
	Month
	Year
)

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
	case Second:
		unix := now.Unix()
		if previous {
			unix -= 1
		}
		return unix

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
	NumSteps: 60,
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
