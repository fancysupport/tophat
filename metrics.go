package tophat

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/garyburd/redigo/redis"
)

type MetricType struct {
	Script *redis.Script
}

var DefaultMetric = MetricType{
	Script: redis.NewScript(1, AggregateHash),
}

const SEP = ":"

type Metric struct {
	Name  string
	Key   string
	Tags  []string
	Steps []*Timestep
	Type  MetricType
}

type MetricValue struct {
	MetricName string
	TagValues  []string
	Timestamp  time.Time
	ValueFloat float64
}

type MetricFn int

const (
	CountFn MetricFn = iota
	SumFn
	MinFn
	MaxFn
	AvgFn
)

type MetricGraphRequest struct {
	MetricName string
	TagValues  []string
	Step       *Timestep
	Fn         MetricFn
	FillZero   bool
}

type MetricGraph [][2]float64

func write_key(key string, mv MetricValue, t *Timestep, previous bool) string {
	// make a key for redis that looks like
	// key:tagv1:tagv2:tagvX:timestamp:stepkey
	// where timestamp is start of the specified period
	k := key + SEP + strings.Join(mv.TagValues, SEP) + SEP
	if previous {
		k += strconv.FormatInt(t.StartOfPreviousPeriod(mv.Timestamp), 10)
	} else {
		k += strconv.FormatInt(t.StartOfPeriod(mv.Timestamp), 10)
	}
	k += SEP + t.Key
	return k
}

func (m *Metric) WriteFloat(conn redis.Conn, mv MetricValue) error {
	// use the aggregation lua function to store data in a hashmap
	// keys for the redis hashmap are the incremental offsets from the lower period of the timestep
	// impression:1234:1427346000:h
	// will hold aggregated impression values for the hour Thu, 26 Mar 2015 05:00:00 GMT
	// with an id of 1234
	// this key will hold a hashmap of 60 items, 0 - 59 representing each minute in that hour
	// each hashmap value holds a packed binary string containing count,sum,min,max

	// do a write for every timestep
	for _, step := range m.Steps {
		redis_key := write_key(m.Key, mv, step, false)
		hash_key := step.PeriodStep(mv.Timestamp)
		expires := step.PeriodExpireAt(mv.Timestamp)

		_, err := m.Type.Script.Do(conn, redis_key, hash_key, expires, mv.ValueFloat)
		if err != nil {
			return err
		}

		//fmt.Println(redis_key, hash_key, expires, mv.ValueFloat)
	}
	return nil
}

func (m *Metric) Graph(conn redis.Conn, mgr MetricGraphRequest) (*MetricGraph, error) {
	// fetch the write_keys for current period and the previous
	// return collection of points from now going back the step count defined in Timestep
	// redis keys return hashmaps, with each value a packed binary string, we need to unpack
	now := time.Now().UTC()
	pkey := write_key(m.Key, MetricValue{Timestamp: now, TagValues: mgr.TagValues}, mgr.Step, true)
	pts := mgr.Step.StartOfPreviousPeriod(now)
	key := write_key(m.Key, MetricValue{Timestamp: now, TagValues: mgr.TagValues}, mgr.Step, false)
	ts := mgr.Step.StartOfPeriod(now)

	pres, err := ByteMap(conn.Do("hgetall", pkey))
	if err != nil {
		return nil, errors.New("Failed fetching previous key for graph (" + pkey + ") " + err.Error())
	}
	res, err := ByteMap(conn.Do("hgetall", key))
	if err != nil {
		return nil, errors.New("Failed fetching key for graph (" + pkey + ") " + err.Error())
	}

	// build a map for timestamp => {unpacked struct}
	unpacked := make(map[float64]float64, len(pres)+len(res))
	keys := make([]float64, 0, len(pres)+len(res))

	for k, v := range pres {
		offset, _ := strconv.Atoi(k)
		timestamp := float64(remake_timestamp(pts, offset, mgr.Step.Period))
		data, err := AggregateHashUnpack(v)
		if err != nil {
			return nil, err
		}
		unpacked[timestamp] = AggregateHashPick(data, mgr.Fn)
		keys = append(keys, timestamp)
	}

	for k, v := range res {
		offset, _ := strconv.Atoi(k)
		timestamp := float64(remake_timestamp(ts, offset, mgr.Step.Period))
		data, err := AggregateHashUnpack(v)
		if err != nil {
			return nil, err
		}
		unpacked[timestamp] = AggregateHashPick(data, mgr.Fn)
		keys = append(keys, timestamp)
	}

	sort.Float64s(keys)

	// now we can actually assemble the sorted array of arrays
	result := make(MetricGraph, 0, len(keys))

	for _, timestamp := range keys {
		result = append(result, [2]float64{timestamp, unpacked[timestamp]})
	}

	fmt.Println(unpacked)

	return &result, nil
}

func remake_timestamp(start int64, offset int, period Time) int64 {
	// we want to add the amount of time for the period under the given
	// so we a remaking an offset timestamp for a day graph, we add offset * seconds_in_hours\
	// being careful that day and month offsets do not start at 0
	end := time.Unix(start, 0).UTC()

	switch period {
	case Minute:
		end = end.Add(time.Duration(offset) * time.Second)
	case Hour:
		end = end.Add(time.Duration(offset) * time.Minute)
	case Day:
		end = end.Add(time.Duration(offset) * time.Hour)
	case Month:
		end = end.AddDate(0, 0, offset-1)
	case Year:
		end = end.AddDate(0, offset-1, 0)
	}

	return end.Unix()
}

// lifted from the redis helper StringMap
// ByteMap is a helper that converts an array of strings (alternating key, value)
// into a map[string][]byte. The HGETALL and CONFIG GET commands return replies in this format.
// Requires an even number of values in result.
func ByteMap(result interface{}, err error) (map[string][]byte, error) {
	values, err := redis.Values(result, err)
	if err != nil {
		return nil, err
	}
	if len(values)%2 != 0 {
		return nil, errors.New("redigo: ByteMap expects even number of values result")
	}
	m := make(map[string][]byte, len(values)/2)
	for i := 0; i < len(values); i += 2 {
		key, okKey := values[i].([]byte)
		value, okValue := values[i+1].([]byte)
		if !okKey || !okValue {
			return nil, errors.New("redigo: ScanMap key not a bulk string value")
		}
		m[string(key)] = value
	}
	return m, nil
}

func (m *Metric) tsdb_string(mv MetricValue) string {
	tags := ""
	for x := range m.Tags {
		tags += m.Tags[x] + "=" + mv.TagValues[x] + " "
	}
	return fmt.Sprintf("put %s %d %f %s", m.Key, mv.Timestamp.Unix(), mv.ValueFloat, tags)
}
