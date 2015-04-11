package tophat

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/fzzy/radix/extra/cluster"
	"github.com/fzzy/radix/redis"
)

type MetricType struct {
	Script *Script
}

var DefaultMetric = MetricType{
	Script: NewScript(1, AggregateHash),
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
	NumSteps   int // optional to override Timestep defined steps
}

type MetricGraph struct {
	Tags   map[string]string `json:"tags"`
	Values [][2]float64      `json:"values"`
}

func (mg *MetricGraph) Spark() []float64 {
	values := make([]float64, 0, len(mg.Values))
	for _, v := range mg.Values {
		values = append(values, v[1])
	}
	return values
}

func (mg *MetricGraph) SparkString() string {
	spark := mg.Spark()
	s := ""
	for _, v := range spark {
		s += strconv.FormatFloat(v, 'f', -1, 64) + " "
	}
	return s
}

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

func (m *Metric) WriteFloat(conn *cluster.Cluster, mv MetricValue) error {
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

		reply := m.Type.Script.Cmd(conn, redis_key, hash_key, expires, mv.ValueFloat)
		if reply.Err != nil {
			return reply.Err
		}

		//fmt.Println(redis_key, hash_key, expires, mv.ValueFloat)
	}
	return nil
}

func (m *Metric) Graph(conn *cluster.Cluster, mgr MetricGraphRequest) (*MetricGraph, error) {
	// fetch the write_keys for current period and the previous
	// return collection of points from now going back the step count defined in Timestep
	// redis keys return hashmaps, with each value a packed binary string, we need to unpack
	now := time.Now().UTC()
	pkey := write_key(m.Key, MetricValue{Timestamp: now, TagValues: mgr.TagValues}, mgr.Step, true)
	pts := mgr.Step.StartOfPreviousPeriod(now)
	key := write_key(m.Key, MetricValue{Timestamp: now, TagValues: mgr.TagValues}, mgr.Step, false)
	ts := mgr.Step.StartOfPeriod(now)

	pres, err := ByteMap(conn.Cmd("hgetall", pkey))
	if err != nil {
		return nil, errors.New("Failed fetching previous key for graph (" + pkey + ") " + err.Error())
	}
	res, err := ByteMap(conn.Cmd("hgetall", key))
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

	// determine how many steps we need
	num_steps := mgr.Step.NumSteps
	if mgr.NumSteps > 0 {
		num_steps = mgr.NumSteps
	}

	// now we can actually assemble the sorted array of arrays
	values := make([][2]float64, 0, num_steps)

	// get the list of steps we need to return
	list := mgr.Step.PeriodStepList(now, num_steps)

	// construct the result, optionally fill empty steps
	for _, timestamp := range list {
		t := float64(timestamp)
		val, exists := unpacked[t]
		if exists {
			values = append(values, [2]float64{t, val})
		} else if mgr.FillZero {
			values = append(values, [2]float64{t, 0})
		}
	}

	result := &MetricGraph{
		Tags:   m.tag_map(mgr.TagValues),
		Values: values,
	}

	return result, nil
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

func ByteMap(r *redis.Reply) (map[string][]byte, error) {
	if r.Type == redis.ErrorReply {
		return nil, r.Err
	}

	rmap := make(map[string][]byte, len(r.Elems)/2)

	if r.Type != redis.MultiReply {
		return nil, errors.New("reply type is not MultiReply")
	}

	if len(r.Elems)%2 != 0 {
		return nil, errors.New("reply has odd number of elements")
	}

	for i := 0; i < len(r.Elems)/2; i++ {
		key, err := r.Elems[i*2].Str()
		if err != nil {
			return nil, errors.New("key element has no string reply")
		}

		v := r.Elems[i*2+1]
		if v.Type == redis.BulkReply {
			val, _ := v.Bytes()
			rmap[key] = val
		} else if v.Type == redis.NilReply {
		} else {
			return nil, errors.New("value element type is not BulkReply or NilReply")
		}
	}

	return rmap, nil
}

func (m *Metric) tsdb_string(mv MetricValue) string {
	tags := ""
	for x := range m.Tags {
		tags += m.Tags[x] + "=" + mv.TagValues[x] + " "
	}
	return fmt.Sprintf("put %s %d %f %s", m.Key, mv.Timestamp.Unix(), mv.ValueFloat, tags)
}

func (m *Metric) tag_map(values []string) map[string]string {
	tagmap := map[string]string{}
	for x := range m.Tags {
		tagmap[m.Tags[x]] = values[x]
	}
	return tagmap
}
