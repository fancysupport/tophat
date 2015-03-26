package main

import (
	"fmt"
	"time"

	"github.com/fancysupport/tophat"
	"github.com/garyburd/redigo/redis"
)

func main() {
	now := time.Now().UTC()
	today := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	yesterday := time.Date(now.Year(), now.Month(), now.Day()-1, 0, 0, 0, 0, time.UTC)
	//feb := today.AddDate(0, 0, -24)
	//feb2 := time.Date(now.Year()-3, now.Month(), now.Day()-24, 0, 0, 0, 0, time.UTC)
	//april := today.AddDate(0, 0, 8)
	march1 := time.Date(2015, time.March, 1, 0, 0, 0, 0, time.UTC)
	feb28 := march1.AddDate(0, 0, -1)
	fmt.Println(today)
	fmt.Println(yesterday)
	fmt.Println(feb28)

	t := 10 * time.Second
	red := &redis.Pool{
		MaxIdle:     50,
		IdleTimeout: 3 * time.Minute,
		Dial: func() (redis.Conn, error) {
			c, err := redis.DialTimeout("tcp", "localhost:6379", t, t, t)
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}

	th, err := tophat.NewClient(red)
	if err != nil {
		panic(err)
	}

	err = th.AddMetric(&tophat.Metric{
		Name:  "impression",
		Key:   "impression",
		Tags:  []string{"app", "cid"},
		Steps: tophat.DefaultTimesteps,
		Type:  tophat.DefaultMetric,
	})
	if err != nil {
		panic(err)
	}

	for x := 0; x < 2; x++ {
		err = th.Write(tophat.MetricValue{
			MetricName: "impression",
			TagValues:  []string{"test", "test"},
			Timestamp:  time.Now(),
			ValueFloat: 2.2,
		})
		if err != nil {
			panic(err)
		}
	}

	start := time.Now()

	var graph *tophat.MetricGraph
	for x := 0; x < 1000; x++ {
		graph, err = th.Graph(tophat.MetricGraphRequest{
			MetricName: "impression",
			TagValues:  []string{"test", "test"},
			Step:       tophat.TimestepHour,
			Fn:         tophat.SumFn,
			FillZero:   true,
		})
		if err != nil {
			panic(err)
		}
	}

	fmt.Println(time.Now().Sub(start))

	fmt.Println(graph)

}
