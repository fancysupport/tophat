package main

import (
	crand "crypto/rand"
	"encoding/base64"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/fancysupport/tophat"
	"github.com/garyburd/redigo/redis"
)

func main() {

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

	start := time.Now()
	for _, cid := range []string{"iV90", "FBpO", "1loL", "1loL", "fq-e", "ZHQU", "ibSJ", "NmmM", "Hf4K", "o_AF"} {
		//for a := 0; a < 1000; a++ {
		//app := token(10)
		app := "test"
		//for c := 0; c < 1000; c++ {
		//cid := token(10)

		for x := 0; x < 100; x++ {
			err = th.Write(tophat.MetricValue{
				MetricName: "impression",
				TagValues:  []string{app, cid},
				Timestamp:  time.Unix(time.Now().Unix()-int64(rand.Intn(60*60*24)), 0),
				ValueFloat: 1,
			})
			if err != nil {
				panic(err)
			}
		}
		fmt.Println(app, cid)
	}
	//}

	fmt.Println(time.Now().Sub(start))

	var graph *tophat.MetricGraph
	graph, err = th.Graph(tophat.MetricGraphRequest{
		MetricName: "impression",
		TagValues:  []string{"test", "iV90"},
		Step:       tophat.TimestepHour,
		Fn:         tophat.CountFn,
		FillZero:   true,
	})
	if err != nil {
		panic(err)
	}

	graphs, err := th.GraphEachTag(tophat.MetricGraphRequest{
		MetricName: "impression",
		TagValues:  []string{"test", "test"},
		Step:       tophat.TimestepHour,
		Fn:         tophat.CountFn,
		FillZero:   true,
	}, "cid", []string{"iV90", "FBpO", "1loL", "1loL", "fq-e", "ZHQU", "ibSJ", "NmmM", "Hf4K", "o_AF"})
	if err != nil {
		panic(err)
	}

	fmt.Println(graph)
	for _, g := range graphs {
		fmt.Println(g.SparkString())
	}
	//fmt.Println(graph.SparkString())

}

func token(length int) string {
	bytes := make([]byte, length)
	crand.Read(bytes)
	return strings.TrimRight(base64.URLEncoding.EncodeToString(bytes), "=")
}
