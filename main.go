package tophat

import "github.com/garyburd/redigo/redis"

func NewClient(pool *redis.Pool) (*Client, error) {

	client := &Client{
		pool:    pool,
		steps:   map[string]*Timestep{},
		metrics: map[string]*Metric{},
	}

	// preload default steps
	if err := client.AddTimestep(TimestepHour); err != nil {
		return nil, err
	}
	if err := client.AddTimestep(TimestepDay); err != nil {
		return nil, err
	}
	if err := client.AddTimestep(TimestepMonth); err != nil {
		return nil, err
	}
	if err := client.AddTimestep(TimestepYear); err != nil {
		return nil, err
	}

	return client, nil
}
