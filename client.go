package tophat

import (
	"errors"

	"github.com/garyburd/redigo/redis"
)

type Client struct {
	pool    *redis.Pool
	steps   map[string]*Timestep
	metrics map[string]*Metric
}

func (c *Client) AddTimestep(t *Timestep) error {
	// check for dups
	for _, v := range c.steps {
		if t.Name == v.Name {
			return errors.New("Timestep name already exists.")
		}
	}

	// add to available steps
	c.steps[t.Name] = t
	return nil
}

func (c *Client) AddMetric(m *Metric) error {
	// check for dups
	for _, v := range c.metrics {
		if m.Name == v.Name {
			return errors.New("Metric name already exists.")
		}
	}

	if len(m.Steps) == 0 {
		return errors.New("No timesteps given.")
	}

	// check the timesteps exist
	for _, v := range m.Steps {
		if _, exists := c.steps[v.Name]; !exists {
			return errors.New("Step name not loaded, load it before adding metrics. (" + v.Name + ")")
		}
	}

	if m.Type != DefaultMetric {
		return errors.New("Unsupported metric type.")
	}

	// add to available metrics
	c.metrics[m.Name] = m

	return nil
}

func (c *Client) Write(mv MetricValue) error {
	// find the metric by name
	m, exists := c.metrics[mv.MetricName]
	if !exists {
		return errors.New("No metric with name: " + mv.MetricName)
	}

	// make sure tag lengths match
	if len(m.Tags) != len(mv.TagValues) {
		return errors.New("TagValues don't match the Tags count for the metric.")
	}

	// get a redis con
	conn := c.pool.Get()
	defer conn.Close()

	// pass write off to metric
	return m.WriteFloat(conn, mv)
}

func (c *Client) Graph(mgr MetricGraphRequest) (*MetricGraph, error) {
	// find the metric by name
	m, exists := c.metrics[mgr.MetricName]
	if !exists {
		return nil, errors.New("No metric with name: " + mgr.MetricName)
	}

	// timestep needs to be in the list
	found := false
	for _, step := range m.Steps {
		if mgr.Step.Name == step.Name {
			found = true
		}
	}
	if !found {
		return nil, errors.New("That timestep is not in the list for that metric.")
	}

	// get a redis con
	conn := c.pool.Get()
	defer conn.Close()

	// pass graph request
	return m.Graph(conn, mgr)
}
