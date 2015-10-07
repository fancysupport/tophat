package tophat

import (
	"errors"

	"github.com/fzzy/radix/extra/pool"
)

type Client struct {
	pool    *pool.Pool
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

	// pass write off to metric
	return m.WriteFloat(c.pool, mv)
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

	// pass graph request
	return m.Graph(c.pool, mgr)
}

func (c *Client) GraphEachTag(mgr MetricGraphRequest, tag string, tag_values []string) ([]*MetricGraph, error) {
	// a helper function to return a multi series given a substitution list of tag values
	// get a group by result but it doesn't go and discover the key list as it's provided
	// find the metric by name
	m, exists := c.metrics[mgr.MetricName]
	if !exists {
		return nil, errors.New("No metric with name: " + mgr.MetricName)
	}

	// find the tag index
	index := -1
	for i, t := range m.Tags {
		if t == tag {
			index = i
		}
	}

	// validate we can replace tags
	if index == -1 {
		return nil, errors.New("Replacement tag index invalid for metric: " + m.Name)
	}

	graphs := make([]*MetricGraph, 0, len(tag_values))

	for _, tv := range tag_values {
		new_request := MetricGraphRequest{
			MetricName: mgr.MetricName,
			TagValues:  make([]string, len(m.Tags)),
			Step:       mgr.Step,
			Fn:         mgr.Fn,
			FillZero:   mgr.FillZero,
			NumSteps:   mgr.NumSteps,
		}
		// replace the tags
		copy(new_request.TagValues, mgr.TagValues)
		new_request.TagValues[index] = tv

		g, err := c.Graph(new_request)
		if err != nil {
			return nil, err
		}

		graphs = append(graphs, g)
	}

	return graphs, nil
}

func NewClient(p *pool.Pool) (*Client, error) {

	client := &Client{
		pool:    p,
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
