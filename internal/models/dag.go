package models

import (
	"fmt"

	"github.com/robfig/cron/v3"
	"gopkg.in/yaml.v3"
)

// TaskDef defines a unit of work within a DAG
type TaskDef struct {
	ID        string   `yaml:"id"`
	Type      string   `yaml:"type"` // e.g. "command"
	Command   string   `yaml:"command"`
	DependsOn []string `yaml:"depends_on"`
}

// DAGDef defines the workflow graph of Tasks
type DAGDef struct {
	ID          string    `yaml:"id"`
	Description string    `yaml:"description"`
	Schedule    string    `yaml:"schedule"` // Cron expression
	Tasks       []TaskDef `yaml:"tasks"`
}

// ParseDAG parses a YAML byte slice into a DAGDef
func ParseDAG(data []byte) (*DAGDef, error) {
	var dag DAGDef
	err := yaml.Unmarshal(data, &dag)
	if err != nil {
		return nil, err
	}
	return &dag, nil
}

// Validate checks the structural integrity logically of the DAG
func (d *DAGDef) Validate() error {
	if d.ID == "" {
		return fmt.Errorf("DAG ID cannot be empty")
	}

	if len(d.Tasks) == 0 {
		return fmt.Errorf("DAG %s has no tasks defined", d.ID)
	}

	// 1. Validate Cron Schedule
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
	if _, err := parser.Parse(d.Schedule); err != nil {
		return fmt.Errorf("invalid cron schedule '%s' for DAG %s: %v", d.Schedule, d.ID, err)
	}

	taskIDs := make(map[string]bool)
	taskMap := make(map[string]TaskDef)

	// Base structural checks
	for _, t := range d.Tasks {
		if t.ID == "" {
			return fmt.Errorf("DAG %s contains a task with an empty ID", d.ID)
		}
		if _, exists := taskIDs[t.ID]; exists {
			return fmt.Errorf("DAG %s contains duplicate task ID: %s", d.ID, t.ID)
		}
		taskIDs[t.ID] = true
		taskMap[t.ID] = t

		for _, dep := range t.DependsOn {
			if dep == t.ID {
				return fmt.Errorf("task %s depends on itself", t.ID)
			}
		}
	}

	// Validate missing dependencies
	for _, t := range d.Tasks {
		for _, dep := range t.DependsOn {
			if !taskIDs[dep] {
				return fmt.Errorf("task %s depends on unknown task %s", t.ID, dep)
			}
		}
	}

	// Detect Circular Dependencies using DFS state: 0=unvisited, 1=visiting, 2=visited
	state := make(map[string]int)
	var hasCycle func(taskID string) bool
	hasCycle = func(taskID string) bool {
		if state[taskID] == 1 {
			return true // Cycle detected
		}
		if state[taskID] == 2 {
			return false // Already verified clean
		}

		state[taskID] = 1 // Mark as visiting

		for _, dep := range taskMap[taskID].DependsOn {
			if hasCycle(dep) {
				return true
			}
		}

		state[taskID] = 2 // Mark as completed
		return false
	}

	for id := range taskIDs {
		if state[id] == 0 {
			if hasCycle(id) {
				return fmt.Errorf("circular dependency detected involving task %s", id)
			}
		}
	}

	return nil
}
