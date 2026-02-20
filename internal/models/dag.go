package models

import (
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
