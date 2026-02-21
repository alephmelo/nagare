package models

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/robfig/cron/v3"
	"gopkg.in/yaml.v3"
)

// ResourcesDef declares CPU, memory, and GPU limits for a container task.
// Values mirror Docker's --cpus, --memory, and --gpus flags.
//
//	cpus:   "2.0"          # fractional CPU count, e.g. "0.5", "4"
//	memory: "512m"         # size with suffix b/k/m/g (case-insensitive)
//	gpus:   "all"          # "all" or a positive integer string
type ResourcesDef struct {
	CPUs   string `yaml:"cpus,omitempty"`
	Memory string `yaml:"memory,omitempty"`
	GPUs   string `yaml:"gpus,omitempty"`
}

// TaskDef defines a unit of work within a DAG
type TaskDef struct {
	ID                string            `yaml:"id"`
	Type              string            `yaml:"type"`               // e.g. "command" or "trigger_dag" or "map"
	MapOver           string            `yaml:"map_over,omitempty"` // For "map" tasks
	Pool              string            `yaml:"pool,omitempty"`     // Specific worker queue to run on
	DagID             string            `yaml:"dag_id,omitempty"`   // For "trigger_dag" tasks
	Command           string            `yaml:"command,omitempty"`
	Env               map[string]string `yaml:"env,omitempty"`
	Retries           int               `yaml:"retries,omitempty"`
	RetryDelaySeconds int               `yaml:"retry_delay_seconds,omitempty"`
	TimeoutSeconds    int               `yaml:"timeout_seconds,omitempty"`
	DependsOn         []string          `yaml:"depends_on"`
	WithItems         []string          `yaml:"with_items,omitempty"`

	// Container executor fields — only used when Image is non-empty.
	Image     string        `yaml:"image,omitempty"`     // Docker image, e.g. "python:3.12-slim"
	Workdir   string        `yaml:"workdir,omitempty"`   // Working directory inside the container
	Volumes   []string      `yaml:"volumes,omitempty"`   // Bind mounts: "host_path:container_path[:ro]"
	Resources *ResourcesDef `yaml:"resources,omitempty"` // CPU / memory / GPU limits
}

// TriggerDef defines an ad-hoc event trigger for the DAG
type TriggerDef struct {
	Type            string            `yaml:"type"`                       // e.g. "webhook"
	Path            string            `yaml:"path,omitempty"`             // e.g. "/api/webhooks/github"
	Method          string            `yaml:"method,omitempty"`           // e.g. "POST"
	Secret          string            `yaml:"secret,omitempty"`           // HMAC-SHA256 secret; when set, X-Hub-Signature-256 is verified
	SignatureHeader string            `yaml:"signature_header,omitempty"` // header carrying the signature (default: X-Hub-Signature-256)
	ExtractPayload  map[string]string `yaml:"extract_payload,omitempty"`  // jq queries mapped to env-var names
}

// DAGDef defines the workflow graph of Tasks
type DAGDef struct {
	ID          string      `yaml:"id"`
	Description string      `yaml:"description"`
	Schedule    string      `yaml:"schedule"`          // Cron expression
	Catchup     *bool       `yaml:"catchup,omitempty"` // Controls backfill behavior
	Trigger     *TriggerDef `yaml:"trigger,omitempty"` // Event-driven trigger
	Tasks       []TaskDef   `yaml:"tasks"`
}

func ParseDAG(data []byte) (*DAGDef, error) {
	var dag DAGDef
	err := yaml.Unmarshal(data, &dag)
	if err != nil {
		return nil, err
	}

	// Expand Tasks with WithItems
	var expandedTasks []TaskDef

	// Track expansions: map[original_task_id][]new_task_ids
	expansions := make(map[string][]string)

	for _, t := range dag.Tasks {
		if len(t.WithItems) > 0 {
			var newIDs []string
			for _, item := range t.WithItems {
				clone := t
				clone.ID = fmt.Sprintf("%s_%s", t.ID, item)
				clone.Command = strings.ReplaceAll(t.Command, "{{item}}", item)

				// Empty the specific WithItems list so it doesn't duplicate loop logic inherently
				clone.WithItems = nil

				expandedTasks = append(expandedTasks, clone)
				newIDs = append(newIDs, clone.ID)
			}
			expansions[t.ID] = newIDs
		} else {
			expandedTasks = append(expandedTasks, t)
		}
	}

	for i := range expandedTasks {
		var newDeps []string
		for _, dep := range expandedTasks[i].DependsOn {
			if expandedNewIDs, found := expansions[dep]; found {
				newDeps = append(newDeps, expandedNewIDs...)
			} else {
				newDeps = append(newDeps, dep)
			}
		}

		if expandedTasks[i].Type == "map" && expandedTasks[i].MapOver != "" {
			// Auto-inject MapOver as dependency if not implicitly there
			foundDep := false
			for _, d := range newDeps {
				if d == expandedTasks[i].MapOver {
					foundDep = true
				}
			}
			if !foundDep {
				newDeps = append(newDeps, expandedTasks[i].MapOver)
			}
		}

		expandedTasks[i].DependsOn = newDeps
	}

	dag.Tasks = expandedTasks

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

	// 1. Validate Cron Schedule (allow empty or workflow_dispatch)
	if d.Schedule != "" && d.Schedule != "workflow_dispatch" {
		parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
		if _, err := parser.Parse(d.Schedule); err != nil {
			return fmt.Errorf("invalid cron schedule '%s' for DAG %s: %v", d.Schedule, d.ID, err)
		}
	}

	// 2. Validate Trigger if present
	if d.Trigger != nil {
		if d.Trigger.Type == "" {
			return fmt.Errorf("trigger type cannot be empty for DAG %s", d.ID)
		}
		if d.Trigger.Type == "webhook" {
			if d.Trigger.Path == "" {
				return fmt.Errorf("webhook trigger must specify a path for DAG %s", d.ID)
			}
			if d.Trigger.Method == "" {
				d.Trigger.Method = "POST" // Default to POST
			}
		}
	}

	taskIDs := make(map[string]bool)
	taskMap := make(map[string]TaskDef)

	// Base structural checks
	for _, t := range d.Tasks {
		if t.ID == "" {
			return fmt.Errorf("DAG %s contains a task with an empty ID", d.ID)
		}
		if t.Type == "map" && t.MapOver == "" {
			return fmt.Errorf("task %s is type 'map' but missing 'map_over' property", t.ID)
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

		// Container field validation
		if t.Image != "" {
			if t.Type == "trigger_dag" {
				return fmt.Errorf("task %s: 'image' cannot be used with type 'trigger_dag'", t.ID)
			}
			if t.Command == "" {
				return fmt.Errorf("task %s: 'command' is required when 'image' is set", t.ID)
			}
		}
		if t.Resources != nil {
			if err := validateResources(t.ID, t.Resources); err != nil {
				return err
			}
			if t.Image == "" {
				return fmt.Errorf("task %s: 'resources' requires 'image' to be set", t.ID)
			}
		}
		if len(t.Volumes) > 0 && t.Image == "" {
			return fmt.Errorf("task %s: 'volumes' requires 'image' to be set", t.ID)
		}
		if t.Workdir != "" && t.Image == "" {
			return fmt.Errorf("task %s: 'workdir' requires 'image' to be set", t.ID)
		}
		for _, vol := range t.Volumes {
			if err := validateVolume(t.ID, vol); err != nil {
				return err
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

// validateResources checks that resource limit values are well-formed.
func validateResources(taskID string, r *ResourcesDef) error {
	if r.CPUs != "" {
		v, err := strconv.ParseFloat(r.CPUs, 64)
		if err != nil || v <= 0 {
			return fmt.Errorf("task %s: resources.cpus must be a positive number (got %q)", taskID, r.CPUs)
		}
	}
	if r.Memory != "" {
		matched, _ := regexp.MatchString(`(?i)^\d+[bkmg]?$`, r.Memory)
		if !matched {
			return fmt.Errorf("task %s: resources.memory must be a number with optional suffix b/k/m/g (got %q)", taskID, r.Memory)
		}
	}
	if r.GPUs != "" {
		if r.GPUs != "all" {
			v, err := strconv.Atoi(r.GPUs)
			if err != nil || v <= 0 {
				return fmt.Errorf("task %s: resources.gpus must be \"all\" or a positive integer (got %q)", taskID, r.GPUs)
			}
		}
	}
	return nil
}

// validateVolume checks that a volume mount string has the expected format.
func validateVolume(taskID, vol string) error {
	parts := strings.Split(vol, ":")
	if len(parts) < 2 || len(parts) > 3 {
		return fmt.Errorf("task %s: invalid volume %q — expected host_path:container_path[:ro|rw]", taskID, vol)
	}
	if parts[0] == "" || parts[1] == "" {
		return fmt.Errorf("task %s: invalid volume %q — host and container paths must be non-empty", taskID, vol)
	}
	if len(parts) == 3 && parts[2] != "ro" && parts[2] != "rw" {
		return fmt.Errorf("task %s: invalid volume mode %q — must be 'ro' or 'rw'", taskID, parts[2])
	}
	return nil
}
