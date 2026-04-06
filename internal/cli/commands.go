package cli

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"text/tabwriter"
	"time"
)

var commands = map[string]bool{
	"dags": true, "trigger": true, "pause": true, "activate": true,
	"runs": true, "run": true, "tasks": true, "logs": true,
	"retry": true, "kill": true, "stats": true, "workers": true,
}

// IsCommand reports whether name is a known CLI subcommand.
func IsCommand(name string) bool {
	return commands[name]
}

// Run dispatches the given subcommand. args[0] is the subcommand name.
func Run(args []string) {
	if len(args) == 0 {
		printUsage()
		os.Exit(1)
	}

	// Global flags parsed from the tail of every subcommand.
	fs := flag.NewFlagSet("nagare", flag.ExitOnError)
	apiURL := fs.String("api", envOr("NAGARE_API_URL", "http://localhost:8080"), "Nagare API base URL")
	apiKey := fs.String("api-key", os.Getenv("NAGARE_API_KEY"), "API key for authentication")
	jsonOut := fs.Bool("json", false, "Output raw JSON instead of formatted tables")

	cmd := args[0]
	// Parse flags from args[1:] but stop at the first non-flag (positional args).
	// We need to separate positional args from flags manually because flag.FlagSet
	// doesn't natively support intermixed positional + flag args well.
	var positional []string
	var flagArgs []string
	for _, a := range args[1:] {
		if strings.HasPrefix(a, "-") {
			flagArgs = append(flagArgs, a)
		} else {
			positional = append(positional, a)
		}
	}

	// Add command-specific flags before parsing.
	var dagFilter, statusFilter *string
	var limitFlag *int
	if cmd == "runs" {
		dagFilter = fs.String("dag", "", "Filter by DAG ID")
		statusFilter = fs.String("status", "", "Filter by status (running, success, failed)")
		limitFlag = fs.Int("limit", 20, "Maximum number of runs to show")
	}

	if err := fs.Parse(flagArgs); err != nil {
		os.Exit(1)
	}

	client := NewClient(*apiURL, *apiKey)

	var err error
	switch cmd {
	case "dags":
		err = cmdDags(client, *jsonOut)
	case "trigger":
		err = requireArgs(positional, 1, "trigger <dag_id>")
		if err == nil {
			err = cmdTrigger(client, positional[0], *jsonOut)
		}
	case "pause":
		err = requireArgs(positional, 1, "pause <dag_id>")
		if err == nil {
			err = cmdPause(client, positional[0])
		}
	case "activate":
		err = requireArgs(positional, 1, "activate <dag_id>")
		if err == nil {
			err = cmdActivate(client, positional[0])
		}
	case "runs":
		err = cmdRuns(client, *dagFilter, *statusFilter, *limitFlag, *jsonOut)
	case "run":
		err = requireArgs(positional, 1, "run <run_id>")
		if err == nil {
			err = cmdRun(client, positional[0], *jsonOut)
		}
	case "tasks":
		err = requireArgs(positional, 1, "tasks <run_id>")
		if err == nil {
			err = cmdTasks(client, positional[0], *jsonOut)
		}
	case "logs":
		err = requireArgs(positional, 2, "logs <run_id> <task_instance_id>")
		if err == nil {
			err = cmdLogs(client, positional[0], positional[1])
		}
	case "retry":
		err = requireArgs(positional, 2, "retry <run_id> <task_id>")
		if err == nil {
			err = cmdRetry(client, positional[0], positional[1])
		}
	case "kill":
		if len(positional) == 1 {
			err = cmdKillRun(client, positional[0])
		} else if len(positional) == 2 {
			err = cmdKillTask(client, positional[0], positional[1])
		} else {
			err = fmt.Errorf("usage: nagare kill <run_id> [task_id]")
		}
	case "stats":
		err = cmdStats(client, *jsonOut)
	case "workers":
		err = cmdWorkers(client, *jsonOut)
	default:
		printUsage()
		os.Exit(1)
	}

	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func requireArgs(positional []string, n int, usage string) error {
	if len(positional) < n {
		return fmt.Errorf("usage: nagare %s", usage)
	}
	return nil
}

func printUsage() {
	fmt.Fprintf(os.Stderr, `Nagare CLI — manage DAGs and runs from the terminal

Usage:
  nagare <command> [arguments] [flags]

Commands:
  dags                         List all loaded DAGs
  trigger <dag_id>             Trigger a manual DAG run
  pause <dag_id>               Pause a DAG's schedule
  activate <dag_id>            Resume a paused DAG
  runs [--dag=X] [--status=X]  List recent runs
  run <run_id>                 Show details of a single run
  tasks <run_id>               List tasks for a run
  logs <run_id> <task_id>      Stream task logs
  retry <run_id> <task_id>     Retry a failed task
  kill <run_id> [task_id]      Kill a run or a specific task
  stats                        Show system statistics
  workers                      List connected workers

Global Flags:
  --api <url>      API base URL (default: $NAGARE_API_URL or http://localhost:8080)
  --api-key <key>  API key      (default: $NAGARE_API_KEY)
  --json           Output raw JSON
`)
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// ----- Commands --------------------------------------------------------------

func cmdDags(c *Client, jsonOut bool) error {
	data, err := c.Get("/api/dags")
	if err != nil {
		return err
	}
	if jsonOut {
		fmt.Println(string(data))
		return nil
	}

	var dags []struct {
		ID       string `json:"id"`
		Schedule string `json:"schedule"`
		Paused   bool   `json:"Paused"`
		Tasks    []struct {
			ID string `json:"id"`
		} `json:"tasks"`
	}
	if err := json.Unmarshal(data, &dags); err != nil {
		return fmt.Errorf("parsing response: %w", err)
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(w, "DAG ID\tSCHEDULE\tTASKS\tSTATUS")
	for _, d := range dags {
		status := "active"
		if d.Paused {
			status = "paused"
		}
		schedule := d.Schedule
		if schedule == "" {
			schedule = "manual"
		}
		_, _ = fmt.Fprintf(w, "%s\t%s\t%d\t%s\n", d.ID, schedule, len(d.Tasks), status)
	}
	return w.Flush()
}

func cmdTrigger(c *Client, dagID string, jsonOut bool) error {
	data, err := c.Post(fmt.Sprintf("/api/dags/%s/runs", dagID), nil)
	if err != nil {
		return err
	}
	if jsonOut {
		fmt.Println(string(data))
		return nil
	}

	var run struct {
		ID string `json:"ID"`
	}
	if err := json.Unmarshal(data, &run); err != nil {
		return fmt.Errorf("parsing response: %w", err)
	}
	fmt.Printf("Triggered DAG %s → run %s\n", dagID, run.ID)
	return nil
}

func cmdPause(c *Client, dagID string) error {
	_, err := c.Post(fmt.Sprintf("/api/dags/%s/pause", dagID), nil)
	if err != nil {
		return err
	}
	fmt.Printf("Paused DAG %s\n", dagID)
	return nil
}

func cmdActivate(c *Client, dagID string) error {
	_, err := c.Post(fmt.Sprintf("/api/dags/%s/activate", dagID), nil)
	if err != nil {
		return err
	}
	fmt.Printf("Activated DAG %s\n", dagID)
	return nil
}

func cmdRuns(c *Client, dagFilter, statusFilter string, limit int, jsonOut bool) error {
	path := fmt.Sprintf("/api/runs?limit=%d", limit)
	if dagFilter != "" {
		path += "&dag_id=" + dagFilter
	}
	if statusFilter != "" {
		path += "&status=" + statusFilter
	}

	data, err := c.Get(path)
	if err != nil {
		return err
	}
	if jsonOut {
		fmt.Println(string(data))
		return nil
	}

	var resp struct {
		Data []struct {
			ID          string `json:"ID"`
			DAGID       string `json:"DAGID"`
			Status      string `json:"Status"`
			TriggerType string `json:"TriggerType"`
			CreatedAt   string `json:"CreatedAt"`
		} `json:"data"`
		Total int `json:"total"`
	}
	if err := json.Unmarshal(data, &resp); err != nil {
		return fmt.Errorf("parsing response: %w", err)
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(w, "RUN ID\tDAG\tSTATUS\tTRIGGER\tCREATED")
	for _, r := range resp.Data {
		created := formatTime(r.CreatedAt)
		_, _ = fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n", r.ID, r.DAGID, r.Status, r.TriggerType, created)
	}
	_ = w.Flush()
	fmt.Printf("\nShowing %d of %d total runs\n", len(resp.Data), resp.Total)
	return nil
}

func cmdRun(c *Client, runID string, jsonOut bool) error {
	data, err := c.Get(fmt.Sprintf("/api/runs/%s", runID))
	if err != nil {
		return err
	}
	if jsonOut {
		fmt.Println(string(data))
		return nil
	}

	var run struct {
		ID          string `json:"ID"`
		DAGID       string `json:"DAGID"`
		Status      string `json:"Status"`
		TriggerType string `json:"TriggerType"`
		CreatedAt   string `json:"CreatedAt"`
		CompletedAt string `json:"CompletedAt"`
	}
	if err := json.Unmarshal(data, &run); err != nil {
		return fmt.Errorf("parsing response: %w", err)
	}

	fmt.Printf("Run:       %s\n", run.ID)
	fmt.Printf("DAG:       %s\n", run.DAGID)
	fmt.Printf("Status:    %s\n", run.Status)
	fmt.Printf("Trigger:   %s\n", run.TriggerType)
	fmt.Printf("Created:   %s\n", formatTime(run.CreatedAt))
	if run.CompletedAt != "" {
		fmt.Printf("Completed: %s\n", formatTime(run.CompletedAt))
	}
	return nil
}

func cmdTasks(c *Client, runID string, jsonOut bool) error {
	data, err := c.Get(fmt.Sprintf("/api/runs/%s/tasks", runID))
	if err != nil {
		return err
	}
	if jsonOut {
		fmt.Println(string(data))
		return nil
	}

	var tasks []struct {
		ID        string `json:"ID"`
		TaskID    string `json:"TaskID"`
		Status    string `json:"Status"`
		Attempt   int    `json:"Attempt"`
		Command   string `json:"Command"`
		CreatedAt string `json:"CreatedAt"`
	}
	if err := json.Unmarshal(data, &tasks); err != nil {
		return fmt.Errorf("parsing response: %w", err)
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(w, "INSTANCE ID\tTASK\tSTATUS\tATTEMPT\tCOMMAND")
	for _, t := range tasks {
		cmd := t.Command
		if len(cmd) > 50 {
			cmd = cmd[:47] + "..."
		}
		_, _ = fmt.Fprintf(w, "%s\t%s\t%s\t%d\t%s\n", t.ID, t.TaskID, t.Status, t.Attempt, cmd)
	}
	return w.Flush()
}

func cmdLogs(c *Client, runID, taskID string) error {
	path := fmt.Sprintf("/api/runs/%s/tasks/%s/logs", runID, taskID)
	return c.StreamSSE(path, func(line string) {
		fmt.Println(line)
	})
}

func cmdRetry(c *Client, runID, taskID string) error {
	_, err := c.Post(fmt.Sprintf("/api/runs/%s/tasks/%s/retry", runID, taskID), nil)
	if err != nil {
		return err
	}
	fmt.Printf("Queued retry for task %s in run %s\n", taskID, runID)
	return nil
}

func cmdKillRun(c *Client, runID string) error {
	_, err := c.Post(fmt.Sprintf("/api/runs/%s/kill", runID), nil)
	if err != nil {
		return err
	}
	fmt.Printf("Killed run %s\n", runID)
	return nil
}

func cmdKillTask(c *Client, runID, taskID string) error {
	_, err := c.Post(fmt.Sprintf("/api/runs/%s/tasks/%s/kill", runID, taskID), nil)
	if err != nil {
		return err
	}
	fmt.Printf("Killed task %s in run %s\n", taskID, runID)
	return nil
}

func cmdStats(c *Client, jsonOut bool) error {
	data, err := c.Get("/api/stats")
	if err != nil {
		return err
	}
	if jsonOut {
		fmt.Println(string(data))
		return nil
	}

	var stats map[string]interface{}
	if err := json.Unmarshal(data, &stats); err != nil {
		return fmt.Errorf("parsing response: %w", err)
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	for k, v := range stats {
		_, _ = fmt.Fprintf(w, "%s\t%v\n", k, v)
	}
	return w.Flush()
}

func cmdWorkers(c *Client, jsonOut bool) error {
	data, err := c.Get("/api/workers")
	if err != nil {
		return err
	}
	if jsonOut {
		fmt.Println(string(data))
		return nil
	}

	var workers []struct {
		WorkerID    string   `json:"worker_id"`
		Hostname    string   `json:"hostname"`
		Pools       []string `json:"pools"`
		Status      string   `json:"status"`
		ActiveTasks int      `json:"active_tasks"`
		MaxTasks    int      `json:"max_tasks"`
	}
	if err := json.Unmarshal(data, &workers); err != nil {
		return fmt.Errorf("parsing response: %w", err)
	}

	if len(workers) == 0 {
		fmt.Println("No workers connected")
		return nil
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(w, "WORKER ID\tHOSTNAME\tPOOLS\tSTATUS\tTASKS")
	for _, wk := range workers {
		pools := strings.Join(wk.Pools, ",")
		_, _ = fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%d/%d\n", wk.WorkerID, wk.Hostname, pools, wk.Status, wk.ActiveTasks, wk.MaxTasks)
	}
	return w.Flush()
}

// ----- Helpers ---------------------------------------------------------------

func formatTime(raw string) string {
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339, "2006-01-02T15:04:05Z", "2006-01-02 15:04:05"} {
		if t, err := time.Parse(layout, raw); err == nil {
			return t.Local().Format("2006-01-02 15:04:05")
		}
	}
	return raw
}
