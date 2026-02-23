package main

import (
	"context"
	"flag"
	"fmt"
	"io/fs"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/alephmelo/nagare/internal/api"
	"github.com/alephmelo/nagare/internal/autoscaler"
	"github.com/alephmelo/nagare/internal/cluster"
	"github.com/alephmelo/nagare/internal/config"
	"github.com/alephmelo/nagare/internal/logbroker"
	"github.com/alephmelo/nagare/internal/models"
	"github.com/alephmelo/nagare/internal/scheduler"
	"github.com/alephmelo/nagare/internal/worker"
)

// frontendEmbedFS is declared in frontend_embed.go (normal builds) or
// frontend_stub.go (nofrontend tag — worker-only Docker image builds).

func main() {
	// ----- CLI flags ---------------------------------------------------------
	workerMode := flag.Bool("worker", false, "Run in worker-only mode (connect to a master)")
	joinAddr := flag.String("join", "", "Master address for worker mode (e.g. http://host:8080)")
	poolsFlag := flag.String("pools", "default", "Comma-separated pool names this worker serves")
	token := flag.String("token", "", "Shared secret for master-worker authentication")
	port := flag.String("port", ":8080", "Listen address for the master API server")
	masterAddr := flag.String("master-addr", "", "Public address of this master, passed to cloud workers via --join (e.g. http://1.2.3.4:8080). Defaults to http://localhost<port>.")
	dbPath := flag.String("db", "nagare.db", "SQLite database path")
	dagsDir := flag.String("dags", "dags", "Directory containing DAG definitions")
	apiKey := flag.String("api-key", "", "API key to protect all /api/* routes (overrides nagare.yaml and NAGARE_API_KEY env var)")
	flag.Parse()

	if *workerMode {
		runWorker(*joinAddr, *poolsFlag, *token)
		return
	}

	runMaster(*port, *masterAddr, *dbPath, *dagsDir, *token, *apiKey)
}

// runMaster starts the full Nagare master node: scheduler + local worker pool +
// optional cluster coordinator for remote workers + HTTP API.
func runMaster(addr, masterAddr, dbPath, dagsDir, token, apiKeyFlag string) {
	log.Println("Booting up Nagare: Lean Airflow in Go")

	// Ensure dags directory exists.
	if err := os.MkdirAll(dagsDir, 0750); err != nil {
		log.Fatalf("Failed to create dags directory: %v", err)
	}

	// 0. Load configuration.
	cfg, err := config.LoadConfig("nagare.yaml")
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}
	log.Printf("Loaded configuration with %d worker pools", len(cfg.WorkerPools))

	// Resolve API key: flag > NAGARE_API_KEY env var > nagare.yaml api_key.
	resolvedAPIKey := cfg.APIKey
	if envKey := os.Getenv("NAGARE_API_KEY"); envKey != "" {
		resolvedAPIKey = envKey
	}
	if apiKeyFlag != "" {
		resolvedAPIKey = apiKeyFlag
	}

	// 1. Initialize SQLite database.
	store, err := models.NewStore(fmt.Sprintf("file:%s?cache=shared", dbPath))
	if err != nil {
		log.Fatalf("Failed to initialize database: %v", err)
	}
	defer store.Close()

	// 2. Initialize scheduler and load DAGs.
	sched := scheduler.NewScheduler(store)
	if err := sched.LoadDAGs(dagsDir); err != nil {
		store.Close()                              //nolint:errcheck // error on close after fatal is non-actionable
		log.Fatalf("Failed to load DAGs: %v", err) //nolint:gocritic // store.Close() called explicitly above
	}

	getDAG := func(id string) (*models.DAGDef, bool) {
		d, ok := sched.GetDAGs()[id]
		return d, ok
	}

	// 3. Initialize log broker and local worker pool.
	broker := logbroker.NewBroker()
	pool := worker.NewPool(store, getDAG, sched.TriggerDAG, cfg.WorkerPools, broker)

	// 4. Initialize cluster coordinator (always-on; only used when remote
	//    workers connect — zero overhead when no workers register).
	coord := cluster.NewCoordinator(store, getDAG, 60*time.Second, token)
	coord.SetBroker(broker)

	// 5. Initialize autoscaler when enabled.
	//    The coordinator implements StatsSource; a storeAdapter translates
	//    models.Store into the autoscaler's InstanceStore interface.
	var as *autoscaler.Autoscaler
	if cfg.Autoscaler.Enabled {
		provider, err := autoscaler.NewProvider(cfg.Autoscaler)
		if err != nil {
			log.Fatalf("Autoscaler: failed to initialize provider %q: %v", cfg.Autoscaler.Provider, err)
		}
		storeAdapt := autoscaler.NewStoreAdapter(store)
		as = autoscaler.New(cfg.Autoscaler, provider, coord, storeAdapt, sched.GetDAGs)
		coord.SetAutoscaler(as)
		log.Printf("Autoscaler: initialized (provider=%s, max=%d)", cfg.Autoscaler.Provider, cfg.Autoscaler.MaxCloudWorkers)
	} else {
		// Disabled autoscaler: pass a no-op instance store so coordinator still works.
		as = autoscaler.New(cfg.Autoscaler, nil, coord, autoscaler.NewStoreAdapter(store), sched.GetDAGs)
	}

	// 6. Initialize API server and attach coordinator and autoscaler.
	apiServer := api.NewServer(store, sched, pool, broker, cfg.CORS.AllowedOrigins, resolvedAPIKey)
	apiServer.WithCoordinator(coord)
	apiServer.WithAutoscaler(as)
	apiServer.WithDAGsDir(dagsDir)

	// Context for graceful shutdown.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 7. Reset any tasks left running/queued by a previous process that was killed
	//    or crashed. Those processes are dead, so the tasks can never complete —
	//    mark them failed so runs surface as failed in the UI and can be re-triggered.
	if n, err := store.ResetStaleTasks(); err != nil {
		log.Printf("Warning: failed to reset stale tasks: %v", err)
	} else if n > 0 {
		log.Printf("Startup: reset %d stale task(s) from previous run to 'failed'", n)
	}

	// 8. Start local workers, autoscaler, and API server.
	pool.Start(ctx)

	// Start the autoscaler background loop (no-op when disabled).
	// masterAddr is the address cloud workers will use to connect back to this
	// master via --join.  It defaults to http://localhost<port> (works for
	// Docker provider) but must be set to a publicly reachable address when
	// using the AWS provider.
	effectiveMasterAddr := masterAddr
	if effectiveMasterAddr == "" {
		effectiveMasterAddr = fmt.Sprintf("http://localhost%s", addr)
	}
	go as.Run(ctx, 30*time.Second, effectiveMasterAddr, token)

	go func() {
		fSys, err := fs.Sub(frontendEmbedFS, "web/out")
		if err != nil {
			log.Fatalf("Failed to initialize frontend FS: %v", err)
		}
		if err := apiServer.Start(addr, fSys); err != nil {
			log.Fatalf("API Server failed: %v", err)
		}
	}()

	// 9. Periodic stale-worker expiry.
	go func() {
		t := time.NewTicker(30 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				coord.ExpireStaleWorkers()
			}
		}
	}()

	// OS signal handling.
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	log.Println("Nagare is running. Press Ctrl+C to exit.")

	for {
		select {
		case <-sigChan:
			log.Println("Received shutdown signal, terminating workers...")
			cancel()
			pool.Stop()
			log.Println("Nagare shut down successfully")
			return

		case <-ticker.C:
			if err := sched.LoadDAGs(dagsDir); err != nil {
				log.Printf("Scheduler DAG reload error: %v", err)
			}
			if err := sched.Tick(); err != nil {
				log.Printf("Scheduler tick error: %v", err)
			}
			if err := pool.Dispatch(); err != nil {
				log.Printf("Worker dispatch error: %v", err)
			}
		}
	}
}

// runWorker starts a worker-only node that registers with and polls a master.
func runWorker(masterAddr, poolsFlag, token string) {
	if masterAddr == "" {
		log.Fatal("--join is required in worker mode (e.g. --join http://master:8080)")
	}

	pools := strings.Split(poolsFlag, ",")
	for i, p := range pools {
		pools[i] = strings.TrimSpace(p)
	}

	hostname, _ := os.Hostname()
	workerID := fmt.Sprintf("%s-%d", hostname, os.Getpid())

	log.Printf("Starting Nagare worker (id=%s, pools=%v, master=%s)", workerID, pools, masterAddr)

	cfg := cluster.RemoteWorkerConfig{
		MasterAddr:          masterAddr,
		WorkerID:            workerID,
		Pools:               pools,
		Hostname:            hostname,
		MaxTasks:            4,
		Token:               token,
		PollInterval:        2 * time.Second,
		HeartbeatInterval:   10 * time.Second,
		CancelCheckInterval: 5 * time.Second,
	}

	rw := cluster.NewRemoteWorker(cfg)
	if err := rw.Register(); err != nil {
		log.Fatalf("Failed to register with master: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Println("Worker received shutdown signal, stopping...")
		cancel()
	}()

	rw.Run(ctx)
	log.Println("Worker stopped")
}
