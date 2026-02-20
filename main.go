package main

import (
	"context"
	"embed"
	"io/fs"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/alephmelo/nagare/internal/api"
	"github.com/alephmelo/nagare/internal/models"
	"github.com/alephmelo/nagare/internal/scheduler"
	"github.com/alephmelo/nagare/internal/worker"
)

//go:embed all:web/out
var frontendEmbedFS embed.FS

func main() {
	log.Println("Booting up Nagare: Lean Airflow in Go")

	// Ensure dags directory exists
	if err := os.MkdirAll("dags", 0755); err != nil {
		log.Fatalf("Failed to create dags directory: %v", err)
	}

	// 1. Initialize SQLite Database
	store, err := models.NewStore("file:nagare.db?cache=shared")
	if err != nil {
		log.Fatalf("Failed to initialize database: %v", err)
	}
	defer store.Close()

	// 2. Initialize Scheduler and load DAGs
	sched := scheduler.NewScheduler(store)
	if err := sched.LoadDAGs("dags"); err != nil {
		log.Fatalf("Failed to load DAGs: %v", err)
	}

	// 3. Initialize Worker Pool
	// Passing the dags map from scheduler for task lookups.
	// We'll use a small fixed pool of 4 workers for the MVP.
	pool := worker.NewPool(store, sched.GetDAGs(), 4)

	// 4. Initialize API Server
	apiServer := api.NewServer(store, sched)

	// Context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 5. Start concurrent workers and API Server
	pool.Start(ctx)

	go func() {
		// Strip the "web/out" prefix from the embedded file system
		fSys, err := fs.Sub(frontendEmbedFS, "web/out")
		if err != nil {
			log.Fatalf("Failed to initialize frontend FS: %v", err)
		}

		if err := apiServer.Start(":8080", fSys); err != nil {
			log.Fatalf("API Server failed: %v", err)
		}
	}()

	// Setup OS signal capture for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// 6. Main Control Loop
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	log.Println("Nagare is running. Press Ctrl+C to exit.")

	for {
		select {
		case <-sigChan:
			log.Println("Received shutdown signal, terminating workers...")
			cancel()    // Cancel context to stop workers
			pool.Stop() // Wait for workers to finish current jobs
			log.Println("Nagare shut down successfully")
			return

		case <-ticker.C:
			// Run the scheduler tick to evaluate crons and check dependencies
			if err := sched.Tick(); err != nil {
				log.Printf("Scheduler tick error: %v", err)
			}

			// Dispatch queued tasks to the workers
			if err := pool.Dispatch(); err != nil {
				log.Printf("Worker dispatch error: %v", err)
			}
		}
	}
}
