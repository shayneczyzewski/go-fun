package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"time"
)

type Job struct {
	Name    string
	Timeout int
}

func (j Job) String() string {
	return "[" + j.Name + ", " + strconv.Itoa(j.Timeout) + "s]"
}

type Worker struct {
	Name          string
	WorkerChannel chan Worker
	JobChannel    chan Job
	QuitChannel   chan bool
}

func CreateWorker(name string, workerChannel chan Worker) Worker {
	return Worker{
		Name:          name,
		WorkerChannel: workerChannel,
		JobChannel:    make(chan Job),
		QuitChannel:   make(chan bool)}
}

func (w Worker) start(wg *sync.WaitGroup) {
	go func() {
		defer wg.Done()

		for {
			// Indicate we are ready to start new work
			w.WorkerChannel <- w

			select {
			case job := <-w.JobChannel:
				fmt.Println(time.Now().String() + " - " + w.Name + ": " + job.String())

				// TODO: Do some actual work
				time.Sleep(time.Duration(job.Timeout) * time.Second)

			case <-w.QuitChannel:
				fmt.Printf("%s stopping\n", w.Name)
				return
			}
		}
	}()
}

type WorkerPool struct {
	Name          string
	NumWorkers    int
	WorkerChannel chan Worker
	QuitChannel   chan bool
}

func CreateWorkerPool(name string, numWorkers int) WorkerPool {
	return WorkerPool{
		Name:          name,
		NumWorkers:    numWorkers,
		WorkerChannel: make(chan Worker, numWorkers),
		QuitChannel:   make(chan bool)}
}

func (wp WorkerPool) start(wg *sync.WaitGroup) {
	go func() {
		defer wg.Done()

		var workerWg sync.WaitGroup

		// Create and start all workers
		var workers []Worker
		for i := 1; i <= wp.NumWorkers; i++ {
			var worker = CreateWorker(wp.Name+" | worker"+strconv.Itoa(i), wp.WorkerChannel)
			workers = append(workers, worker)
			workerWg.Add(1)
			worker.start(&workerWg)
		}

		// Send them work to do, and...
		go func() {
			for i := 0; ; i++ {
				select {
				case worker := <-wp.WorkerChannel:
					worker.JobChannel <- Job{Name: "job" + strconv.Itoa(i), Timeout: rand.Intn(15)}
				case <-wp.QuitChannel:
					fmt.Println(wp.Name + " | Received stop request. Stopping all workers!")
					go func() {
						for _, worker := range workers {
							fmt.Printf("%s | Quitting Worker %s\n", wp.Name, worker.Name)
							worker.QuitChannel <- true
						}
					}()
					return
				}

			}
		}()

		// Wait for them to finish after receiving a stop request
		workerWg.Wait()
	}()
}

func main() {
	numWorkerPools := flag.Int("numWorkerPools", 5, "")
	numWorkers := flag.Int("numWorkers", 10, "")
	flag.Parse()

	var workerPools []WorkerPool

	for i := 1; i <= *numWorkerPools; i++ {
		workerPools = append(workerPools, CreateWorkerPool("wp"+strconv.Itoa(i), *numWorkers))
	}

	// Use a SIGINT to stop the worker pools
	go func() {
		interruptChannel := make(chan os.Signal)
		signal.Notify(interruptChannel, os.Interrupt)
		<-interruptChannel

		for _, wp := range workerPools {
			wp.QuitChannel <- true
		}
	}()

	// Start all worker pools and wait for all of them to complete after interrupt
	var workerPoolWg sync.WaitGroup
	workerPoolWg.Add(len(workerPools))
	for _, wp := range workerPools {
		wp.start(&workerPoolWg)
	}
	workerPoolWg.Wait()
}
