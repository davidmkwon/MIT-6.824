package mapreduce

import (
	"fmt"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (Map
// or Reduce). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//

	// populate readyChan with registerChan workers
	readyChan := make(chan string)
	go func() {
		for {
			select {
			case worker := <-registerChan:
				readyChan <- worker
			default:
			}
		}
	}()

	// create task chan that holds the index of each task to be completed
	taskChan := make(chan int, ntasks)
	for i := 0; i < ntasks; i++ {
		taskChan <- i
	}

	// select workers from the readyChan and call DoTask rpc on them until
	// we have completed ntasks
	var wg sync.WaitGroup
	wg.Add(ntasks)
	go func() {
		for taskIdx := range taskChan {
			worker := <-readyChan
			go func(worker string, taskIdx int) {
				task := DoTaskArgs{jobName, mapFiles[taskIdx], phase, taskIdx, n_other}
				success := call(worker, "Worker.DoTask", task, nil)
				if success {
					wg.Done()
				} else {
					// failed: add the task back to the taskChan queue
					taskChan <- taskIdx
				}
				readyChan <- worker
			}(worker, taskIdx)
		}
	}()

	// wait for tasks to finish
	wg.Wait()
	close(taskChan)

	fmt.Printf("Schedule: %v phase done\n", phase)
}
