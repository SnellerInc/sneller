// Copyright (C) 2022 Sneller, Inc.
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"

	"golang.org/x/sys/cpu"
)

func exitf(err error) {
	log.Print(err)
	_, _ = fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}

func getDirectory() string {
	_, filename, _, ok := runtime.Caller(1)
	if !ok {
		exitf(fmt.Errorf("unable to get the current filename"))
	}
	return path.Join(path.Dir(filename), "../../vm")
}

func getTestsNames(testDir string) []string {
	cmd := exec.Command("go", "test", "-list", ".")
	var outBuffer bytes.Buffer
	cmd.Stdout = &outBuffer
	cmd.Dir = testDir
	if err := cmd.Run(); err != nil {
		exitf(err)
	}

	result := make([]string, 0)
	for _, line := range strings.Split(outBuffer.String(), "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "ok ") {
			break
		}
		if strings.HasPrefix(line, "Test") {
			result = append(result, line)
		}
	}
	return result
}

func runTestAllSingletons(testDir, crashDir string, count, timeoutSec, par int, vmFence bool) {
	testNames := getTestsNames(testDir)
	nTests := len(testNames)
	counter := uint32(0)
	totalCount := nTests
	log.Printf("retrieved %v tests in directory %v", nTests, testDir)

	var wg sync.WaitGroup
	worker := func(workerId int, jobChan <-chan uint32) {
		defer wg.Done()
		for i := range jobChan {
			currentCount := atomic.AddUint32(&counter, 1)
			info := fmt.Sprintf("Singleton %v/%v", currentCount, totalCount)
			fileID := fmt.Sprintf("s%02v", i)
			callGoTest(testDir, crashDir, []string{testNames[i]}, count, timeoutSec, vmFence, fileID, info)
		}
	}

	jobs := make(chan uint32, par*2)
	for w := 0; w < par; w++ {
		go worker(w, jobs)
	}
	wg.Add(par)
	for i := 0; i < nTests; i++ {
		jobs <- uint32(i)
	}
	close(jobs)
	wg.Wait()
}

func runTestAllPairs(testDir, crashDir string, count, timeoutSec, par int, vmFence bool) {
	testNames := getTestsNames(testDir)
	nTests := len(testNames)
	counter := uint32(0)
	testPairs := make([]uint32, 0)

	for i := 0; i < nTests; i++ {
		for j := i + 1; j < nTests; j++ {
			testPairs = append(testPairs, (uint32(i)<<16)|uint32(j))
		}
	}
	totalCount := len(testPairs)

	log.Printf("retrieved %v tests in directory %v", nTests, testDir)

	var wg sync.WaitGroup
	worker := func(workerId int, jobChan <-chan uint32) {
		defer wg.Done()
		for k := range jobChan {
			i := k >> 16
			j := k & 0xFFFF

			currentCount := atomic.AddUint32(&counter, 1)
			info := fmt.Sprintf("Pair %v/%v", currentCount, totalCount)
			fileID := fmt.Sprintf("p%02v-%02v", i, j)
			callGoTest(testDir, crashDir, []string{testNames[i], testNames[j]}, count, timeoutSec, vmFence, fileID, info)
		}
	}

	jobs := make(chan uint32, 2*par)
	for w := 0; w < par; w++ {
		go worker(w, jobs)
	}
	wg.Add(par)

	for i := range testPairs {
		jobs <- testPairs[i]
	}
	close(jobs)
	wg.Wait()
}

// callGoTest invokes a "go test -run <testNames> -count <count> -timeout <timeoutSec>s" and
// saves issues to file in crashDir
func callGoTest(testDir, crashDir string, testNames []string, count, timeoutSec int, vmFence bool, fileID, info string) {
	// create arguments for the cmd
	args := []string{"test"}
	nTests := len(testNames)
	if nTests == 0 {
		args = append(args, "./...")
	} else if nTests == 1 {
		args = append(args, "-run", testNames[0])
	} else {
		args = append(args, "-run", fmt.Sprintf("%v", strings.Join(testNames, "|")))
	}
	if vmFence {
		args = append(args, "-tags=vmfence")
	}
	args = append(args, "-count", fmt.Sprintf("%v", count))
	args = append(args, "-timeout", fmt.Sprintf("%vs", timeoutSec))
	cmd := exec.Command("go", args...)

	var outBuffer bytes.Buffer
	cmd.Stdout = &outBuffer
	cmd.Dir = testDir

	log.Printf("%v: cmd=%v", info, cmd.String())
	_ = cmd.Run() //ignore the error, it is handled when parsing outBuffer

	// process the results of the cmd
	resultLines := strings.Split(outBuffer.String(), "\n")
	if len(resultLines) > 1 {
		if (resultLines[0] == "PASS") && strings.HasPrefix(resultLines[1], "ok") {
			// thumbs up; everything ok
		} else if strings.HasPrefix(resultLines[0], "panic: test timed out after") {
			// no issues, just a timeout
			log.Printf("%v: Timeout after %v seconds", info, timeoutSec)
		} else {
			if _, err := os.Stat(crashDir); errors.Is(err, os.ErrNotExist) {
				if err = os.MkdirAll(crashDir, os.ModePerm); err != nil {
					exitf(err)
				}
			}

			name := strings.ReplaceAll(strings.Join(testNames, "-"), "/", "_")
			file, err := os.CreateTemp(crashDir, fmt.Sprintf("crash.%v.%v.*.txt", fileID, name))
			if err != nil {
				exitf(err)
			}
			if _, err := file.WriteString(fmt.Sprintf("%v: cmd=%v\n\n%v", info, cmd.String(), outBuffer.String())); err != nil {
				exitf(err)
			}
			if err := file.Close(); err != nil {
				exitf(err)
			}

			colorRed := "\033[31m"
			colorReset := "\033[0m"
			log.Printf("%v: %vCrash in: %v; saved %v%v", info, colorRed, testNames, file.Name(), colorReset)
		}
	}
}

var (
	dashTestDir  string // test directory
	dashCrashDir string // crash directory
	dashPair     bool   // singleton / pair
	dashCount    int    // count
	dashTimeout  int    // timeout seconds
	dashPar      int    // parallelism
	dashVMFence  bool   // use vmFence
)

func init() {
	flag.StringVar(&dashTestDir, "testdir", getDirectory(), "directory contains the Go tests")
	flag.StringVar(&dashCrashDir, "crashdir", dashTestDir+"/crashes", "directory to save crash reports")
	flag.BoolVar(&dashPair, "pair", false, "either run single tests; or run tests pairs")
	flag.IntVar(&dashCount, "count", 10000, "number of times a test is run")
	flag.IntVar(&dashTimeout, "timeout", 60, "timeout in seconds")
	flag.IntVar(&dashPar, "par", 16, "parallelism")
	flag.BoolVar(&dashVMFence, "vmfence", false, "whether to use vmfence")
}

func main() {
	if !cpu.X86.HasAVX512 {
		exitf(fmt.Errorf("CPU doesn't support AVX-512"))
	}
	flag.Parse()
	log.Printf("retrieved param -testdir %v", dashTestDir)
	log.Printf("retrieved param -crashdir %v", dashCrashDir)
	log.Printf("retrieved param -pair %v", dashPair)
	log.Printf("retrieved param -count %v", dashCount)
	log.Printf("retrieved param -timeout %v", dashTimeout)
	log.Printf("retrieved param -par %v", dashPar)
	log.Printf("retrieved param -vmfence %v", dashVMFence)

	if dashPair {
		runTestAllPairs(dashTestDir, dashCrashDir, dashCount, dashTimeout, dashPar, dashVMFence)
	} else {
		runTestAllSingletons(dashTestDir, dashCrashDir, dashCount, dashTimeout, dashPar, dashVMFence)
	}
}
