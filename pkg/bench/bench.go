// Copyright © 2018 Giacomo Guiulfo <giacomoguiulfo@gmail.com>
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package bench

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"io"
	"io/ioutil"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

// Config holds the configuration paramters for the Runner type
type Config struct {
	AccessKey        string
	SecretKey        string
	Region           string
	Operation        string
	Clients          uint
	ObjectSize       int64
	ObjectSplit      uint
	ObjectCount      uint
	ObjectNamePrefix string
	Bucket           string
	Endpoint         string
	Verbose          bool
	Write            bool
	Read             bool
	Cleanup          bool
}

type request interface{}

type response struct {
	err      error
	duration time.Duration
	bytes    int64
}

type report struct {
	Operation        string
	bytesTransmitted int64
	numErrors        int
	opDurations      []float64
	totalDuration    time.Duration
}

// A Runner performs benchmark tests by managing multiple goroutines
type Runner struct {
	conf      *Config
	endpoints []string
	requests  chan request
	responses chan response
}

const (
	readOp     = "Read"
	writeOp    = "Write"
	commitSize = 1000
)

type RepeatReader struct {
	reader        io.ReadSeeker
	numBytes      int64
	numRepeat     uint
	currentRepeat uint
}

// Mark performs a benchmark test on the configured service
func Mark(conf *Config) error {
	if err := conf.validate(); err != nil {
		return err
	}
	awsCfg := &aws.Config{
		Credentials:      credentials.NewStaticCredentials(conf.AccessKey, conf.SecretKey, ""),
		Region:           aws.String(conf.Region),
		S3ForcePathStyle: aws.Bool(true),
	}
	runner := Runner{
		conf:      conf,
		requests:  make(chan request),
		responses: make(chan response),
		endpoints: strings.Split(conf.Endpoint, ","),
	}
	fmt.Println(runner)
	bufferBytes, err := generateSampleData(conf.ObjectSize / int64(conf.ObjectSplit))
	if err != nil {
		return err
	}
	runner.prepare(awsCfg)
	writeReport := runner.run(writeOp, bufferBytes)
	readReport := runner.run(readOp, bufferBytes)
	runner.cleanup(awsCfg)
	fmt.Println(writeReport)
	fmt.Println(readReport)
	return nil
}

func (conf *Config) validate() error {
	if conf.Clients > conf.ObjectCount || conf.ObjectCount < 1 {
		return fmt.Errorf("numClients(%d) needs to be less than numSamples(%d) and greater than 0", conf.Clients, conf.ObjectCount)
	}
	if conf.Endpoint == "" {
		return fmt.Errorf("You need to specify one or more endpoints")
	}
	return nil
}

func generateSampleData(size int64) ([]byte, error) {
	fmt.Printf("Generating in-memory sample data... ")
	timeGenData := time.Now()
	buffer := make([]byte, size, size)
	_, err := rand.Read(buffer)
	if err != nil {
		return nil, fmt.Errorf("Could not allocate a buffer")
	}
	fmt.Printf("Done (%s)\n", time.Since(timeGenData))
	return buffer, nil
}

func (r *Runner) prepare(cfg *aws.Config) {
	for i := uint(0); i < r.conf.Clients; i++ {
		cfg.Endpoint = aws.String(r.endpoints[i%uint(len(r.endpoints))])
		go r.startClient(cfg)
	}
}

func (r *Runner) startClient(cfg *aws.Config) {
	client := s3.New(session.New(), cfg)
	for request := range r.requests {
		startTime := time.Now()
		bytes := r.conf.ObjectSize
		var err error
		switch reqType := request.(type) {
		case *s3.PutObjectInput:
			req, _ := client.PutObjectRequest(reqType)
			req.HTTPRequest.Header.Add("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")
			err = req.Send()
		case *s3.GetObjectInput:
			req, resp := client.GetObjectRequest(reqType)
			if err = req.Send(); err == nil {
				bytes, err = io.Copy(ioutil.Discard, resp.Body)
			} else {
				bytes = 0
			}
			if bytes != r.conf.ObjectSize {
				err = fmt.Errorf("Expected object length %d, actual %d", r.conf.ObjectSize, bytes)
			}
		default:
			panic("Unexpected error")
		}
		r.responses <- response{
			err,
			time.Since(startTime),
			bytes,
		}
	}
}

func (r *Runner) run(op string, bufferBytes []byte) report {
	if (r.conf.Write == false && op == writeOp) ||
		(r.conf.Read == false && op == readOp) {
		return report{}
	}
	startTime := time.Now()
	fmt.Printf("Running %s test...\n", op)
	go r.submitLoad(op, bufferBytes)
	report := report{opDurations: make([]float64, 0, r.conf.ObjectCount), Operation: op}
	for i := uint(0); i < r.conf.ObjectCount; i++ {
		resp := <-r.responses
		errorString := ""
		if resp.err != nil {
			report.numErrors++
			errorString = fmt.Sprintf(", error: %s", resp.err)
		} else {
			report.bytesTransmitted += r.conf.ObjectSize
			report.opDurations = append(report.opDurations, resp.duration.Seconds())
		}
		if r.conf.Verbose {
			fmt.Printf("%v Operation completed in %0.2fs (%d/%d) - %0.2fMB/s%s\n",
				op, resp.duration.Seconds(), i+1, r.conf.ObjectCount,
				(float64(report.bytesTransmitted)/(1024*1024))/time.Since(startTime).Seconds(),
				errorString)
		}
	}
	report.totalDuration = time.Since(startTime)
	sort.Float64s(report.opDurations)
	return report
}

func (r *Runner) submitLoad(op string, bufferBytes []byte) {
	Bucket := aws.String(r.conf.Bucket)
	for i := uint(0); i < r.conf.ObjectCount; i++ {
		key := aws.String(fmt.Sprintf("%s%d", r.conf.ObjectNamePrefix, i))
		if op == writeOp {
			reader := RepeatReader{bytes.NewReader(bufferBytes), int64(len(bufferBytes)), r.conf.ObjectSplit, 0}
			r.requests <- &s3.PutObjectInput{
				Bucket: Bucket,
				Key:    key,
				Body:   &reader,
			}
		} else if op == readOp {
			r.requests <- &s3.GetObjectInput{
				Bucket: Bucket,
				Key:    key,
			}
		} else {
			panic("Invalid Operation")
		}
	}
}

func (r *Runner) cleanup(awsCfg *aws.Config) {
	if r.conf.Cleanup == false {
		return
	}
	fmt.Printf("Cleaning up %d objects...\n", r.conf.ObjectCount)
	startTime := time.Now()
	client := s3.New(session.New(), awsCfg)

	deletedObjects := 0

	keyList := make([]*s3.ObjectIdentifier, 0, commitSize)
	for i := 0; uint(i) < r.conf.ObjectCount; i++ {
		key := s3.ObjectIdentifier{
			Key: aws.String(fmt.Sprintf("%s%d", r.conf.ObjectNamePrefix, i)),
		}
		keyList = append(keyList, &key)
		if len(keyList) == commitSize || i == int(r.conf.ObjectCount)-1 {
			fmt.Printf("Deleting a batch of %d objects in range {%d, %d}... ", len(keyList), i-len(keyList)+1, i)
			params := &s3.DeleteObjectsInput{
				Bucket: aws.String(r.conf.Bucket),
				Delete: &s3.Delete{
					Objects: keyList}}
			_, err := client.DeleteObjects(params)
			if err == nil {
				deletedObjects += len(keyList)
				fmt.Println("Succeeded")
			} else {
				fmt.Printf("Failed (%v)\n", err)
			}
			keyList = keyList[:0]
		}
	}
	fmt.Printf("Successfully deleted %d/%d objects in %s\n", deletedObjects, r.conf.ObjectCount, time.Since(startTime))
}

func (r Runner) String() string {
	output := fmt.Sprintln("Test parameters")
	output += fmt.Sprintf("Endpoint(s):      %s\n", r.endpoints)
	output += fmt.Sprintf("Bucket:           %s\n", r.conf.Bucket)
	output += fmt.Sprintf("ObjectNamePrefix: %s\n", r.conf.ObjectNamePrefix)
	output += fmt.Sprintf("ObjectSize:       %0.4f MB\n", float64(r.conf.ObjectSize)/(1024*1024))
	output += fmt.Sprintf("ObjectSplit:      %d\n", r.conf.ObjectSplit)
	output += fmt.Sprintf("numClients:       %d\n", r.conf.Clients)
	output += fmt.Sprintf("numSamples:       %d\n", r.conf.ObjectCount)
	output += fmt.Sprintf("Verbose:          %t\n", r.conf.Verbose)
	return output
}

func (r report) String() string {
	report := fmt.Sprintf("Results Summary for %s Operation(s)\n", r.Operation)
	report += fmt.Sprintf("Total Transferred: %0.3f MB\n", float64(r.bytesTransmitted)/(1024*1024))
	report += fmt.Sprintf("Total Throughput:  %0.2f MB/s\n", (float64(r.bytesTransmitted)/(1024*1024))/r.totalDuration.Seconds())
	report += fmt.Sprintf("Total Duration:    %0.3f s\n", r.totalDuration.Seconds())
	report += fmt.Sprintf("Number of Errors:  %d\n", r.numErrors)
	if len(r.opDurations) > 0 {
		report += fmt.Sprintln("------------------------------------")
		report += fmt.Sprintf("%s times Max:       %0.3f s\n", r.Operation, r.percentile(100))
		report += fmt.Sprintf("%s times 99th %%ile: %0.3f s\n", r.Operation, r.percentile(99))
		report += fmt.Sprintf("%s times 90th %%ile: %0.3f s\n", r.Operation, r.percentile(90))
		report += fmt.Sprintf("%s times 75th %%ile: %0.3f s\n", r.Operation, r.percentile(75))
		report += fmt.Sprintf("%s times 50th %%ile: %0.3f s\n", r.Operation, r.percentile(50))
		report += fmt.Sprintf("%s times 25th %%ile: %0.3f s\n", r.Operation, r.percentile(25))
		report += fmt.Sprintf("%s times Min:       %0.3f s\n", r.Operation, r.percentile(0))
	}
	return report
}

func (r report) percentile(i int) float64 {
	if i >= 100 {
		i = len(r.opDurations) - 1
	} else if i > 0 && i < 100 {
		i = int(float64(i) / 100 * float64(len(r.opDurations)))
	}
	return r.opDurations[i]
}

func (r *RepeatReader) Read(b []byte) (int, error) {
	n, err := r.reader.Read(b)
	if err != nil && err != io.EOF {
		return n, err
	}
	if err != nil && n != 0 {
		return n, nil
	}
	if err != nil {
		r.currentRepeat += 1
		if r.currentRepeat == r.numRepeat {
			return n, err
		}
		r.reader.Seek(0, io.SeekStart)
		return r.Read(b)
	}
	return n, err
}

func (r *RepeatReader) Seek(offset int64, whence int) (int64, error) {
	if whence == io.SeekCurrent {
		currentOffset, err := r.reader.Seek(0, io.SeekCurrent)
		if err != nil {
			return currentOffset * int64(r.currentRepeat), err
		}
		offset = currentOffset + offset
		whence = io.SeekStart
	}
	div, rem := offset/int64(r.numBytes), offset%int64(r.numBytes)
	switch whence {
	case io.SeekStart:
		r.currentRepeat = uint(div)
	case io.SeekEnd:
		r.currentRepeat = r.numRepeat - uint(div) - 1
	}
	currentOffset, err := r.reader.Seek(rem, whence)
	return int64(r.numBytes)*int64(r.currentRepeat) + currentOffset, err
}
