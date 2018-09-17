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

package cmd

import (
	"bytes"
	"crypto/rand"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

type runCmd struct {
	accessKey        string
	secretKey        string
	endpoint         string
	bucket           string
	objectSize       int64
	objectNamePrefix string
	numClients       uint
	numSamples       uint
	verbose          bool
}

// runCmd represents the run command
var cmd = &cobra.Command{
	Use:   "run",
	Short: "Run benchmark tests",
	Long:  ``,
	Args:  cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		var run = &runCmd{
			accessKey:        viper.GetString("accessKey"),
			secretKey:        viper.GetString("secretKey"),
			endpoint:         viper.GetString("endpoint"),
			bucket:           viper.GetString("bucket"),
			objectSize:       viper.GetInt64("objectSize"),
			objectNamePrefix: viper.GetString("objectNamePrefix"),
			numClients:       viper.GetSizeInBytes("numClients"),
			numSamples:       viper.GetSizeInBytes("numSamples"),
			verbose:          viper.GetBool("verbose"),
		}
		run.benchmark()
	},
}

func init() {
	rootCmd.AddCommand(cmd)
	cmd.Flags().String("endpoint", "", "AWS endpoint (required)")
	viper.BindPFlag("endpoint", cmd.Flags().Lookup("endpoint"))
	// fmt.Printf("log: %s\n", viper.Get("endpoint"))
	// if viper.GetString("endpoint") == "" {
	// 	cmd.MarkFlagRequired("endpoint")
	// }
}

type Req interface{}

type Resp struct {
	err      error
	duration time.Duration
	numBytes int64
}

type Params struct {
	operation        string
	requests         chan Req
	responses        chan Resp
	numSamples       uint
	numClients       uint
	objectSize       int64
	objectNamePrefix string
	bucket           string
	endpoints        []string
	verbose          bool
}

var bufferBytes []byte

const (
	opRead  = "Read"
	opWrite = "Write"
	//max that can be deleted at a time via DeleteObjects()
	commitSize = 1000
)

func (r *runCmd) benchmark() error {
	if r.numClients > r.numSamples || r.numSamples < 1 {
		fmt.Printf("numClients(%d) needs to be less than numSamples(%d) and greater than 0\n", r.numClients, r.numSamples)
		os.Exit(1)
	}

	if r.endpoint == "" {
		fmt.Println("You need to specify endpoint(s)")
		flag.PrintDefaults()
		os.Exit(1)
	}

	params := Params{
		requests:         make(chan Req),
		responses:        make(chan Resp),
		numSamples:       r.numSamples,
		numClients:       uint(r.numClients),
		objectSize:       r.objectSize,
		objectNamePrefix: r.objectNamePrefix,
		bucket:           r.bucket,
		endpoints:        strings.Split(r.endpoint, ","),
		verbose:          r.verbose,
	}
	fmt.Println(params)

	fmt.Println()

	// Generate the data from which we will do the writting
	fmt.Printf("Generating in-memory sample data... ")
	timeGenData := time.Now()
	bufferBytes = make([]byte, r.objectSize, r.objectSize)
	_, err := rand.Read(bufferBytes)
	if err != nil {
		return fmt.Errorf("Could not allocate a buffer")
	}
	fmt.Printf("Done (%s)\n", time.Since(timeGenData))
	fmt.Println()

	region := "us-east-1"
	// Start the load clients and run a write test followed by a read test
	cfg := &aws.Config{
		Credentials:      credentials.NewStaticCredentials(r.accessKey, r.secretKey, ""),
		Region:           aws.String(region),
		S3ForcePathStyle: aws.Bool(true),
	}

	params.StartClients(cfg)

	fmt.Printf("Running %s test...\n", opWrite)
	writeResult := params.Run(opWrite)
	fmt.Println()

	fmt.Printf("Running %s test...\n", opRead)
	readResult := params.Run(opRead)
	fmt.Println()

	fmt.Println(params)
	fmt.Println()
	fmt.Println(writeResult)
	fmt.Println()
	fmt.Println(readResult)

	skipCleanup := true
	if !skipCleanup {
		fmt.Println()
		fmt.Printf("Cleaning up %d objects...\n", r.numSamples)
		delStartTime := time.Now()
		svc := s3.New(session.New(), cfg)

		numSuccessfullyDeleted := 0

		keyList := make([]*s3.ObjectIdentifier, 0, commitSize)
		for i := 0; uint(i) < r.numSamples; i++ {
			bar := s3.ObjectIdentifier{
				Key: aws.String(fmt.Sprintf("%s%d", r.objectNamePrefix, i)),
			}
			keyList = append(keyList, &bar)
			if len(keyList) == commitSize || i == int(r.numSamples)-1 {
				fmt.Printf("Deleting a batch of %d objects in range {%d, %d}... ", len(keyList), i-len(keyList)+1, i)
				params := &s3.DeleteObjectsInput{
					Bucket: aws.String(r.bucket),
					Delete: &s3.Delete{
						Objects: keyList}}
				_, err := svc.DeleteObjects(params)
				if err == nil {
					numSuccessfullyDeleted += len(keyList)
					fmt.Printf("Succeeded\n")
				} else {
					fmt.Printf("Failed (%v)\n", err)
				}
				//set cursor to 0 so we can move to the next batch.
				keyList = keyList[:0]

			}
		}
		fmt.Printf("Successfully deleted %d/%d objects in %s\n", numSuccessfullyDeleted, r.numSamples, time.Since(delStartTime))
	}

	return nil
}

func (params Params) String() string {
	output := fmt.Sprintln("Test parameters")
	output += fmt.Sprintf("endpoint(s):      %s\n", params.endpoints)
	output += fmt.Sprintf("bucket:           %s\n", params.bucket)
	output += fmt.Sprintf("objectNamePrefix: %s\n", params.objectNamePrefix)
	output += fmt.Sprintf("objectSize:       %0.4f MB\n", float64(params.objectSize)/(1024*1024))
	output += fmt.Sprintf("numClients:       %d\n", params.numClients)
	output += fmt.Sprintf("numSamples:       %d\n", params.numSamples)
	output += fmt.Sprintf("verbose:          %t\n", params.verbose)
	return output
}

func (r Result) String() string {
	report := fmt.Sprintf("Results Summary for %s Operation(s)\n", r.operation)
	report += fmt.Sprintf("Total Transferred: %0.3f MB\n", float64(r.bytesTransmitted)/(1024*1024))
	report += fmt.Sprintf("Total Throughput:  %0.2f MB/s\n", (float64(r.bytesTransmitted)/(1024*1024))/r.totalDuration.Seconds())
	report += fmt.Sprintf("Total Duration:    %0.3f s\n", r.totalDuration.Seconds())
	report += fmt.Sprintf("Number of Errors:  %d\n", r.numErrors)
	if len(r.opDurations) > 0 {
		report += fmt.Sprintln("------------------------------------")
		report += fmt.Sprintf("%s times Max:       %0.3f s\n", r.operation, r.percentile(100))
		report += fmt.Sprintf("%s times 99th %%ile: %0.3f s\n", r.operation, r.percentile(99))
		report += fmt.Sprintf("%s times 90th %%ile: %0.3f s\n", r.operation, r.percentile(90))
		report += fmt.Sprintf("%s times 75th %%ile: %0.3f s\n", r.operation, r.percentile(75))
		report += fmt.Sprintf("%s times 50th %%ile: %0.3f s\n", r.operation, r.percentile(50))
		report += fmt.Sprintf("%s times 25th %%ile: %0.3f s\n", r.operation, r.percentile(25))
		report += fmt.Sprintf("%s times Min:       %0.3f s\n", r.operation, r.percentile(0))
	}
	return report
}

func (r Result) percentile(i int) float64 {
	if i >= 100 {
		i = len(r.opDurations) - 1
	} else if i > 0 && i < 100 {
		i = int(float64(i) / 100 * float64(len(r.opDurations)))
	}
	return r.opDurations[i]
}

func (params *Params) Run(op string) Result {
	startTime := time.Now()

	// Start submitting load requests
	go params.submitLoad(op)

	// Collect and aggregate stats for completed requests
	result := Result{opDurations: make([]float64, 0, params.numSamples), operation: op}
	for i := 0; uint(i) < params.numSamples; i++ {
		resp := <-params.responses
		errorString := ""
		if resp.err != nil {
			result.numErrors++
			errorString = fmt.Sprintf(", error: %s", resp.err)
		} else {
			result.bytesTransmitted = result.bytesTransmitted + params.objectSize
			result.opDurations = append(result.opDurations, resp.duration.Seconds())
		}
		if params.verbose {
			fmt.Printf("%v operation completed in %0.2fs (%d/%d) - %0.2fMB/s%s\n",
				op, resp.duration.Seconds(), i+1, params.numSamples,
				(float64(result.bytesTransmitted)/(1024*1024))/time.Since(startTime).Seconds(),
				errorString)
		}
	}

	result.totalDuration = time.Since(startTime)
	sort.Float64s(result.opDurations)
	return result
}

func (params *Params) submitLoad(op string) {
	bucket := aws.String(params.bucket)
	for i := 0; uint(i) < params.numSamples; i++ {
		key := aws.String(fmt.Sprintf("%s%d", params.objectNamePrefix, i))
		if op == opWrite {
			params.requests <- &s3.PutObjectInput{
				Bucket: bucket,
				Key:    key,
				Body:   bytes.NewReader(bufferBytes),
			}
		} else if op == opRead {
			params.requests <- &s3.GetObjectInput{
				Bucket: bucket,
				Key:    key,
			}
		} else {
			panic("Developer error")
		}
	}
}

type Result struct {
	operation        string
	bytesTransmitted int64
	numErrors        int
	opDurations      []float64
	totalDuration    time.Duration
}

func (params *Params) StartClients(cfg *aws.Config) {
	for i := 0; i < int(params.numClients); i++ {
		cfg.Endpoint = aws.String(params.endpoints[i%len(params.endpoints)])
		go params.startClient(cfg)
	}
}

// Run an individual load request
func (params *Params) startClient(cfg *aws.Config) {
	svc := s3.New(session.New(), cfg)
	for request := range params.requests {
		putStartTime := time.Now()
		var err error
		numBytes := params.objectSize

		switch r := request.(type) {
		case *s3.PutObjectInput:
			req, _ := svc.PutObjectRequest(r)
			// Disable payload checksum calculation (very expensive)
			req.HTTPRequest.Header.Add("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")
			err = req.Send()
		case *s3.GetObjectInput:
			req, resp := svc.GetObjectRequest(r)
			err = req.Send()
			numBytes = 0
			if err == nil {
				numBytes, err = io.Copy(ioutil.Discard, resp.Body)
			}
			if numBytes != params.objectSize {
				err = fmt.Errorf("expected object length %d, actual %d", params.objectSize, numBytes)
			}
		default:
			panic("Developer error")
		}

		params.responses <- Resp{err, time.Since(putStartTime), numBytes}
	}
}
