package main

import (
	"bytes"
	"crypto/sha1"
	"encoding/csv"
	"flag"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/external"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/schollz/progressbar/v2"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"
)

// represents the duration from making an S3 GetObject request to getting the first byte and last byte
type latency struct {
	FirstByte time.Duration
	LastByte  time.Duration
}

// represents the duration for upload operations
type uploadLatency struct {
	StartTime time.Duration  // Time to start the upload
	EndTime   time.Duration  // Time to complete the upload
}

// summary statistics used to summarize first byte and last byte latencies
type stat int

const (
	avg stat = iota + 1
)

// a benchmark record for one object size and thread count
type benchmark struct {
	objectSize uint64
	threads    int
	firstByte  map[stat]float64
	lastByte   map[stat]float64
	dataPoints []latency
}

// upload benchmark record for one object size and thread count
type uploadBenchmark struct {
	objectSize     uint64
	threads        int
	rate           float64  // throughput rate in MB/s
	startTime      map[stat]float64
	endTime        map[stat]float64
	uploadDataPoints []uploadLatency
}

// absolute limits
const maxPayload = 18
const maxThreads = 64

// default settings
const defaultRegion = "us-west-2"
const bucketNamePrefix = "s3-benchmark"

// the hostname or EC2 instance id
var hostname = getHostname()

// the EC2 instance region if available
var region = getRegion()

// the endpoint URL if applicable
var endpoint string

// the EC2 instance type if available
var instanceType = getInstanceType()

// the script will automatically create an S3 bucket to use for the test, and it tries to get a unique bucket name
// by generating a sha hash of the hostname
var bucketName = fmt.Sprintf("%s-%x", bucketNamePrefix, sha1.Sum([]byte(hostname)))

// the min and max object sizes to test - 1 = 1 KB, and the size doubles with every increment
var payloadsMin int
var payloadsMax int

// the min and max thread count to use in the test
var threadsMin int
var threadsMax int

// the number of samples to collect for each benchmark record
var samples int

// a test mode to find out when EC2 network throttling kicks in
var throttlingMode bool

// flag to cleanup the s3 bucket and exit the program
var cleanupOnly bool

// if not empty, the results of the test get uploaded to S3 using this key prefix
var csvResults string

// flag to create the s3 bucket
var createBucket bool

// the S3 SDK client
var s3Client *s3.S3

// store upload benchmarks for later display
var uploadBenchmarks []uploadBenchmark

// program entry point
func main() {
	// parse the program arguments and set the global variables
	parseFlags()

	// set up the S3 SDK
	setupS3Client()

	// if given the flag to cleanup only, then run the cleanup and exit the program
	if cleanupOnly {
		cleanup()
		return
	}

	// create the S3 bucket and upload the test data
	setup()

	// run the test against the uploaded data
	runBenchmark()

	// remove the objects uploaded to S3 for this test (but doesn't remove the bucket)
	cleanup()
}

func parseFlags() {
	threadsMinArg := flag.Int("threads-min", 8, "The minimum number of threads to use when fetching objects from S3.")
	threadsMaxArg := flag.Int("threads-max", 16, "The maximum number of threads to use when fetching objects from S3.")
	payloadsMinArg := flag.Int("payloads-min", 1, "The minimum object size to test, with 1 = 1 KB, and every increment is a double of the previous value.")
	payloadsMaxArg := flag.Int("payloads-max", 10, "The maximum object size to test, with 1 = 1 KB, and every increment is a double of the previous value.")
	samplesArg := flag.Int("samples", 1000, "The number of samples to collect for each test of a single object size and thread count.")
	bucketNameArg := flag.String("bucket-name", "", "Cleans up all the S3 artifacts used by the benchmarks.")
	regionArg := flag.String("region", "", "Sets the AWS region to use for the S3 bucket. Only applies if the bucket doesn't already exist.")
	endpointArg := flag.String("endpoint", "", "Sets the S3 endpoint to use. Only applies to non-AWS, S3-compatible stores.")
	fullArg := flag.Bool("full", false, "Runs the full exhaustive test, and overrides the threads and payload arguments.")
	throttlingModeArg := flag.Bool("throttling-mode", false, "Runs a continuous test to find out when EC2 network throttling kicks in.")
	cleanupArg := flag.Bool("cleanup", false, "Cleans all the objects uploaded to S3 for this test.")
	csvResultsArg := flag.String("upload-csv", "", "Uploads the test results to S3 as a CSV file.")
	createBucketArg := flag.Bool("create-bucket", true, "Create the bucket")
	
	// parse the arguments and set all the global variables accordingly
	flag.Parse()

	if *bucketNameArg != "" {
		bucketName = *bucketNameArg
	}

	if *regionArg != "" {
		region = *regionArg
	}

	if *endpointArg != "" {
		endpoint = *endpointArg
	}

	payloadsMin = *payloadsMinArg
	payloadsMax = *payloadsMaxArg
	threadsMin = *threadsMinArg
	threadsMax = *threadsMaxArg
	samples = *samplesArg
	cleanupOnly = *cleanupArg
	csvResults = *csvResultsArg
	createBucket = *createBucketArg

	if payloadsMin > payloadsMax {
		payloadsMin = payloadsMax
	}

	if threadsMin > threadsMax {
		threadsMin = threadsMax
	}

	if *fullArg {
		// if running the full exhaustive test, the threads and payload arguments get overridden with these
		threadsMin = 1
		threadsMax = 48
		payloadsMin = 1  //  1 KB
		payloadsMax = 16 // 32 MB
	}

	if *throttlingModeArg {
		// if running the network throttling test, the threads and payload arguments get overridden with these
		threadsMin = 36
		threadsMax = 36
		payloadsMin = 15 // 16 MB
		payloadsMax = 15 // 16 MB
		throttlingMode = *throttlingModeArg
	}
}

func setupS3Client() {
	// gets the AWS credentials from the default file or from the EC2 instance profile
	cfg, err := external.LoadDefaultAWSConfig()
	if err != nil {
		panic("Unable to load AWS SDK config: " + err.Error())
	}

	// set the SDK region to either the one from the program arguments or else to the same region as the EC2 instance
	cfg.Region = region

	// set the endpoint in the configuration
	if endpoint != "" {
		cfg.EndpointResolver = aws.ResolveWithEndpointURL(endpoint)
	}

	// set a 3-minute timeout for all S3 calls, including downloading the body
	cfg.HTTPClient = &http.Client{
		Timeout: time.Second * 180,
	}

	// crete the S3 client
	s3Client = s3.New(cfg)

	// custom endpoints don't generally work with the bucket in the host prefix
	if endpoint != "" {
		s3Client.ForcePathStyle = true
	}
}

func setup() {
	fmt.Print("\n--- \033[1;32mUPLOAD\033[0m --------------------------------------------------------------------------------------------------------------------\n\n")
	if createBucket {
		// try to create the S3 bucket
		createBucketReq := s3Client.CreateBucketRequest(&s3.CreateBucketInput{
			Bucket: aws.String(bucketName),
			CreateBucketConfiguration: &s3.CreateBucketConfiguration{
				LocationConstraint: s3.NormalizeBucketLocation(s3.BucketLocationConstraint(region)),
			},
		})

		// AWS S3 has this peculiar issue in which if you want to create bucket in us-east-1 region, you should NOT specify 
		// any location constraint. https://github.com/boto/boto3/issues/125
		if strings.ToLower(region) == "us-east-1" {
			createBucketReq = s3Client.CreateBucketRequest(&s3.CreateBucketInput{
				Bucket: aws.String(bucketName),
			})
		}

		_, err := createBucketReq.Send()

		// if the error is because the bucket already exists, ignore the error
		if err != nil && !strings.Contains(err.Error(), "BucketAlreadyOwnedByYou:") {
			panic("Failed to create S3 bucket: " + err.Error())
		}	
	}

	// an object size iterator that starts from 1 KB and doubles the size on every iteration
	generatePayload := payloadSizeGenerator()

	// print the header for the upload benchmark of this object size
	printHeader()

	// loop over every payload size
	for p := 1; p <= payloadsMax; p++ {
		// get an object size from the iterator
		objectSize := generatePayload()

		// ignore payloads smaller than the min argument
		if p < payloadsMin {
			continue
		}

		// first, create objects for all thread counts (needed for download benchmarks)
		for t := 1; t <= threadsMax; t++ {
			// generate an S3 key from the sha hash of the hostname, thread index, and object size
			key := generateS3Key(hostname, t, objectSize)

			// do a HeadObject request to avoid uploading the object if it already exists from a previous test run
			headReq := s3Client.HeadObjectRequest(&s3.HeadObjectInput{
				Bucket: aws.String(bucketName),
				Key:    aws.String(key),
			})

			_, err := headReq.Send()

			// if no error, then the object exists, so skip this one
			if err == nil {
				continue
			}

			// if other error, exit
			if err != nil && !strings.Contains(err.Error(), "NotFound:") {
				panic("Failed to head S3 object: " + err.Error())
			}

			// generate empty payload
			payload := make([]byte, objectSize)

			// do a PutObject request to create the object
			putReq := s3Client.PutObjectRequest(&s3.PutObjectInput{
				Bucket: aws.String(bucketName),
				Key:    aws.String(key),
				Body:   bytes.NewReader(payload),
			})

			_, err = putReq.Send()

			// if the put fails, exit
			if err != nil {
				panic("Failed to put S3 object: " + err.Error())
			}
		}

		// run upload tests per thread count and object size combination
		for t := threadsMin; t <= threadsMax; t++ {
			execUploadTest(t, objectSize)
		}
		fmt.Print("+---------+--------------+----------------+-------------------------+----------------------------------------------+\n")
	}
}

func execUploadTest(threadCount int, payloadSize uint64) {
	// this overrides the sample count on small hosts that can get overwhelmed by a large throughput
	samples := getTargetSampleCount(threadCount, samples)

	// a channel to submit the upload test tasks
	uploadTasks := make(chan int, threadCount)

	// a channel to receive results from the upload test tasks back on the main thread
	results := make(chan uploadLatency, samples)

	// create the workers for all the threads in this test
	for w := 1; w <= threadCount; w++ {
		go func(o int, tasks <-chan int, results chan<- uploadLatency) {
			for taskNum := range tasks {
				// generate a unique S3 key for upload benchmarking (different from download test objects)
				key := fmt.Sprintf("upload-benchmark-%s-%03d-%012d-%03d", hostname, o, payloadSize, taskNum)

				// generate empty payload
				payload := make([]byte, payloadSize)

				// start the timer to measure the upload latencies
				uploadTimer := time.Now()

				// do a PutObject request to create the object
				putReq := s3Client.PutObjectRequest(&s3.PutObjectInput{
					Bucket: aws.String(bucketName),
					Key:    aws.String(key),
					Body:   bytes.NewReader(payload),
				})

				// measure the start time (time to initiate request)
				startTime := time.Now().Sub(uploadTimer)

				_, err := putReq.Send()

				// if the put fails, exit
				if err != nil {
					panic("Failed to put S3 object: " + err.Error())
				}

				// measure the end time (time to complete upload)
				endTime := time.Now().Sub(uploadTimer)

				// add the upload latency result to the results channel
				results <- uploadLatency{StartTime: startTime, EndTime: endTime}

				// clean up the benchmark object immediately after measurement
				deleteReq := s3Client.DeleteObjectRequest(&s3.DeleteObjectInput{
					Bucket: aws.String(bucketName),
					Key:    aws.String(key),
				})
				_, _ = deleteReq.Send() // ignore errors for cleanup
			}
		}(w, uploadTasks, results)
	}

	// start the timer for this benchmark
	benchmarkTimer := time.Now()

	// submit all the upload test tasks
	for j := 1; j <= samples; j++ {
		uploadTasks <- j
	}

	// close the channel
	close(uploadTasks)

	// construct a new upload benchmark record
	uploadBenchmarkRecord := uploadBenchmark{
		startTime: make(map[stat]float64),
		endTime:   make(map[stat]float64),
	}
	sumStartTime := int64(0)
	sumEndTime := int64(0)
	uploadBenchmarkRecord.threads = threadCount
	uploadBenchmarkRecord.objectSize = payloadSize

	// wait for all the results to come and collect the individual datapoints
	for s := 1; s <= samples; s++ {
		timing := <-results
		uploadBenchmarkRecord.uploadDataPoints = append(uploadBenchmarkRecord.uploadDataPoints, timing)
		sumStartTime += timing.StartTime.Nanoseconds()
		sumEndTime += timing.EndTime.Nanoseconds()
	}

	// stop the timer for this benchmark
	totalTime := time.Now().Sub(benchmarkTimer)

	// calculate the summary statistics for the start time latencies
	sort.Sort(ByUploadStartTime(uploadBenchmarkRecord.uploadDataPoints))
	uploadBenchmarkRecord.startTime[avg] = (float64(sumStartTime) / float64(samples)) / 1000000

	// calculate the summary statistics for the end time latencies
	sort.Sort(ByUploadEndTime(uploadBenchmarkRecord.uploadDataPoints))
	uploadBenchmarkRecord.endTime[avg] = (float64(sumEndTime) / float64(samples)) / 1000000

	// calculate the throughput rate (total bytes uploaded / total time)
	totalBytes := float64(payloadSize * uint64(samples))
	rate := totalBytes / totalTime.Seconds() / 1024 / 1024

	// store the rate in the benchmark record
	uploadBenchmarkRecord.rate = rate

	// determine what to put in the first column of the results
	c := uploadBenchmarkRecord.threads
	if throttlingMode {
		c = 1 // or some run number if needed
	}

	// calculate bandwidth in different units
	bandwidthMiB := rate // rate is already in MB/s, convert to MiB/s: MB/s * (1000^2) / (1024^2)
	bandwidthMiB = bandwidthMiB * 1000000 / (1024 * 1024)
	bandwidthGiB := bandwidthMiB / 1024
	bandwidthGB := rate * 1000 / 1000 // Convert MB/s to GB/s: MB/s * 1000 / 1000

	// convert average latency from milliseconds to microseconds
	avgLatencyUs := uploadBenchmarkRecord.endTime[avg] * 1000

	// print the results to stdout
	fmt.Printf("| %7d | %12s | \033[1;31m%9.1f MB/s\033[0m | %21.0f | %14.9f  %14.9f  %14.9f |\n",
		c, formatPayloadSize(payloadSize), rate, avgLatencyUs, bandwidthMiB, bandwidthGiB, bandwidthGB)

	// store the upload benchmark for later use in CSV
	uploadBenchmarks = append(uploadBenchmarks, uploadBenchmarkRecord)
}

func runBenchmark() {
	fmt.Print("\n--- \033[1;32mDOWNLOAD\033[0m ----------------------------------------------------------------------------------------------------------------\n\n")

	// array of csv records used to upload the results to S3 when the test is finished
	var csvRecords [][]string

	// add CSV header row
	csvRecords = append(csvRecords, []string{
		"hostname", "instance_type", "payload_size", "threads",
		"download_rate_mbps", "download_avg_latency_us", "download_bandwidth_mib_sec", "download_bandwidth_gib_sec", "download_bandwidth_gb_sec",
		"upload_rate_mbps", "upload_avg_latency_us", "upload_bandwidth_mib_sec", "upload_bandwidth_gib_sec", "upload_bandwidth_gb_sec",
	})

	// an object size iterator that starts from 1 KB and doubles the size on every iteration
	generatePayload := payloadSizeGenerator()

	// print the header for the benchmark of this object size
	printHeader()

	// loop over every payload size
	for p := 1; p <= payloadsMax; p++ {
		// get an object size from the iterator
		payload := generatePayload()

		// ignore payloads smaller than the min argument
		if p < payloadsMin {
			continue
		}

		// run a test per thread count and object size combination
		for t := threadsMin; t <= threadsMax; t++ {
			// if throttling mode, loop forever
			for n := 1; true; n++ {
				csvRecords = execTest(t, payload, n, csvRecords)
				if !throttlingMode {
					break
				}
			}
		}
		fmt.Print("+---------+--------------+----------------+-------------------------+----------------------------------------------+\n")
	}

	// if the csv option is true, upload the csv results to S3
	if csvResults != "" {
		b := &bytes.Buffer{}
		w := csv.NewWriter(b)
		_ = w.WriteAll(csvRecords)

		// create the s3 key based on the prefix argument and instance type
		key := "results/" + csvResults + "-" + instanceType

		// do the PutObject request
		putReq := s3Client.PutObjectRequest(&s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    &key,
			Body:   bytes.NewReader(b.Bytes()),
		})

		_, err := putReq.Send()

		// if the request fails, exit
		if err != nil {
			panic("Failed to put object: " + err.Error())
		}

		fmt.Printf("CSV results uploaded to \033[1;33ms3://%s/%s\033[0m\n", bucketName, key)
	}
}

func execTest(threadCount int, payloadSize uint64, runNumber int, csvRecords [][]string) [][]string {
	// this overrides the sample count on small hosts that can get overwhelmed by a large throughput
	samples := getTargetSampleCount(threadCount, samples)

	// a channel to submit the test tasks
	testTasks := make(chan int, threadCount)

	// a channel to receive results from the test tasks back on the main thread
	results := make(chan latency, samples)

	// create the workers for all the threads in this test
	for w := 1; w <= threadCount; w++ {
		go func(o int, tasks <-chan int, results chan<- latency) {
			for range tasks {
				// generate an S3 key from the sha hash of the hostname, thread index, and object size
				key := generateS3Key(hostname, o, payloadSize)

				// start the timer to measure the first byte and last byte latencies
				latencyTimer := time.Now()

				// do the GetObject request
				req := s3Client.GetObjectRequest(&s3.GetObjectInput{
					Bucket: aws.String(bucketName),
					Key:    aws.String(key),
				})

				resp, err := req.Send()

				// if a request fails, exit
				if err != nil {
					panic("Failed to get object: " + err.Error())
				}

				// measure the first byte latency
				firstByte := time.Now().Sub(latencyTimer)

				// create a buffer to copy the S3 object body to
				var buf = make([]byte, payloadSize)

				// read the s3 object body into the buffer
				for {
					_, err := resp.Body.Read(buf)

					if err == io.EOF {
						break
					}

					// if the streaming fails, exit
					if err != nil {
						panic("Error reading object body: " + err.Error())
					}
				}

				_ = resp.Body.Close()

				// measure the last byte latency
				lastByte := time.Now().Sub(latencyTimer)

				// add the latency result to the results channel
				results <- latency{FirstByte: firstByte, LastByte: lastByte}
			}
		}(w, testTasks, results)
	}

	// start the timer for this benchmark
	benchmarkTimer := time.Now()

	// submit all the test tasks
	for j := 1; j <= samples; j++ {
		testTasks <- j
	}

	// close the channel
	close(testTasks)

	// construct a new benchmark record
	benchmarkRecord := benchmark{
		firstByte: make(map[stat]float64),
		lastByte:  make(map[stat]float64),
	}
	sumFirstByte := int64(0)
	sumLastByte := int64(0)
	benchmarkRecord.threads = threadCount

	// wait for all the results to come and collect the individual datapoints
	for s := 1; s <= samples; s++ {
		timing := <-results
		benchmarkRecord.dataPoints = append(benchmarkRecord.dataPoints, timing)
		sumFirstByte += timing.FirstByte.Nanoseconds()
		sumLastByte += timing.LastByte.Nanoseconds()
		benchmarkRecord.objectSize += payloadSize
	}

	// stop the timer for this benchmark
	totalTime := time.Now().Sub(benchmarkTimer)

	// calculate the summary statistics for the first byte latencies
	sort.Sort(ByFirstByte(benchmarkRecord.dataPoints))
	benchmarkRecord.firstByte[avg] = (float64(sumFirstByte) / float64(samples)) / 1000000

	// calculate the summary statistics for the last byte latencies
	sort.Sort(ByLastByte(benchmarkRecord.dataPoints))
	benchmarkRecord.lastByte[avg] = (float64(sumLastByte) / float64(samples)) / 1000000

	// calculate the throughput rate
	rate := (float64(benchmarkRecord.objectSize)) / (totalTime.Seconds()) / 1024 / 1024

	// determine what to put in the first column of the results
	c := benchmarkRecord.threads
	if throttlingMode {
		c = runNumber
	}

	// calculate bandwidth in different units
	bandwidthMiB := rate // rate is already in MB/s, convert to MiB/s: MB/s * (1000^2) / (1024^2)
	bandwidthMiB = bandwidthMiB * 1000000 / (1024 * 1024)
	bandwidthGiB := bandwidthMiB / 1024
	bandwidthGB := rate * 1000 / 1000 // Convert MB/s to GB/s: MB/s * 1000 / 1000

	// convert average latency from milliseconds to microseconds  
	avgLatencyUs := benchmarkRecord.lastByte[avg] * 1000

	// print the results to stdout
	fmt.Printf("| %7d | %12s | \033[1;31m%9.1f MB/s\033[0m | %21.0f | %14.9f  %14.9f  %14.9f |\n",
		c, formatPayloadSize(payloadSize), rate, avgLatencyUs, bandwidthMiB, bandwidthGiB, bandwidthGB)

	// find matching upload benchmark for this payload size and thread count
	var uploadBenchmarkRecord *uploadBenchmark
	for i := range uploadBenchmarks {
		if uploadBenchmarks[i].objectSize == payloadSize && uploadBenchmarks[i].threads == benchmarkRecord.threads {
			uploadBenchmarkRecord = &uploadBenchmarks[i]
			break
		}
	}

	// add the results to the csv array
	csvRecord := []string{
		fmt.Sprintf("%s", hostname),
		fmt.Sprintf("%s", instanceType),
		fmt.Sprintf("%d", payloadSize),
		fmt.Sprintf("%d", benchmarkRecord.threads),
		fmt.Sprintf("%.3f", rate), // download rate
		fmt.Sprintf("%.0f", avgLatencyUs), // download average latency in microseconds
		fmt.Sprintf("%.2f", bandwidthMiB), // download bandwidth in MiB/sec
		fmt.Sprintf("%.3f", bandwidthGiB), // download bandwidth in GiB/sec
		fmt.Sprintf("%.3f", bandwidthGB), // download bandwidth in GB/sec
	}

	// add upload statistics if available
	if uploadBenchmarkRecord != nil {
		// calculate upload bandwidth in different units
		uploadBandwidthMiB := uploadBenchmarkRecord.rate * 1000000 / (1024 * 1024)
		uploadBandwidthGiB := uploadBandwidthMiB / 1024
		uploadBandwidthGB := uploadBenchmarkRecord.rate * 1000 / 1000
		uploadAvgLatencyUs := uploadBenchmarkRecord.endTime[avg] * 1000

		csvRecord = append(csvRecord, []string{
			fmt.Sprintf("%.3f", uploadBenchmarkRecord.rate), // upload rate
			fmt.Sprintf("%.0f", uploadAvgLatencyUs), // upload average latency in microseconds
			fmt.Sprintf("%.2f", uploadBandwidthMiB), // upload bandwidth in MiB/sec
			fmt.Sprintf("%.3f", uploadBandwidthGiB), // upload bandwidth in GiB/sec
			fmt.Sprintf("%.3f", uploadBandwidthGB), // upload bandwidth in GB/sec
		}...)
	} else {
		// add empty upload statistics if no upload data available
		csvRecord = append(csvRecord, []string{
			"0", "0", "0", "0", "0", // upload rate and stats
		}...)
	}

	csvRecords = append(csvRecords, csvRecord)

	return csvRecords
}

// prints the table header for the test results
func printHeader() {
	// print the table header
	fmt.Println("+------------------------+----------------+-----------------------+------------------------------------------------+")
	fmt.Println("|                        |                |        Avg Lat.       |                   Bandwidth                    |")
	fmt.Println("|                        |                |          (us)         |                                                |")
	fmt.Println("+---------+--------------+----------------+-----------------------+------------------------------------------------+")
	if !throttlingMode {
		fmt.Println("| Threads | Payload Size |   Throughput   |        Latency        |  B/W (MiB/Sec)    B/W (GiB/Sec)    B/W (GB/Sec)|")
	} else {
		fmt.Println("|       # | Payload Size |   Throughput   |        Latency        |  B/W (MiB/Sec)    B/W (GiB/Sec)    B/W (GB/Sec)|")
	}
	fmt.Println("+---------+--------------+----------------+-------------------------+----------------------------------------------+")
}

// generates an S3 key from the sha hash of the hostname, thread index, and object size
func generateS3Key(host string, threadIndex int, payloadSize uint64) string {
	keyHash := sha1.Sum([]byte(fmt.Sprintf("%s-%03d-%012d", host, threadIndex, payloadSize)))
	key := fmt.Sprintf("%x", keyHash)
	return key
}

// cleans up the objects uploaded to S3 for this test (but doesn't remove the bucket)
func cleanup() {
	fmt.Print("\n--- \033[1;32mCLEANUP\033[0m ------------------------------------------------------------------------------------------------------------------\n\n")

	fmt.Printf("Deleting any objects uploaded from %s\n", hostname)

	// create a progress bar
	bar := progressbar.NewOptions(maxPayload*maxThreads-1, progressbar.OptionSetRenderBlankState(true))

	// an object size iterator that starts from 1 KB and doubles the size on every iteration
	generatePayload := payloadSizeGenerator()

	// loop over every payload size
	for p := 1; p <= maxPayload; p++ {
		// get an object size from the iterator
		payloadSize := generatePayload()

		// loop over each possible thread to clean up objects from any previous test execution
		for t := 1; t <= maxThreads; t++ {
			// increment the progress bar
			_ = bar.Add(1)

			// generate an S3 key from the sha hash of the hostname, thread index, and object size
			key := generateS3Key(hostname, t, payloadSize)

			// make a DeleteObject request
			headReq := s3Client.DeleteObjectRequest(&s3.DeleteObjectInput{
				Bucket: aws.String(bucketName),
				Key:    aws.String(key),
			})

			_, err := headReq.Send()

			// if the object doesn't exist, ignore the error
			if err != nil && !strings.HasPrefix(err.Error(), "NotFound: Not Found") {
				panic("Failed to delete object: " + err.Error())
			}
		}
	}
	fmt.Print("\n\n")
}

// gets the hostname or the EC2 instance ID
func getHostname() string {
	instanceId := getInstanceId()
	if instanceId != "" {
		return instanceId
	}

	hostname, err := os.Hostname()
	if err != nil {
		panic(err)
	}
	return hostname
}



// gets the EC2 region from the instance metadata
func getRegion() string {
	httpClient := &http.Client{
		Timeout: time.Second,
	}

	link := "http://169.254.169.254/latest/meta-data/placement/availability-zone"
	response, err := httpClient.Get(link)
	if err != nil {
		return defaultRegion
	}

	content, _ := ioutil.ReadAll(response.Body)
	_ = response.Body.Close()

	az := string(content)

	return az[:len(az)-1]
}

// gets the EC2 instance type from the instance metadata
func getInstanceType() string {
	httpClient := &http.Client{
		Timeout: time.Second,
	}

	link := "http://169.254.169.254/latest/meta-data/instance-type"
	response, err := httpClient.Get(link)
	if err != nil {
		return ""
	}

	content, _ := ioutil.ReadAll(response.Body)
	_ = response.Body.Close()

	return string(content)
}

// gets the EC2 instance ID from the instance metadata
func getInstanceId() string {
	httpClient := &http.Client{
		Timeout: time.Second,
	}

	link := "http://169.254.169.254/latest/meta-data/instance-id"
	response, err := httpClient.Get(link)
	if err != nil {
		return ""
	}

	content, _ := ioutil.ReadAll(response.Body)
	_ = response.Body.Close()

	return string(content)
}

// format payload size for display (convert bytes to KB, MB, GB as appropriate)
func formatPayloadSize(size uint64) string {
	if size < 1024 {
		return fmt.Sprintf("%dB", size)
	} else if size < 1024*1024 {
		return fmt.Sprintf("%dKB", size/1024)
	} else if size < 1024*1024*1024 {
		return fmt.Sprintf("%dMB", size/(1024*1024))
	} else {
		return fmt.Sprintf("%.1fGB", float64(size)/(1024*1024*1024))
	}
}

// returns an object size iterator, starting from 1 KB and double in size by each iteration
func payloadSizeGenerator() func() uint64 {
	nextPayloadSize := uint64(1024)

	return func() uint64 {
		thisPayloadSize := nextPayloadSize
		nextPayloadSize *= 2
		return thisPayloadSize
	}
}

// adjust the sample count for small instances and for low thread counts (so that the test doesn't take forever)
func getTargetSampleCount(threads int, tasks int) int {
	if instanceType == "" {
		return minimumOf(50, tasks)
	}
	if !strings.Contains(instanceType, "xlarge") && !strings.Contains(instanceType, "metal") {
		return minimumOf(50, tasks)
	}
	if threads <= 4 {
		return minimumOf(100, tasks)
	}
	if threads <= 8 {
		return minimumOf(250, tasks)
	}
	if threads <= 16 {
		return minimumOf(500, tasks)
	}
	return tasks
}

// go doesn't seem to have a min function in the std lib!
func minimumOf(x, y int) int {
	if x < y {
		return x
	}
	return y
}

// comparator to sort by first byte latency
type ByFirstByte []latency

func (a ByFirstByte) Len() int           { return len(a) }
func (a ByFirstByte) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByFirstByte) Less(i, j int) bool { return a[i].FirstByte < a[j].FirstByte }

// comparator to sort by last byte latency
type ByLastByte []latency

func (a ByLastByte) Len() int           { return len(a) }
func (a ByLastByte) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByLastByte) Less(i, j int) bool { return a[i].LastByte < a[j].LastByte }

// comparator to sort by upload start time
type ByUploadStartTime []uploadLatency

func (a ByUploadStartTime) Len() int           { return len(a) }
func (a ByUploadStartTime) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByUploadStartTime) Less(i, j int) bool { return a[i].StartTime < a[j].StartTime }

// comparator to sort by upload end time
type ByUploadEndTime []uploadLatency

func (a ByUploadEndTime) Len() int           { return len(a) }
func (a ByUploadEndTime) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByUploadEndTime) Less(i, j int) bool { return a[i].EndTime < a[j].EndTime }
