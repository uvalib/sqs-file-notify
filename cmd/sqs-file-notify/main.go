package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"

	"github.com/uvalib/uva-aws-s3-sdk/uva-s3"
	"github.com/uvalib/virgo4-sqs-sdk/awssqs"
)

//
// main entry point
//
func main() {

	// Get config params and use them to init service context. Any issues are fatal
	cfg := LoadConfiguration()

	// load our AWS sqs helper object
	aws, err := awssqs.NewAwsSqs(awssqs.AwsSqsConfig{MessageBucketName: "bla"})
	fatalIfError(err)

	// load our AWS s3 helper object
	s3Svc, err := uva_s3.NewUvaS3(uva_s3.UvaS3Config{Logging: true})
	fatalIfError(err)

	// get the queue handles from the queue name
	outQueueHandle, err := aws.QueueHandle(cfg.OutQueueName)
	fatalIfError(err)

	// just doing a single key
	if len(cfg.ObjectKey) != 0 {
		// get the necessary S3 attributes and create the outbound message
		message, err := makeOutboundMessage(s3Svc, cfg.BucketName, cfg.ObjectKey)
		fatalIfError(err)

		// the block of messages to send
		block := make([]awssqs.Message, 0, 1)
		block = append(block, *message)

		// write to the outbound queue
		_, err = aws.BatchMessagePut(outQueueHandle, block)
		fatalIfError(err)

		fmt.Printf("1 of 1: notified s3://%s/%s OK\n", cfg.BucketName, cfg.ObjectKey)
	} else {
		file, err := os.Open(cfg.ObjectKeyFile)
		fatalIfError(err)
		scanner := bufio.NewScanner(file)
		scanner.Split(bufio.ScanLines)
		var keys []string

		for scanner.Scan() {
			keys = append(keys, scanner.Text())
		}

		file.Close()

		batchSize := 10
		block := make([]awssqs.Message, 0, batchSize)
		for ix, key := range keys {

			// get the necessary S3 attributes and create the outbound message
			message, err := makeOutboundMessage(s3Svc, cfg.BucketName, key)
			fatalIfError(err)

			// the block of messages to send
			block = append(block, *message)

			// time to send the block
			if len(block) == batchSize {

				// write to the outbound queue
				_, err = aws.BatchMessagePut(outQueueHandle, block)
				fatalIfError(err)
				fmt.Printf("%d of %d: notified OK\n", ix+1, len(keys))

				// truncate the block
				block = block[:0]
			}

		}
		if len(block) != 0 {

			// write to the outbound queue
			_, err = aws.BatchMessagePut(outQueueHandle, block)
			fatalIfError(err)
			fmt.Printf("%d of %d: notified OK\n", len(keys), len(keys))
		}
	}
}

func makeOutboundMessage(s3Svc uva_s3.UvaS3, bucket string, key string) (*awssqs.Message, error) {

	// get the bucket object details
	o := uva_s3.NewUvaS3Object(bucket, key)
	s3Obj, err := s3Svc.StatObject(o)
	if err != nil {
		return nil, err
	}

	// build the expected structure
	events := Events{}
	events.Records = make([]S3EventRecord, 0, 1)
	event := S3EventRecord{}
	res := S3Record{}
	res.Bucket.Name = s3Obj.BucketName()
	res.Object.Key = s3Obj.KeyName()
	res.Object.Size = s3Obj.Size()
	event.S3 = res
	events.Records = append(events.Records, event)

	buff, err := json.Marshal(events)
	if err != nil {
		return nil, err
	}

	// the outbound message
	message := awssqs.Message{}

	// because SQS expects you to include an attribute
	message.Attribs = append(message.Attribs, awssqs.Attribute{Name: "this", Value: "that"})
	message.Payload = buff

	return &message, nil
}

//
// end of file
//
