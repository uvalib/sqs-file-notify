package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/uvalib/virgo4-sqs-sdk/awssqs"
	"os"
)

//
// main entry point
//
func main() {

	// Get config params and use them to init service context. Any issues are fatal
	cfg := LoadConfiguration()

	// load our AWS_SQS helper object
	aws, err := awssqs.NewAwsSqs(awssqs.AwsSqsConfig{MessageBucketName: "bla"})
	fatalIfError(err)

	// get the queue handles from the queue name
	outQueueHandle, err := aws.QueueHandle(cfg.OutQueueName)
	fatalIfError(err)

	// just doing a single key
	if len(cfg.ObjectKey) != 0 {
		// get the necessary S3 attributes and create the outbound message
		message, err := makeOutboundMessage(cfg.BucketName, cfg.ObjectKey)
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
			message, err := makeOutboundMessage(cfg.BucketName, key)
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

func makeOutboundMessage(bucket string, key string) (*awssqs.Message, error) {

	s3Info, err := s3info(bucket, key)
	if err != nil {
		return nil, err
	}

	// build the expected structure
	events := Events{}
	event := S3EventRecord{}
	events.Records = make([]S3EventRecord, 0, 1)
	event.S3 = *s3Info
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
