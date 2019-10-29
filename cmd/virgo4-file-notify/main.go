package main

import (
	"encoding/json"
	"fmt"
	"github.com/uvalib/virgo4-sqs-sdk/awssqs"
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

	// get the necessary S3 attributes and create the outbound message
	message, err := makeOutboundMessage(cfg.BucketName, cfg.ObjectKey)
	fatalIfError(err)

	// the block of messages to send
	block := make([]awssqs.Message, 0, 1)
	block = append(block, *message)

	// write to the outbound queue
	_, err = aws.BatchMessagePut(outQueueHandle, block)
	fatalIfError(err)

	fmt.Printf("Notified s3://%s/%s OK\n", cfg.BucketName, cfg.ObjectKey )
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
