package main

import (
	"flag"
	"log"
)

// ServiceConfig defines all of the service configuration parameters
type ServiceConfig struct {
	OutQueueName  string // SQS queue name for outbound documents
	BucketName    string // the bucket name
	ObjectKey     string // the object key
	ObjectKeyFile string // the file of object keys
}

// LoadConfiguration will load the service configuration from env/cmdline
// and return a pointer to it. Any failures are fatal.
func LoadConfiguration() *ServiceConfig {

	var cfg ServiceConfig

	flag.StringVar(&cfg.OutQueueName, "outqueue", "", "Output queue name")
	flag.StringVar(&cfg.BucketName, "bucket", "", "The bucket name")
	flag.StringVar(&cfg.ObjectKey, "key", "", "The object key")
	flag.StringVar(&cfg.ObjectKeyFile, "keyfile", "", "The object key file")

	flag.Parse()

	if len(cfg.OutQueueName) == 0 {
		log.Fatalf("outqueue cannot be blank")
	}
	if len(cfg.BucketName) == 0 {
		log.Fatalf("bucket cannot be blank")
	}
	if len(cfg.ObjectKey) == 0 && len(cfg.ObjectKeyFile) == 0 {
		log.Fatalf("key or keyfile must be specified")
	} else {
		if len(cfg.ObjectKey) != 0 && len(cfg.ObjectKeyFile) != 0 {
			log.Fatalf("one of key or keyfile must be specified")
		}
	}

	return &cfg
}
