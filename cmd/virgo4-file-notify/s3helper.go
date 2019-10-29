package main

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

// taken from https://docs.aws.amazon.com/sdk-for-go/api/service/s3/#example_S3_HeadObject_shared00

func s3info(bucket string, key string) (*S3Record, error) {

	sess, err := session.NewSession()
	if err != nil {
		return nil, err
	}

	service := s3.New(sess)

	input := &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}

	result, err := service.HeadObject(input)
	if err != nil {
		return nil, err
	}

	res := S3Record{}
	res.Bucket.Name = bucket
	res.Object.Key = key
	res.Object.Size = *result.ContentLength

	return &res, nil
}

//
// end of file
//
