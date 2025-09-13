package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"sort"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func main() {
	bucket := flag.String("bucket", "", "S3 bucket name")
	key := flag.String("key", "", "S3 object key")
	filePath := flag.String("file", "", "Path to the local file")
	flag.Parse()

	if *bucket == "" || *key == "" || *filePath == "" {
		flag.Usage()
		log.Fatal("bucket, key, and file must all be provided")
	}

	ctx := context.Background()

	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		log.Fatalf("unable to load SDK config, %v", err)
	}

	client := s3.NewFromConfig(cfg)

	f, err := os.Open(*filePath)
	if err != nil {
		log.Fatalf("failed to open file, %v", err)
	}

	defer f.Close()

	// 1. Initiate multipart upload
	createResp, err := client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: bucket,
		Key:    key,
	})
	if err != nil {
		log.Fatalf("failed to create multipart upload: %v", err)
	}

	uploadID := *createResp.UploadId
	fmt.Println("Upload ID: ", uploadID)

	var completedParts []types.CompletedPart

	partNum := int32(1)

	buffer := make([]byte, 5*1024*1024)

	for {
		n, err := f.Read(buffer)

		if err != nil && err != io.EOF {
			log.Fatalf("failed to read file: %v", err)
		}

		if n == 0 {
			break
		}

		// 2. Upload each part
		partResp, err := client.UploadPart(ctx, &s3.UploadPartInput{
			Bucket:     bucket,
			Key:        key,
			PartNumber: aws.Int32(partNum),
			UploadId:   &uploadID,
			Body:       bytes.NewReader(buffer[:n]),
		})
		if err != nil {
			// Abort on failure
			_, _ = client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
				Bucket:   bucket,
				Key:      key,
				UploadId: &uploadID,
			})
			log.Fatalf("failed to upload part %d: %v", partNum, err)
		}

		fmt.Printf("Uploaded part %d, ETag: %s\n", partNum, *partResp.ETag)

		pn := partNum
		completedParts = append(completedParts, types.CompletedPart{
			ETag:       partResp.ETag,
			PartNumber: aws.Int32(pn),
		})

		partNum++
	}

	sort.Slice(completedParts, func(i, j int) bool {
		return *completedParts[i].PartNumber < *completedParts[j].PartNumber
	})

	// 3. Complete the upload
	_, err = client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   bucket,
		Key:      key,
		UploadId: &uploadID,
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: completedParts,
		},
	})
	if err != nil {
		log.Fatalf("failed to complete multipart upload: %v", err)
	}

	fmt.Println("Upload completed successfully!")
}
