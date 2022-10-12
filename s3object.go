package objectstore

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type s3Object struct {
	basepath string
}

func newS3Object(basepath string) *s3Object {
	return &s3Object{basepath: basepath}
}

func (s3o *s3Object) Read(name string) ([]byte, error) {
	bucket, prefix, err := parseS3Path(s3o.basepath)
	if err != nil {
		return []byte{}, err
	}
	object := joinPath(prefix, name)
	client, err := getS3Client()
	if err != nil {
		return []byte{}, err
	}
	ctx := context.Background()
	params := &s3.GetObjectInput{
		Bucket: &bucket,
		Key:    &object,
	}
	obj, err := client.GetObject(ctx, params)
	if err != nil {
		return []byte{}, err
	}
	return io.ReadAll(obj.Body)
}

func (s3o *s3Object) Write(name string, data []byte) error {
	bucket, prefix, err := parseS3Path(s3o.basepath)
	if err != nil {
		return err
	}
	object := joinPath(prefix, name)
	client, err := getS3Client()
	if err != nil {
		return err
	}
	ctx := context.Background()
	ct := http.DetectContentType(data)
	params := &s3.PutObjectInput{
		Bucket: &bucket,
		Key:    &object,
		ACL:    types.ObjectCannedACLPrivate,
		Body:   bytes.NewReader(data),
		//ContentLength: aws.Int64(len(data)),
		ContentType: &ct,
	}
	_, err = client.PutObject(ctx, params)
	return err
}

func (s3o *s3Object) Delete(name string) error {
	bucket, prefix, err := parseS3Path(s3o.basepath)
	if err != nil {
		return err
	}
	object := joinPath(prefix, name)
	client, err := getS3Client()
	if err != nil {
		return err
	}
	ctx := context.Background()
	input := &s3.DeleteObjectInput{
		Bucket: &bucket,
		Key:    &object,
	}
	_, err = client.DeleteObject(ctx, input)
	return err
}

func (s3o *s3Object) List() ([]string, error) {
	files := []string{}
	bucket, prefix, err := parseS3Path(s3o.basepath)
	if err != nil {
		return files, err
	}
	client, err := getS3Client()
	if err != nil {
		return files, err
	}
	ctx := context.Background()
	params := &s3.ListObjectsV2Input{
		Bucket: &bucket,
		Prefix: &prefix,
	}
	p := s3.NewListObjectsV2Paginator(client, params, func(o *s3.ListObjectsV2PaginatorOptions) {
		if v := int32(100); v != 0 {
			o.Limit = v
		}
	})
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			return files, err
		}
		for _, obj := range page.Contents {
			name := strings.TrimPrefix(*obj.Key, prefix+"/")
			files = append(files, name)
		}
	}
	return files, err
}

func getS3Client() (*s3.Client, error) {
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		return nil, err
	}
	return s3.NewFromConfig(cfg), nil
}

func parseS3Path(path string) (bucket, prefix string, err error) {
	uri, err := url.Parse(path)
	if err != nil {
		return
	}
	bucket = uri.Host
	prefix = uri.Path
	return
}
