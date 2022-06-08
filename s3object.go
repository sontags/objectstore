package objectstore

import (
	"bytes"
	"net/http"
	"net/url"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

type s3Object struct {
	path string
}

func newS3Object(path string) *s3Object {
	return &s3Object{path: path}
}

func (s3o *s3Object) Read() ([]byte, error) {
	bucket, object, err := parseS3Path(s3o.path)
	if err != nil {
		return []byte{}, err
	}

	s3svc := s3.New(session.New())
	input := &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(object),
	}

	result, err := s3svc.GetObject(input)
	if err != nil {
		return []byte{}, err
	}

	buf := new(bytes.Buffer)
	buf.ReadFrom(result.Body)

	data := []byte{}
	file := buf.Bytes()
	if len(file) > 0 {
		data = append(data, file...)
	}
	return data, nil
}

func (s3o *s3Object) Write(data []byte) error {
	bucket, object, err := parseS3Path(s3o.path)
	if err != nil {
		return err
	}

	s3svc := s3.New(session.New())
	input := &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(object),
		ACL:    aws.String("private"),
		Body:   bytes.NewReader(data),
		//ContentLength: aws.Int64(len(data)),
		ContentType: aws.String(http.DetectContentType(data)),
	}

	_, err = s3svc.PutObject(input)
	if err != nil {
		return err
	}
	return nil
}

func (s3o *s3Object) Delete() error {
	bucket, object, err := parseS3Path(s3o.path)
	if err != nil {
		return err
	}
	s3svc := s3.New(session.New())
	input := &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(object),
	}

	_, err = s3svc.DeleteObject(input)
	if err != nil {
		return err
	}
	return nil
}

func parseS3Path(path string) (bucket, object string, err error) {
	uri, err := url.Parse(path)
	if err != nil {
		return
	}
	bucket = uri.Host
	object = uri.Path
	return
}
