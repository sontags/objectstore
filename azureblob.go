package objectstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/streaming"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
)

type azureBlob struct {
	basepath string
}

func newAzureBlob(basepath string) *azureBlob {
	return &azureBlob{basepath: basepath}
}

func (ab *azureBlob) Read(name string) ([]byte, error) {
	account, container, prefix, err := parseBlobPath(ab.basepath)
	if err != nil {
		return []byte{}, err
	}
	object := joinPath(prefix, name)
	client, err := getBlobClient(account)
	if err != nil {
		return []byte{}, err
	}
	ctx := context.Background()
	reader, err := client.DownloadStream(ctx, container, object, nil)
	if err != nil {
		return []byte{}, err
	}
	rs := reader.Body
	stream := streaming.NewResponseProgress(
		rs,
		func(bytesTransferred int64) {},
	)
	defer func(stream io.ReadCloser) {
		err := stream.Close()
		if err != nil {
			log.Fatal(err)
		}
	}(stream)
	return io.ReadAll(stream)
}

func (ab *azureBlob) Write(name string, data []byte) error {
	account, container, prefix, err := parseBlobPath(ab.basepath)
	if err != nil {
		return err
	}
	object := joinPath(prefix, name)
	client, err := getBlobClient(account)
	if err != nil {
		return err
	}
	ctx := context.Background()
	_, err = client.UploadBuffer(ctx, container, object, data, nil)
	return err
}

func (ab *azureBlob) Delete(name string) error {
	account, container, prefix, err := parseBlobPath(ab.basepath)
	if err != nil {
		return err
	}
	object := joinPath(prefix, name)
	client, err := getBlobClient(account)
	if err != nil {
		return err
	}
	ctx := context.Background()
	_, err = client.DeleteBlob(ctx, container, object, nil)
	return err
}

func (ab *azureBlob) List() ([]string, error) {
	files := []string{}
	account, container, prefix, err := parseBlobPath(ab.basepath)
	if err != nil {
		return files, err
	}
	client, err := getBlobClient(account)
	if err != nil {
		return files, err
	}
	ctx := context.Background()
	var nextMarker *string
	for {
		options := &azblob.ListBlobsFlatOptions{
			Prefix: &prefix,
			Marker: nextMarker,
		}
		p := client.NewListBlobsFlatPager(container, options)
		data, err := p.NextPage(ctx)
		if err != nil {
			return files, err
		}
		for _, item := range data.Segment.BlobItems {
			files = append(files, *item.Name)
		}
		if data.NextMarker != nil && *data.NextMarker != "" {
			nextMarker = data.NextMarker
		} else {
			break
		}
	}
	return files, err
}

func getBlobClient(account string) (*azblob.Client, error) {
	accountKey, ok := os.LookupEnv("AZURE_STORAGE_ACCOUNT_KEY")
	if !ok {
		return nil, errors.New("AZURE_STORAGE_ACCOUNT_KEY could not be found")
	}
	credential, err := azblob.NewSharedKeyCredential(account, accountKey)
	if err != nil {
		return nil, err
	}
	accountPath := fmt.Sprintf("https://%s.blob.core.windows.net/", account)
	client, err := azblob.NewClientWithSharedKeyCredential(accountPath, credential, nil)
	if err != nil {
		return nil, err
	}
	return client, nil
}

func parseBlobPath(path string) (account, container, prefix string, err error) {
	uri, err := url.Parse(path)
	if err != nil {
		return
	}
	fragments := strings.SplitN(strings.TrimPrefix(uri.Path, "/"), "/", 2)
	if len(fragments) > 1 {
		prefix = fragments[1]
	}
	account = uri.Host
	container = strings.Trim(fragments[0], "/")
	return
}
