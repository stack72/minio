package cmd

import (
	"context"
	"io"
	"net/http"

	"bytes"
	"fmt"

	"github.com/davecgh/go-spew/spew"
	triton "github.com/joyent/triton-go"
	"github.com/joyent/triton-go/authentication"
	"github.com/joyent/triton-go/storage"
	"github.com/minio/minio-go/pkg/policy"
	"io/ioutil"
	"path"
)

const DefaultMantaURL = "https://us-east.manta.joyent.com"
const MantaDefaultRootStore = "/stor"

// tritonObjects - Implements Object layer for Triton Manta storage
type tritonObjects struct {
	client *storage.StorageClient
}

func newTritonGateway(host string) (GatewayLayer, error) {
	var err error
	var endpoint = DefaultMantaURL

	if host != "" {
		endpoint, _, err = parseGatewayEndpoint(host)
		if err != nil {
			return nil, err
		}
	}

	creds := serverConfig.GetCredential()
	signer, err := authentication.NewSSHAgentSigner(creds.SecretKey, creds.AccessKey)
	if err != nil {
		return nil, err
	}

	config := &triton.ClientConfig{
		MantaURL:    endpoint,
		AccountName: creds.AccessKey,
		Signers:     []authentication.Signer{signer},
	}
	triton, err := storage.NewClient(config)
	if err != nil {
		return nil, err
	}
	triton.Client.HTTPClient = &http.Client{Transport: newCustomHTTPTransport()}

	gateway := &tritonObjects{
		client: triton,
	}

	return gateway, nil
}

// Shutdown - save any gateway metadata to disk
// if necessary and reload upon next restart.
func (t *tritonObjects) Shutdown() error {
	return nil
}

// StorageInfo - Not relevant to Triton backend.
func (t *tritonObjects) StorageInfo() (si StorageInfo) {
	return si
}

//
// ~~~ Buckets ~~~
//

// MakeBucketWithLocation - Create a new directory within manta.
//
// https://apidocs.joyent.com/manta/api.html#PutDirectory
func (t *tritonObjects) MakeBucketWithLocation(bucket, location string) error {
	log.Printf("Calling MakeBucketWithLocation with params: %q: %q\n", bucket, location)
	ctx := context.Background()
	err := t.client.Dir().Put(ctx, &storage.PutDirectoryInput{
		DirectoryName: path.Join(MantaDefaultRootStore, bucket),
	})
	if err != nil {
		return err
	}
	return nil
}

// GetBucketInfo - Get directory metadata..
//
// https://apidocs.joyent.com/manta/api.html#GetObject
func (t *tritonObjects) GetBucketInfo(bucket string) (bi BucketInfo, e error) {
	var info BucketInfo
	ctx := context.Background()
	resp, err := t.client.Objects().Get(ctx, &storage.GetObjectInput{
		ObjectPath: path.Join(MantaDefaultRootStore, bucket),
	})
	if err != nil {
		return info, err
	}

	info = BucketInfo{
		Name:    bucket,
		Created: resp.LastModified,
	}

	return info, nil
}

// ListBuckets - Lists all Manta directories, uses Manta equivalent
// ListDirectories.
//
// https://apidocs.joyent.com/manta/api.html#ListDirectory
func (t *tritonObjects) ListBuckets() (buckets []BucketInfo, err error) {
	ctx := context.Background()
	dirs, err := t.client.Dir().List(ctx, &storage.ListDirectoryInput{
		DirectoryName: path.Join(MantaDefaultRootStore),
	})
	if err != nil {
		return nil, err
	}

	bucketList := make([]BucketInfo, 0)
	for _, dir := range dirs.Entries {
		if dir.Type == "directory" {
			bucket := BucketInfo{
				Name:    dir.Name,
				Created: dir.ModifiedTime,
			}

			bucketList = append(bucketList, bucket)
		}
	}

	return bucketList, nil
}

// DeleteBucket - Delete a directory in Manta, uses Manta equivalent
// DeleteDirectory.
//
// https://apidocs.joyent.com/manta/api.html#DeleteDirectory
func (t *tritonObjects) DeleteBucket(bucket string) error {
	ctx := context.Background()
	return t.client.Dir().Delete(ctx, &storage.DeleteDirectoryInput{
		DirectoryName: path.Join(MantaDefaultRootStore, bucket),
	})
}

//
// ~~~ Objects ~~~
//

// ListObjects - Lists all objects in Manta with a container filtered by prefix
// and marker, uses Manta equivalent ListDirectory.
//
// https://apidocs.joyent.com/manta/api.html#ListDirectory
func (t *tritonObjects) ListObjects(bucket, prefix, marker, delimiter string, maxKeys int) (result ListObjectsInfo, err error) {
	ctx := context.Background()
	objs, err := t.client.Dir().List(ctx, &storage.ListDirectoryInput{
		DirectoryName: path.Join(MantaDefaultRootStore, bucket, prefix),
	})
	if err != nil {
		return result, err
	}

	blObjects := make([]ObjectInfo, 0)
	blPrefixes := make([]string, 0)

	for _, obj := range objs.Entries {

		if obj.Type == "directory" {
			blPrefixes = append(blPrefixes, fmt.Sprintf("%s/", obj.Name))
		} else {
			var bucketInfo ObjectInfo
			bucketInfo.Name = obj.Name

			pathway := path.Join(bucket, prefix)
			objInfo, err := t.GetObjectInfo(pathway, obj.Name)
			if err != nil {
				return result, err
			}

			bucketInfo.ContentType = objInfo.ContentType
			bucketInfo.ETag = objInfo.ETag
			bucketInfo.ContentEncoding = objInfo.ContentEncoding
			bucketInfo.Size = objInfo.Size
			bucketInfo.ModTime = objInfo.ModTime

			blObjects = append(blObjects, bucketInfo)
		}
	}

	return ListObjectsInfo{
		Prefixes: blPrefixes,
		Objects:  blObjects,
	}, nil
}

// GetObject - Reads an object from Manta. Supports additional parameters like
// offset and length which are synonymous with HTTP Range requests.
//
// startOffset indicates the starting read location of the object.  length
// indicates the total length of the object.
//
// https://apidocs.joyent.com/manta/api.html#GetObject
func (t *tritonObjects) GetObject(bucket, object string, startOffset int64, length int64, writer io.Writer) error {

	ctx := context.Background()
	output, err := t.client.Objects().Get(ctx, &storage.GetObjectInput{
		ObjectPath: path.Join(MantaDefaultRootStore, bucket, object),
	})
	if err != nil {
		return err
	}

	_, err = io.Copy(writer, output.ObjectReader)
	defer output.ObjectReader.Close()
	return nil
}

// GetObjectInfo - reads blob metadata properties and replies back ObjectInfo,
// uses Triton equivalent GetBlobProperties.
//
// https://apidocs.joyent.com/manta/api.html#GetObject
func (t *tritonObjects) GetObjectInfo(bucket, object string) (objInfo ObjectInfo, err error) {
	var info ObjectInfo
	ctx := context.Background()
	obj, err := t.client.Objects().Get(ctx, &storage.GetObjectInput{
		ObjectPath: path.Join(MantaDefaultRootStore, bucket, object),
	})
	if err != nil {
		return info, err
	}

	info = ObjectInfo{
		Bucket:      bucket,
		ContentType: obj.ContentType,
		Size:        int64(obj.ContentLength),
		ModTime:     obj.LastModified,
	}

	return info, nil
}

// PutObject - Create a new blob with the incoming data, uses Triton equivalent
// CreateBlockBlobFromReader.
//
// https://apidocs.joyent.com/manta/api.html#PutObject
func (t *tritonObjects) PutObject(bucket, object string, size int64, data io.Reader, metadata map[string]string, sha256sum string) (objInfo ObjectInfo, err error) {
	ctx := context.Background()
	var oInfo ObjectInfo

	b, err := ioutil.ReadAll(data)
	if err != nil {
		return oInfo, err
	}
	r := bytes.NewReader(b)
	r.Seek(0, 0)

	err = t.client.Objects().Put(ctx, &storage.PutObjectInput{
		ContentLength: uint64(size),
		ObjectPath:    path.Join(MantaDefaultRootStore, bucket, object),
		ContentType:   metadata["content-type"],
		ObjectReader:  r,
	})
	if err != nil {
		return oInfo, err
	}
	return objInfo, nil
}

// CopyObject - Copies a blob from source container to destination container.
// Uses Azure equivalent CopyBlob API.
func (a *tritonObjects) CopyObject(srcBucket, srcObject, destBucket, destObject string, metadata map[string]string) (objInfo ObjectInfo, err error) {

	log.Printf("Copying from %s to %s\n", path.Join(srcBucket, srcObject), path.Join(destBucket, destObject))
	log.Printf("Metadata found :%s", spew.Sdump(metadata))

	return objInfo, nil
}

// DeleteObject - Delete a blob in Manta, uses Triton equivalent DeleteBlob API.
//
// https://apidocs.joyent.com/manta/api.html#DeleteObject
func (t *tritonObjects) DeleteObject(bucket, object string) error {

	ctx := context.Background()

	err := t.client.Objects().Delete(ctx, &storage.DeleteObjectInput{
		ObjectPath: path.Join(MantaDefaultRootStore, bucket, object),
	})
	if err != nil {
		return err
	}

	return nil
}

//
// ~~~ Bucket Policy ~~~
//
func (a *tritonObjects) SetBucketPolicies(bucket string, policyInfo policy.BucketAccessPolicy) error {
	return nil
}

func (a *tritonObjects) GetBucketPolicies(bucket string) (policy.BucketAccessPolicy, error) {
	return policy.BucketAccessPolicy{}, nil
}

// DeleteBucketPolicies - Set the container ACL to "private"
func (a *tritonObjects) DeleteBucketPolicies(bucket string) error {
	return nil
}

//
//
//
