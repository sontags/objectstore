# objectstore

... is a very simplistic library to interact with _objects_ (as in _object storage_) on various backends.

These backends are:

## AWS S3 Object

Ensure credentials are provided via one of the [common ways](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-envvars.html).
With this in place just specify a path that matches a pattern like `s3://[bucktname]/[objectname]`:

```golang
# preparing the store
o, err := objectstore.New("s3://example-bucket/path/to/object")
if err != nil {
  return err
}

# writing data
err = o.Write([]byte("Hello, World!"))
if err != nil {
  return err
}

# reading data
data, err := o.Read()
if err != nil {
  return err
}
fmt.Println(string(data))


# deleting the object
err = o.Delete()
data, err := o.Read()
if err != nil {
  return err
}

```

## Azure Blob

Ensure that the _key_ and _account name_ are provided via environment variables:

```
export AZURE_STORAGE_ACCOUNT_NAME="<your_account_name>"
export AZURE_STORAGE_ACCOUNT_KEY="<your_secret_key>"
```

With this in place just specify a path that matches a pattern like `blob://[storage_account]/[container]/[objectname]`:


```golang
# preparing the store
o, err := objectstore.New("blob://stexample/container/path/to/blob")
```

## Local File 

Make sure that the directory you want to write to exists and is writable.

```golang
# preparing the store
o, err := objectstore.New("/tmp")
```
