Synapse S3 Storage Provider
===========================

This module can be used by synapse as a storage provider, allowing it to fetch
and store media in Amazon S3.


Usage
-----

The `s3_storage_provider.py` should be on the PYTHONPATH when starting
synapse.

Example of entry in synapse config:

```yaml
media_storage_providers:
- module: s3_storage_provider.S3StorageProviderBackend
  store_local: True
  store_remote: True
  store_synchronous: True
  config:
    bucket: <S3_BUCKET_NAME>
    # All of the below options are optional, for use with non-AWS S3-like
    # services, or to specify access tokens here instead of some external method.
    region_name: <S3_REGION_NAME>
    endpoint_url: <S3_LIKE_SERVICE_ENDPOINT_URL>
    access_key_id: <S3_ACCESS_KEY_ID>
    secret_access_key: <S3_SECRET_ACCESS_KEY>

    # Server Side Encryption for Customer-provided keys
    #sse_customer_key: <S3_SSEC_KEY>
    # Your SSE-C algorithm is very likely AES256
    # Default is AES256.
    #sse_customer_algo: <S3_SSEC_ALGO>

    # The object storage class used when uploading files to the bucket.
    # Default is STANDARD.
    #storage_class: "STANDARD_IA"

    # Prefix for all media in bucket, can't be changed once media has been uploaded
    # Useful if sharing the bucket between Synapses
    # Blank if not provided
    #prefix: "prefix/to/files/in/bucket"

    # The maximum number of concurrent threads which will be used to connect
    # to S3. Each thread manages a single connection. Default is 40.
    #
    #threadpool_size: 20
```

This module uses `boto3`, and so the credentials should be specified as
described [here](https://boto3.readthedocs.io/en/latest/guide/configuration.html#guide-configuration).

Regular cleanup job
-------------------

There is additionally a script at `scripts/s3_media_upload` which can be used
in a regular job to upload content to s3, then delete that from local disk.
This script can be used in combination with configuration for the storage
provider to pull media from s3, but upload it asynchronously.

Once the package is installed, the script should be run somewhat like the
following. We suggest using `tmux` or `screen` as these can take a long time
on larger servers.

`database.yaml` should contain the keys that would be passed to psycopg2 to
connect to your database. They can be found in the contents of the
`database`.`args` parameter in your homeserver.yaml.

More options are available in the command help.

```
> cd s3_media_upload
# cache.db will be created if absent. database.yaml is required to
# contain PG credentials
> ls
cache.db database.yaml
# Update cache from /path/to/media/store looking for files not used
# within 2 months
> s3_media_upload update /path/to/media/store 2m
Syncing files that haven't been accessed since: 2018-10-18 11:06:21.520602
Synced 0 new rows
100%|█████████████████████████████████████████████████████████████| 1074/1074 [00:33<00:00, 25.97files/s]
Updated 0 as deleted

> s3_media_upload upload /path/to/media/store matrix_s3_bucket_name --storage-class STANDARD_IA --delete
# prepare to wait a long time
```

Packaging and release
---------

For maintainers:

1. Update the `__version__` in setup.py. Commit. Push.
2. Create a release on GitHub for this version.
3. When published, a [GitHub action workflow](https://github.com/matrix-org/synapse-s3-storage-provider/actions/workflows/release.yml) will build the package and upload to [PyPI](https://pypi.org/project/synapse-s3-storage-provider/).
