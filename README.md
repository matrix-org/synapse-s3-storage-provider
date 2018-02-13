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
```

This module uses `boto3`, and so the credentials should be specified as
described [here](https://boto3.readthedocs.io/en/latest/guide/configuration.html#guide-configuration).

