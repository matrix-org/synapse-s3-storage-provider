from setuptools import setup

__version__ = "1.0"

setup(
    name='synapse-s3-storage-provider',
    version=__version__,
    zip_safe=False,
    url="https://github.com/matrix-org/synapse-s3-storage-provider",
    py_modules=['s3_storage_provider'],
    install_requires=[
        'boto3'
    ],
)
