from setuptools import setup

__version__ = "1.0"

setup(
    name="synapse-s3-storage-provider",
    version=__version__,
    zip_safe=False,
    url="https://github.com/matrix-org/synapse-s3-storage-provider",
    py_modules=["s3_storage_provider"],
    scripts=["scripts/s3_media_upload"],
    install_requires=[
        "boto3>=1.9.23<2.0",
        "botocore>=1.12.23<2.0",
        "humanize>=0.5.1<0.6",
        "psycopg2>=2.7.5<3.0",
        "PyYAML>=3.13<4.0",
        "tqdm>=4.26.0<5.0",
        "Twisted",
    ],
)
