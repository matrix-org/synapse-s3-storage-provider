from setuptools import setup

__version__ = "1.5.0"

with open("README.md") as f:
    long_description = f.read()

setup(
    name="synapse-s3-storage-provider",
    version=__version__,
    zip_safe=False,
    author="matrix.org team and contributors",
    description="A storage provider which can fetch and store media in Amazon S3.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/matrix-org/synapse-s3-storage-provider",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License",
    ],
    py_modules=["s3_storage_provider"],
    scripts=["scripts/s3_media_upload"],
    install_requires=[
        "boto3>=1.9.23,<2.0",
        "botocore>=1.31.62,<2.0",
        "humanize>=4.0,<5.0",
        "psycopg2>=2.7.5,<3.0",
        "PyYAML>=5.4,<7.0",
        "tqdm>=4.26.0,<5.0",
        "Twisted",
    ],
)
