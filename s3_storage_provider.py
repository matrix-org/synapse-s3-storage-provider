# -*- coding: utf-8 -*-
# Copyright 2018 New Vector Ltd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import os
import threading

from six import string_types

import boto3
import botocore

from twisted.internet import defer, reactor
from twisted.python.failure import Failure
from twisted.python.threadpool import ThreadPool

from synapse.logging.context import LoggingContext, make_deferred_yieldable
from synapse.rest.media.v1._base import Responder
from synapse.rest.media.v1.storage_provider import StorageProvider

# Synapse 1.13.0 moved current_context to a module-level function.
try:
    from synapse.logging.context import current_context
except ImportError:
    current_context = LoggingContext.current_context

logger = logging.getLogger("synapse.s3")


# The list of valid AWS storage class names
_VALID_STORAGE_CLASSES = (
    "STANDARD",
    "REDUCED_REDUNDANCY",
    "STANDARD_IA",
    "INTELLIGENT_TIERING",
)

# Chunk size to use when reading from s3 connection in bytes
READ_CHUNK_SIZE = 16 * 1024


class S3StorageProviderBackend(StorageProvider):
    """
    Args:
        hs (HomeServer)
        config: The config returned by `parse_config`
    """

    def __init__(self, hs, config):
        self.cache_directory = hs.config.media_store_path
        self.bucket = config["bucket"]
        self.storage_class = config["storage_class"]
        self.api_kwargs = {}

        if "region_name" in config:
            self.api_kwargs["region_name"] = config["region_name"]

        if "endpoint_url" in config:
            self.api_kwargs["endpoint_url"] = config["endpoint_url"]

        if "access_key_id" in config:
            self.api_kwargs["aws_access_key_id"] = config["access_key_id"]

        if "secret_access_key" in config:
            self.api_kwargs["aws_secret_access_key"] = config["secret_access_key"]

        threadpool_size = config.get("threadpool_size", 40)
        self._download_pool = ThreadPool(
            name="s3-download-pool", maxthreads=threadpool_size
        )
        self._download_pool.start()

    def store_file(self, path, file_info):
        """See StorageProvider.store_file"""

        def _store_file():
            session = boto3.session.Session()
            session.resource("s3", **self.api_kwargs).Bucket(self.bucket).upload_file(
                Filename=os.path.join(self.cache_directory, path),
                Key=path,
                ExtraArgs={"StorageClass": self.storage_class},
            )

        # XXX: reactor.callInThread doesn't return anything, so I don't think this does
        # what the author intended.
        return make_deferred_yieldable(reactor.callInThread(_store_file))

    def fetch(self, path, file_info):
        """See StorageProvider.fetch"""
        logcontext = current_context()

        d = defer.Deferred()
        self._download_pool.callInThread(
            s3_download_task, self.bucket, self.api_kwargs, path, d, logcontext
        )
        return make_deferred_yieldable(d)

    @staticmethod
    def parse_config(config):
        """Called on startup to parse config supplied. This should parse
        the config and raise if there is a problem.

        The returned value is passed into the constructor.

        In this case we return a dict with fields, `bucket` and `storage_class`
        """
        bucket = config["bucket"]
        storage_class = config.get("storage_class", "STANDARD")

        assert isinstance(bucket, string_types)
        assert storage_class in _VALID_STORAGE_CLASSES

        result = {
            "bucket": bucket,
            "storage_class": storage_class,
        }

        if "region_name" in config:
            result["region_name"] = config["region_name"]

        if "endpoint_url" in config:
            result["endpoint_url"] = config["endpoint_url"]

        if "access_key_id" in config:
            result["access_key_id"] = config["access_key_id"]

        if "secret_access_key" in config:
            result["secret_access_key"] = config["secret_access_key"]

        return result


def s3_download_task(bucket, api_kwargs, key, deferred, parent_logcontext):
    """Attempts to download a file from S3.

    Args:
        bucket (str): The S3 bucket which may have the file
        api_kwargs (dict): Keyword arguments to pass when invoking the API.
            Generally `endpoint_url`.
        key (str): The key of the file
        deferred (Deferred[_S3Responder|None]): If file exists
            resolved with an _S3Responder instance, if it doesn't
            exist then resolves with None.
        parent_logcontext (LoggingContext): the logcontext to report logs and metrics
            against.
    """
    with LoggingContext(parent_context=parent_logcontext):
        logger.info("Fetching %s from S3", key)

        local_data = threading.local()

        try:
            s3 = local_data.b3_client
        except AttributeError:
            b3_session = boto3.session.Session()
            local_data.b3_client = s3 = b3_session.client("s3", **api_kwargs)

        try:
            resp = s3.get_object(Bucket=bucket, Key=key)
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] in ("404", "NoSuchKey",):
                logger.info("Media %s not found in S3", key)
                reactor.callFromThread(deferred.callback, None)
                return

            reactor.callFromThread(deferred.errback, Failure())
            return

        producer = _S3Responder()
        reactor.callFromThread(deferred.callback, producer)
        _stream_to_producer(reactor, producer, resp["Body"], timeout=90.0)


def _stream_to_producer(reactor, producer, body, status=None, timeout=None):
    """Streams a file like object to the producer.

    Correctly handles producer being paused/resumed/stopped.

    Args:
        reactor
        producer (_S3Responder): Producer object to stream results to
        body (file like): The object to read from
        status (_ProducerStatus|None): Used to track whether we're currently
            paused or not. Used for testing
        timeout (float|None): Timeout in seconds to wait for consume to resume
            after being paused
    """

    # Set when we should be producing, cleared when we are paused
    wakeup_event = producer.wakeup_event

    # Set if we should stop producing forever
    stop_event = producer.stop_event

    if not status:
        status = _ProducerStatus()

    try:
        while not stop_event.is_set():
            # We wait for the producer to signal that the consumer wants
            # more data (or we should abort)
            if not wakeup_event.is_set():
                status.set_paused(True)
                ret = wakeup_event.wait(timeout)
                if not ret:
                    raise Exception("Timed out waiting to resume")
                status.set_paused(False)

            # Check if we were woken up so that we abort the download
            if stop_event.is_set():
                return

            chunk = body.read(READ_CHUNK_SIZE)
            if not chunk:
                return

            reactor.callFromThread(producer._write, chunk)

    except Exception:
        reactor.callFromThread(producer._error, Failure())
    finally:
        reactor.callFromThread(producer._finish)
        if body:
            body.close()


class _S3Responder(Responder):
    """A Responder for S3. Created by _S3DownloadThread
    """

    def __init__(self):
        # Triggered by responder when more data has been requested (or
        # stop_event has been triggered)
        self.wakeup_event = threading.Event()
        # Trigered by responder when we should abort the download.
        self.stop_event = threading.Event()

        # The consumer we're registered to
        self.consumer = None

        # The deferred returned by write_to_consumer, which should resolve when
        # all the data has been written (or there has been a fatal error).
        self.deferred = defer.Deferred()

    def write_to_consumer(self, consumer):
        """See Responder.write_to_consumer
        """
        self.consumer = consumer
        # We are a IPushProducer, so we start producing immediately until we
        # get a pauseProducing or stopProducing
        consumer.registerProducer(self, True)
        self.wakeup_event.set()
        return make_deferred_yieldable(self.deferred)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop_event.set()
        self.wakeup_event.set()

    def resumeProducing(self):
        """See IPushProducer.resumeProducing
        """
        # The consumer is asking for more data, signal _S3DownloadThread
        self.wakeup_event.set()

    def pauseProducing(self):
        """See IPushProducer.stopProducing
        """
        self.wakeup_event.clear()

    def stopProducing(self):
        """See IPushProducer.stopProducing
        """
        # The consumer wants no more data ever, signal _S3DownloadThread
        self.stop_event.set()
        self.wakeup_event.set()
        if not self.deferred.called:
            with LoggingContext():
                self.deferred.errback(Exception("Consumer ask to stop producing"))

    def _write(self, chunk):
        """Writes the chunk of data to consumer. Called by _S3DownloadThread.
        """
        if self.consumer and not self.stop_event.is_set():
            self.consumer.write(chunk)

    def _error(self, failure):
        """Called when a fatal error occured while getting data. Called by
        _S3DownloadThread.
        """
        if self.consumer:
            self.consumer.unregisterProducer()
            self.consumer = None

        if not self.deferred.called:
            self.deferred.errback(failure)

    def _finish(self):
        """Called when there is no more data to write. Called by _S3DownloadThread.
        """
        if self.consumer:
            self.consumer.unregisterProducer()
            self.consumer = None

        if not self.deferred.called:
            self.deferred.callback(None)


class _ProducerStatus(object):
    """Used to track whether the s3 download thread is currently paused
    waiting for consumer to resume. Used for testing.
    """

    def __init__(self):
        self.is_paused = threading.Event()
        self.is_paused.clear()

    def wait_until_paused(self, timeout=None):
        is_paused = self.is_paused.wait(timeout)
        if not is_paused:
            raise Exception("Timed out waiting")

    def set_paused(self, paused):
        if paused:
            self.is_paused.set()
        else:
            self.is_paused.clear()
