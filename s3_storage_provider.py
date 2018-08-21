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


from twisted.internet import defer, reactor
from twisted.python.failure import Failure

from synapse.rest.media.v1.storage_provider import StorageProvider
from synapse.rest.media.v1._base import Responder
from synapse.util.logcontext import make_deferred_yieldable

import boto3
import botocore
import logging
import threading
import os


logger = logging.getLogger("synapse.s3")


# The list of valid AWS storage class names
_VALID_STORAGE_CLASSES = ('STANDARD', 'REDUCED_REDUNDANCY', 'STANDARD_IA')


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
        if "endpoint_url" in config:
            self.api_kwargs["endpoint_url"] = config["endpoint_url"]

    def store_file(self, path, file_info):
        """See StorageProvider.store_file"""

        def _store_file():
            boto3.resource('s3', **self.api_kwargs).Bucket(self.bucket).upload_file(
                Filename=os.path.join(self.cache_directory, path),
                Key=path,
                ExtraArgs={"StorageClass": self.storage_class},
            )

        return make_deferred_yieldable(
            reactor.callInThread(_store_file)
        )

    def fetch(self, path, file_info):
        """See StorageProvider.fetch"""
        d = defer.Deferred()
        _S3DownloadThread(self.bucket, self.api_kwargs, path, d).start()
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

        assert isinstance(bucket, basestring)
        assert storage_class in _VALID_STORAGE_CLASSES

        result = {
            "bucket": bucket,
            "storage_class": storage_class,
        }

        if "endpoint_url" in config:
            result["endpoint_url"] = config["endpoint_url"]

        return result


class _S3DownloadThread(threading.Thread):
    """Attempts to download a file from S3.

    Args:
        bucket (str): The S3 bucket which may have the file
        api_kwargs (dict): Keyword arguments to pass when invoking the API. Generally `endpoint_url`.
        key (str): The key of the file
        deferred (Deferred[_S3Responder|None]): If file exists
            resolved with an _S3Responder instance, if it doesn't
            exist then resolves with None.

    Attributes:
        READ_CHUNK_SIZE (int): The chunk size in bytes used when downloading
            file.
    """

    READ_CHUNK_SIZE = 16 * 1024

    def __init__(self, bucket, api_kwargs, key, deferred):
        super(_S3DownloadThread, self).__init__(name="s3-download")
        self.bucket = bucket
        self.api_kwargs = api_kwargs
        self.key = key
        self.deferred = deferred

    def run(self):
        session = boto3.session.Session()
        s3 = session.client('s3', **self.api_kwargs)

        try:
            resp = s3.get_object(Bucket=self.bucket, Key=self.key)
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] in ("404", "NoSuchKey",):
                reactor.callFromThread(self.deferred.callback, None)
                return

            reactor.callFromThread(self.deferred.errback, Failure())
            return

        # Triggered by responder when more data has been requested (or
        # stop_event has been triggered)
        wakeup_event = threading.Event()
        # Trigered by responder when we should abort the download.
        stop_event = threading.Event()

        producer = _S3Responder(wakeup_event, stop_event)
        reactor.callFromThread(self.deferred.callback, producer)

        try:
            body = resp["Body"]

            while not stop_event.is_set():
                # We wait for the producer to signal that the consumer wants
                # more data (or we should abort)
                wakeup_event.wait()

                # Check if we were woken up so that we abort the download
                if stop_event.is_set():
                    return

                chunk = body.read(self.READ_CHUNK_SIZE)
                if not chunk:
                    return

                # We clear the wakeup_event flag just before we write the data
                # to producer.
                wakeup_event.clear()
                reactor.callFromThread(producer._write, chunk)

        except Exception:
            reactor.callFromThread(producer._error, Failure())
            return
        finally:
            reactor.callFromThread(producer._finish)
            if body:
                body.close()


class _S3Responder(Responder):
    """A Responder for S3. Created by _S3DownloadThread

    Args:
        wakeup_event (threading.Event): Used to signal to _S3DownloadThread
            that consumer is ready for more data (or that we've triggered
            stop_event).
        stop_event (threading.Event): Used to signal to _S3DownloadThread that
            it should stop producing. `wakeup_event` must also be set if
            `stop_event` is used.
    """
    def __init__(self, wakeup_event, stop_event):
        self.wakeup_event = wakeup_event
        self.stop_event = stop_event

        # The consumer we're registered to
        self.consumer = None

        # The deferred returned by write_to_consumer, which should resolve when
        # all the data has been written (or there has been a fatal error).
        self.deferred = defer.Deferred()

    def write_to_consumer(self, consumer):
        """See Responder.write_to_consumer
        """
        self.consumer = consumer
        # We are a IPullProducer, so we expect consumer to call resumeProducing
        # each time they want a new chunk of data.
        consumer.registerProducer(self, False)
        return self.deferred

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop_event.set()
        self.wakeup_event.set()

    def resumeProducing(self):
        """See IPullProducer.resumeProducing
        """
        # The consumer is asking for more data, signal _S3DownloadThread
        self.wakeup_event.set()

    def stopProducing(self):
        """See IPullProducer.stopProducing
        """
        # The consumer wants no more data ever, signal _S3DownloadThread
        self.stop_event.set()
        self.wakeup_event.set()
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
