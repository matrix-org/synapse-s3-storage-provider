# -*- coding: utf-8 -*-
# Copyright 2018 New Vector Ltd
# Copyright 2021 The Matrix.org Foundation C.I.C.
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
import os
import threading
import re
from typing import Dict, Any, Optional
from twisted.internet import reactor, defer, threads
from twisted.python.failure import Failure
from multiprocessing.pool import ThreadPool

import boto3
import logging

from synapse.logging.context import LoggingContext, make_deferred_yieldable
from synapse.rest.media.v1._base import Responder
from synapse.rest.media.v1.storage_provider import StorageProvider

# Try to import current_context from the new location, fall back to the old one
try:
    from synapse.logging.context import current_context
except ImportError:
    current_context = LoggingContext.current_context

logger = logging.getLogger("synapse.s3")

READ_CHUNK_SIZE = 16 * 1024

def parse_duration(duration_str: str) -> Optional[int]:
    """Parse a duration string into milliseconds.
    
    Supports formats:
    - Nd: N days
    - Nh: N hours
    - Nm: N minutes
    - Ns: N seconds
    - N: plain milliseconds
    
    Returns milliseconds or None if invalid
    """
    if duration_str is None:
        return None
        
    if isinstance(duration_str, (int, float)):
        return int(duration_str)
        
    duration_str = str(duration_str).strip().lower()
    if not duration_str:
        return None
        
    # Try to parse as plain milliseconds first
    try:
        return int(duration_str)
    except ValueError:
        pass
        
    # Parse duration with unit
    match = re.match(r'^(\d+)(d|h|m|s|min)$', duration_str)
    if not match:
        logger.warning("Invalid duration format: %s", duration_str)
        return None
        
    value, unit = match.groups()
    value = int(value)
    
    # Convert to milliseconds
    if unit == 'd':
        return value * 24 * 60 * 60 * 1000
    elif unit == 'h':
        return value * 60 * 60 * 1000
    elif unit == 'm' or unit == 'min':
        return value * 60 * 1000
    elif unit == 's':
        return value * 1000
    
    return None

class S3StorageProviderBackend(StorageProvider):
    """Storage provider for S3 storage.

    Args:
        hs (HomeServer): Homeserver instance
        config: The config dict from the homeserver config
    """

    def __init__(self, hs, config):
        StorageProvider.__init__(self)
        self.hs = hs

        # Get paths from Synapse config
        self.media_store_path = hs.config.media.media_store_path
        try:
            self.uploads_path = hs.config.media.uploads_path
        except AttributeError:
            self.uploads_path = os.path.join(self.media_store_path, "uploads")
            logger.info("uploads_path not found in config, using: %s", self.uploads_path)

        self.cache_directory = self.media_store_path

        # Initialize storage settings with null values
        self.store_local = None
        self.store_remote = None
        self.store_synchronous = None
        self.local_media_lifetime = None
        self.remote_media_lifetime = None

        # Parse boolean values from config
        def parse_bool(value):
            if value is None:
                return None
            if isinstance(value, bool):
                return value
            if isinstance(value, str):
                return value.lower() in ("true", "yes", "1", "on")
            return bool(value)

        # Get settings directly from config
        self.store_local = parse_bool(config.get("store_local"))
        self.store_remote = parse_bool(config.get("store_remote"))
        self.store_synchronous = parse_bool(config.get("store_synchronous"))

        # Get media retention settings with duration parsing
        media_retention = config.get("media_retention", {})
        if isinstance(media_retention, dict):
            self.local_media_lifetime = parse_duration(media_retention.get("local_media_lifetime"))
            self.remote_media_lifetime = parse_duration(media_retention.get("remote_media_lifetime"))
        else:
            logger.warning("media_retention config is not a dictionary: %s", media_retention)

        logger.info(
            "S3 Storage Provider initialized with store_local=%s, store_remote=%s, store_synchronous=%s",
            self.store_local,
            self.store_remote,
            self.store_synchronous,
        )
        logger.info(
            "Media retention settings: local=%s, remote=%s milliseconds",
            self.local_media_lifetime,
            self.remote_media_lifetime,
        )

        # S3 configuration
        s3_config = config.get("config", {})
        if not s3_config:
            logger.warning("No config block found, falling back to root-level settings")
            self.bucket = config.get("bucket")
            self.api_kwargs = {
                "region_name": config.get("region_name"),
                "endpoint_url": config.get("endpoint_url"),
                "aws_access_key_id": config.get("access_key_id"),
                "aws_secret_access_key": config.get("secret_access_key"),
            }
            self.prefix = config.get("prefix", "")
            self.extra_args = config.get("extra_args", {})
        else:
            self.bucket = s3_config.get("bucket")
            self.api_kwargs = {
                "region_name": s3_config.get("region_name"),
                "endpoint_url": s3_config.get("endpoint_url"),
                "aws_access_key_id": s3_config.get("access_key_id"),
                "aws_secret_access_key": s3_config.get("secret_access_key"),
            }
            self.prefix = s3_config.get("prefix", "")
            self.extra_args = s3_config.get("extra_args", {})

        if not self.bucket:
            raise Exception("S3 bucket must be specified in config block")
        
        self.api_kwargs = {k: v for k, v in self.api_kwargs.items() if v is not None}
        
        if self.prefix and not self.prefix.endswith("/"):
            self.prefix += "/"

        self._s3_client = None
        self._s3_client_lock = threading.Lock()
        self._s3_pool = ThreadPool(1)
        self._pending_files = {}
        
        logger.info("S3 Storage Provider initialized with bucket: %s", self.bucket)
        logger.info("Using media_store_path: %s", self.media_store_path)
        logger.info("Using uploads_path: %s", self.uploads_path)

    def _get_s3_client(self):
        """Get or create an S3 client."""
        if self._s3_client is None:
            with self._s3_client_lock:
                if self._s3_client is None:
                    self._s3_client = boto3.client("s3", **self.api_kwargs)
        return self._s3_client

    def _cleanup_empty_directories(self, path):
        """Recursively remove empty directories."""
        directory = os.path.dirname(path)
        while directory and directory.startswith(self.media_store_path):
            try:
                os.rmdir(directory)
                logger.debug("Removed empty directory: %s", directory)
            except OSError:
                break  # Directory not empty or already removed
            directory = os.path.dirname(directory)

    async def store_file(self, path: str, file_info: Dict[str, Any]) -> None:
        """Store a file in S3 and handle local storage based on config."""
        if not self.media_store_path:
            logger.error("No media_store_path configured")
            raise Exception("No media_store_path configured")

        local_path = os.path.join(self.media_store_path, path)
        abs_path = os.path.abspath(local_path)
        
        try:
            logger.info("Processing file %s with store_local=%s", path, self.store_local)
            
            # First, ensure the file exists
            if not os.path.exists(local_path):
                logger.error("File %s does not exist at %s", path, local_path)
                raise Exception(f"File {path} does not exist at {local_path}")

            # Upload to S3 if store_remote is True or None (default to True)
            if self.store_remote is not False:
                s3_path = self.prefix + path
                s3_client = self._get_s3_client()

                try:
                    with open(local_path, 'rb') as f:
                        s3_client.upload_fileobj(
                            f,
                            self.bucket,
                            s3_path,
                            ExtraArgs=self.extra_args
                        )
                    logger.info("Successfully uploaded %s to S3 at %s", path, s3_path)
                    
                    # Handle remote media lifetime
                    if self.remote_media_lifetime is not None and self.remote_media_lifetime > 0:
                        # Convert milliseconds to seconds
                        retention_seconds = self.remote_media_lifetime / 1000.0
                        logger.info(
                            "File %s in S3 will be deleted after %s seconds",
                            s3_path,
                            retention_seconds
                        )
                        # Schedule S3 deletion
                        reactor.callLater(
                            retention_seconds,
                            self._delete_s3_file,
                            s3_path
                        )
                except Exception as e:
                    logger.error("Failed to upload %s to S3: %s", path, str(e))
                    raise

            # Handle local file based on store_local and retention settings
            if self.store_local is False:
                # If store_local is explicitly False, delete immediately
                try:
                    os.remove(local_path)
                    logger.info("Removed local file after S3 upload: %s", local_path)
                    self._cleanup_empty_directories(local_path)
                    return
                except Exception as e:
                    logger.error("Failed to remove local file %s: %s", local_path, str(e))
                    raise
            else:
                # If store_local is True, handle retention
                retention_period = self.local_media_lifetime
                if retention_period is not None:
                    if retention_period == 0:
                        # Immediate deletion if retention is 0
                        try:
                            os.remove(local_path)
                            logger.info("Removed local file immediately (retention=0): %s", local_path)
                            self._cleanup_empty_directories(local_path)
                        except Exception as e:
                            logger.error("Failed to remove local file %s: %s", local_path, str(e))
                            raise
                    else:
                        # Schedule deletion after retention period (convert from milliseconds to seconds)
                        retention_seconds = retention_period / 1000.0
                        delete_time = reactor.seconds() + retention_seconds
                        self._pending_files[abs_path] = (delete_time, True)
                        logger.info(
                            "File %s will be deleted after %s seconds (store_local=%s)",
                            local_path,
                            retention_seconds,
                            self.store_local
                        )
                        
                        # Schedule the actual deletion
                        reactor.callLater(
                            retention_seconds,
                            self._delete_file_after_retention,
                            abs_path,
                            local_path
                        )
                else:
                    logger.info(
                        "File %s will be kept indefinitely (store_local=%s, no retention)",
                        local_path,
                        self.store_local
                    )

        except Exception as e:
            logger.error("Failed to process %s: %s", path, str(e))
            raise

    def _delete_file_after_retention(self, abs_path: str, local_path: str) -> None:
        """Delete a file after its retention period has expired."""
        try:
            if os.path.exists(local_path):
                os.remove(local_path)
                logger.info("Deleted file after retention period: %s", local_path)
                self._cleanup_empty_directories(local_path)
            if abs_path in self._pending_files:
                del self._pending_files[abs_path]
        except Exception as e:
            logger.error("Failed to delete file %s after retention: %s", local_path, str(e))

    def _delete_s3_file(self, s3_path: str) -> None:
        """Delete a file from S3 after its retention period."""
        try:
            s3_client = self._get_s3_client()
            s3_client.delete_object(Bucket=self.bucket, Key=s3_path)
            logger.info("Deleted file from S3 after retention period: %s", s3_path)
        except Exception as e:
            logger.error("Failed to delete file %s from S3: %s", s3_path, str(e))

    async def fetch(self, path: str, file_info: Optional[Dict[str, Any]] = None) -> Optional[Responder]:
        """Fetch the file with the given path from S3.
        
        Args:
            path: The path of the file to fetch
            file_info: Optional metadata about the file being fetched
            
        Returns:
            Returns a Responder object or None if not found
        """
        logger.info("Fetching %s from S3", path)
        
        s3_path = self.prefix + path
        try:
            s3_client = self._get_s3_client()
            deferred = defer.Deferred()
            
            # Run the S3 download in a thread
            reactor.callInThread(
                s3_download_task,
                s3_client,
                self.bucket,
                s3_path,
                self.extra_args,
                deferred,
                current_context(),
            )
            
            responder = await make_deferred_yieldable(deferred)
            return responder
            
        except Exception as e:
            logger.error("Failed to fetch %s from S3: %s", path, str(e))
            raise

def s3_download_task(s3_client, bucket, key, extra_args, deferred, parent_logcontext):
    """Downloads a file from S3 in a separate thread."""
    with LoggingContext(parent_context=parent_logcontext):
        logger.info("Fetching %s from S3", key)
        try:
            if "SSECustomerKey" in extra_args and "SSECustomerAlgorithm" in extra_args:
                resp = s3_client.get_object(
                    Bucket=bucket,
                    Key=key,
                    SSECustomerKey=extra_args["SSECustomerKey"],
                    SSECustomerAlgorithm=extra_args["SSECustomerAlgorithm"],
                )
            else:
                resp = s3_client.get_object(Bucket=bucket, Key=key)
                
        except s3_client.exceptions.NoSuchKey:
            logger.info("Media %s not found in S3", key)
            reactor.callFromThread(deferred.callback, None)
            return
        except Exception as e:
            logger.error("Error downloading %s from S3: %s", key, str(e))
            reactor.callFromThread(deferred.errback, Failure())
            return

        producer = _S3Responder()
        reactor.callFromThread(deferred.callback, producer)
        _stream_to_producer(reactor, producer, resp["Body"], timeout=90.0)

class _S3Responder(Responder):
    """A Responder that streams from S3."""
    
    def __init__(self):
        self.wakeup_event = threading.Event()
        self.stop_event = threading.Event()
        self.consumer = None
        self.deferred = defer.Deferred()
        
    def write_to_consumer(self, consumer):
        """Start streaming the S3 response to the consumer."""
        self.consumer = consumer
        consumer.registerProducer(self, True)
        self.wakeup_event.set()
        return make_deferred_yieldable(self.deferred)
        
    def resumeProducing(self):
        """Resume producing data to the consumer."""
        self.wakeup_event.set()
        
    def pauseProducing(self):
        """Pause producing data to the consumer."""
        self.wakeup_event.clear()
        
    def stopProducing(self):
        """Stop producing data to the consumer."""
        self.stop_event.set()
        self.wakeup_event.set()
        if not self.deferred.called:
            self.deferred.errback(Exception("Consumer asked to stop producing"))
            
    def _write(self, chunk):
        """Write a chunk of data to the consumer."""
        if self.consumer and not self.stop_event.is_set():
            self.consumer.write(chunk)
            
    def _error(self, failure):
        """Signal an error to the consumer."""
        if self.consumer:
            self.consumer.unregisterProducer()
            self.consumer = None
        if not self.deferred.called:
            self.deferred.errback(failure)
            
    def _finish(self):
        """Signal completion to the consumer."""
        if self.consumer:
            self.consumer.unregisterProducer()
            self.consumer = None
        if not self.deferred.called:
            self.deferred.callback(None)

def _stream_to_producer(reactor, producer, body, timeout=None):
    """Stream the S3 response body to the producer."""
    try:
        while not producer.stop_event.is_set():
            if not producer.wakeup_event.is_set():
                ret = producer.wakeup_event.wait(timeout)
                if not ret:
                    raise Exception("Timed out waiting to resume")
                    
            if producer.stop_event.is_set():
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