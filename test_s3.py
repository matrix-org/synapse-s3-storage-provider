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

from twisted.internet import defer
from twisted.python.failure import Failure
from twisted.test.proto_helpers import MemoryReactorClock
from twisted.trial import unittest

import sys

is_py2 = sys.version[0] == "2"
if is_py2:
    from Queue import Queue
else:
    from queue import Queue

from threading import Event, Thread

from mock import Mock

from s3_storage_provider import _stream_to_producer, _S3Responder, _ProducerStatus


class StreamingProducerTestCase(unittest.TestCase):
    def setUp(self):
        self.reactor = ThreadedMemoryReactorClock()

        self.body = Channel()
        self.consumer = Mock()
        self.written = ""

        def write(data):
            self.written += data

        self.consumer.write.side_effect = write

        self.producer_status = _ProducerStatus()
        self.producer = _S3Responder()
        self.thread = Thread(
            target=_stream_to_producer,
            args=(self.reactor, self.producer, self.body),
            kwargs={"status": self.producer_status, "timeout": 1.0},
        )
        self.thread.daemon = True
        self.thread.start()

    def tearDown(self):
        # Really ensure that we've stopped the thread
        self.producer.stopProducing()

    def test_simple_produce(self):
        deferred = self.producer.write_to_consumer(self.consumer)

        self.body.write("test")
        self.wait_for_thread()
        self.assertEqual("test", self.written)

        self.body.write(" string")
        self.wait_for_thread()
        self.assertEqual("test string", self.written)

        self.body.finish()
        self.wait_for_thread()

        self.assertTrue(deferred.called)
        self.assertEqual(deferred.result, None)

    def test_pause_produce(self):
        deferred = self.producer.write_to_consumer(self.consumer)

        self.body.write("test")
        self.wait_for_thread()
        self.assertEqual("test", self.written)

        # We pause producing, but the thread will currently be blocked waiting
        # to read data, so we wake it up by writing before asserting that
        # it actually pauses.
        self.producer.pauseProducing()
        self.body.write(" string")
        self.wait_for_thread()
        self.producer_status.wait_until_paused(10.0)
        self.assertEqual("test string", self.written)

        # If we write again we remain paused and nothing gets written
        self.body.write(" second")
        self.producer_status.wait_until_paused(10.0)
        self.assertEqual("test string", self.written)

        # If we call resumeProducing the buffered data gets read and written.
        self.producer.resumeProducing()
        self.wait_for_thread()
        self.assertEqual("test string second", self.written)

        # We can continue writing as normal now
        self.body.write(" third")
        self.wait_for_thread()
        self.assertEqual("test string second third", self.written)

        self.body.finish()
        self.wait_for_thread()

        self.assertTrue(deferred.called)
        self.assertEqual(deferred.result, None)

    def test_error(self):
        deferred = self.producer.write_to_consumer(self.consumer)

        self.body.write("test")
        self.wait_for_thread()
        self.assertEqual("test", self.written)

        excp = Exception("Test Exception")
        self.body.error(excp)
        self.wait_for_thread()

        self.failureResultOf(deferred, Exception)

    def wait_for_thread(self):
        """Wait for something to call `callFromThread` and advance reactor
        """
        self.reactor.thread_event.wait(1)
        self.reactor.thread_event.clear()
        self.reactor.advance(0)


class ThreadedMemoryReactorClock(MemoryReactorClock):
    """
    A MemoryReactorClock that supports callFromThread.
    """

    def __init__(self):
        super(ThreadedMemoryReactorClock, self).__init__()
        self.thread_event = Event()

    def callFromThread(self, callback, *args, **kwargs):
        """
        Make the callback fire in the next reactor iteration.
        """
        d = defer.Deferred()
        d.addCallback(lambda x: callback(*args, **kwargs))
        self.callLater(0, d.callback, True)

        self.thread_event.set()

        return d


class Channel(object):
    """Simple channel to mimic a thread safe file like object
    """

    def __init__(self):
        self._queue = Queue()

    def read(self, _):
        val = self._queue.get()
        if isinstance(val, Exception):
            raise val
        return val

    def write(self, val):
        self._queue.put(val)

    def error(self, err):
        self._queue.put(err)

    def finish(self):
        self._queue.put(None)

    def close(self):
        pass
