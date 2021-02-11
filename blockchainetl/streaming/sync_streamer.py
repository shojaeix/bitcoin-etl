# MIT License
#
# Copyright (c) 2018 Evgeny Medvedev, evge.medvedev@gmail.com
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.


import logging
import time
import json

from blockchainetl.streaming.streamer_adapter_stub import StreamerAdapterStub
import threading
from time import sleep

import pika


class SyncStreamer:
    def __init__(
            self,
            blockchain_streamer_adapter=StreamerAdapterStub(),
            period_seconds=10,
            retry_errors=True,
            propagate_logs=False,
            rabbit_host="localhost",
            rabbit_port=5672,
            signals_queue="queue-signals",
    ):

        self.rabbit_host = rabbit_host
        self.rabbit_port = rabbit_port
        self._last_signal_processed = False
        self.blockchain_streamer_adapter = blockchain_streamer_adapter
        self.period_seconds = period_seconds
        self.retry_errors = retry_errors
        self.signals_queue = signals_queue
        self.propagate_logs = propagate_logs

        # threading.Thread(target=self.send_test_signals).start()
        self._listen_for_signals_in_a_new_thread()

        # disable RabbitMQ debug logs
        logging.getLogger("pika").propagate = self.propagate_logs

    def stream(self):
        try:
            self.blockchain_streamer_adapter.open()
            # run
            self._do_stream()
        finally:
            # cleaning and finish
            self.blockchain_streamer_adapter.close()

    def _listen_for_signals_in_a_new_thread(self):
        threading.Thread(target=self._listen_for_signals).start()

    def _signal_callback(self, ch, method, properties, body):
        self.last_signal_body = body
        self._last_signal_processed = False
        if self.propagate_logs:
            print(" [x] Received signal %r" % body)
        pass

    # listen the signals queue and keep the last message in  self.last_signal
    def _listen_for_signals(self):
        channel = self._new_rabbit_channel()

        channel.queue_declare(queue=self.signals_queue)

        channel.basic_consume(queue=self.signals_queue,
                              auto_ack=True,
                              on_message_callback=self._signal_callback)

        print('Listening for signals')
        channel.start_consuming()

    # process signals and return next block number(-1 for finishing the stream)
    def _calculate_next_block(self, last, last_send_time) -> int:
        # keep sending by order if there is no signal yet
        if not hasattr(self, "last_signal_body") or self.last_signal_body is None:
            if self.propagate_logs:
                print("last signal body is empty")
            return last + 1

        if self.propagate_logs:
            print("last signal body: " + self.last_signal_body.decode("utf-8"))
        # decode the last signal
        last_signal = json.loads(self.last_signal_body)

        # finish the process if the end block is -1
        if "end_block" in last_signal and (last_signal["end_block"] == -1):
            return -1

        if "expecting_block" in last_signal:
            expecting_block = last_signal["expecting_block"]
            # ELSE update the next block if signal expecting block > next
            if expecting_block > last:
                self._last_signal_processed = True
                return expecting_block
            # ELSE nothing, if signal.expectingBlock == next
            elif expecting_block == last:
                self._last_signal_processed = True
                return last + 1
            # ELSE if signal.ExpectingBlock < next
            else:
                if self._last_signal_processed:
                    return last + 1
                self._last_signal_processed = True
                if "timestamp" in last_signal and last_signal["timestamp"] > last_send_time:
                    return expecting_block
                # back to expecting block if there is at least 3 block distance
                elif expecting_block < last - 3:
                    return expecting_block
                else:
                    return last + 1

        return last + 1

    def _new_rabbit_channel(self) -> pika.adapters.blocking_connection.BlockingChannel:

        connection = pika.BlockingConnection(pika.ConnectionParameters(self.rabbit_host))
        if connection.is_open:
            return connection.channel()
        raise Exception("couldn't create a new RabbitMQ connection/channel")

    def _do_stream(self):
        last = -1
        next_block = 0
        last_try = 0
        last_successful_try = 0
        latest_block = self._get_latest_block_number()
        while True:
            # todo check queue messages limit and wait if limit is reached

            if self.retry_errors:
                # sleep between failures
                if last_try > last_successful_try and last_try > time.time() - 3:
                    sleep(3)

            # if it's not the first loop
            if last != -1:
                if self.period_seconds > 0:
                    sleep(self.period_seconds)
                # calculate next block base on signals and last block
                next_block = self._calculate_next_block(last, last_successful_try)
                if next_block == -1:
                    break

            if self.propagate_logs:
                print("Next block should be: " + next_block.__str__())

            last_try = time.time()

            if next_block > latest_block:
                # update latest block
                latest_block = self._get_latest_block_number()
                if next_block > latest_block:
                    if self.propagate_logs:
                        print("next_block is greater than latest block. should wait more")
                    continue

            # Try to stream and send next block
            try:
                if self._stream_and_send_block(next_block):
                    # update last
                    last = next_block
                    last_successful_try = time.time()

            except Exception as e:
                # https://stackoverflow.com/a/4992124/1580227
                logging.exception('An exception occurred while syncing block data.')
                if not self.retry_errors:
                    raise e
        pass

    def _stream_and_send_block(self, block_number: int) -> bool:
        self.blockchain_streamer_adapter.export_all(start_block=block_number, end_block=block_number)
        return True

    def _get_latest_block_number(self) -> int:
        return self.blockchain_streamer_adapter.get_current_block_number()

    def send_test_signals(self):
        channel = self._new_rabbit_channel()
        channel.queue_declare(self.signals_queue)
        signal_body = str.encode(json.dumps({"expecting_block": 390000, "end_block": 300000}))
        channel.basic_publish(exchange="", routing_key=self.signals_queue, body=signal_body)

        sleep(10)

        signal_body = str.encode(json.dumps({"expecting_block": 400000}))
        channel.basic_publish(exchange="", routing_key=self.signals_queue, body=signal_body)

        sleep(10)
        signal_body = str.encode(json.dumps({"expecting_block": 390000, "end_block": 420000}))
        channel.basic_publish(exchange="", routing_key=self.signals_queue, body=signal_body)

        sleep(20)
        signal_body = str.encode(json.dumps({"end_block": -1}))
        channel.basic_publish(exchange="", routing_key=self.signals_queue, body=signal_body)
        pass
