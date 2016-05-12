#
# Copyright (C) 2016 Tomas Nilsson (joekickass). All rights reserved.
#


import boto3
import threading


class Worker(object):
    """
    Polls queue for next batch of log data and sends it to kinesis

    TODO:
    Each PutRecords request can support up to 500 records. Each record in the
    request can be as large as 1 MB, up to a limit of 5 MB for the entire request,
    including partition keys. Each shard can support writes up to 1,000 records per
    second, up to a maximum data write total of 1 MB per second.
    """
    _sentinel = None

    def __init__(self, queue, streamname, region=None, partitionkey="thereisonlyoneshard", ):
        """
        Initialize the worker with queue, Kinesis stream name, AWS region and partition key.

        Specifying region is optional. If region is not specified,
        the AWS SDK will try to resolve the default region and fail if not found.

        Specifying partition key is optional. If partition key is not specified,
        it is assumed the Kinesis stream has only one shard and a constant is used as partition key.

        TODO:
        Handle partition key for each record, see comment above.
        """
        self.queue = queue
        self.kinesis = boto3.client('kinesis', region_name=region)
        self.streamname = streamname
        self.partitionkey = partitionkey
        self._stop = threading.Event()
        self._thread = None
        self.validate_stream()

    def validate_stream(self):
        """
        Validate the specified Kinesis stream

        Raises exception if the specified stream is not accessible nor active.
        """
        if 'ACTIVE' != self.kinesis.describe_stream(StreamName=self.streamname)['StreamDescription']['StreamStatus']:
            raise ValueError("{} is not active".format(self.streamname))

    def start(self):
        """
        Start the worker.
        """
        self._thread = threading.Thread(target=self._monitor)
        self._thread.setDaemon(True)
        self._thread.start()

    def stop(self):
        """
        Stop the worker.
        """
        self._stop.set()
        self.queue.put_nowait(self._sentinel)
        self._thread.join()
        self._thread = None

    def prepare(self, records):
        """
        Prepare the batch of records to be sent to Kinesis.

        Data is sent as a `boto3.Kinesis.Client.put_records()` request,
        and needs to be formatted accordingly.
        """
        def fmt(record):
            return { 'PartitionKey' : self.partitionkey, 'Data' : record }

        return [fmt(record) for record in records]

    def handle(self, records):
        """
        Handle a record.

        Send data to Kinesis as a `boto3.Kinesis.Client.put_records()` request.
        """
        data = self.prepare(records)
        try:
            self.kinesis.put_records(
                Records=data,
                StreamName=self.streamname
            )
        except:
            # Just silently drop records if not able to send them
            pass

    def _monitor(self):
        """
        Monitor the queue for records and handle them.

        This method runs on a separate, internal thread.
        The thread will terminate if it sees a sentinel object in the queue.
        """
        q = self.queue
        has_task_done = hasattr(q, 'task_done')

        # Main loop, run until stopped or sentinel is found
        while not self._stop.isSet():
            try:
                records = self.queue.get(True)
                if records is self._sentinel:
                    break
                self.handle(records)
                if has_task_done:
                    q.task_done()
            except:
                pass

        # There might still be records in the queue, run until empty
        while True:
            try:
                records = self.queue.get(False)
                if records is self._sentinel:
                    break
                self.handle(records)
                if has_task_done:
                    q.task_done()
            except queue.Empty:
                break