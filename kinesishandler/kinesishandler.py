#
# Copyright (C) 2016 Tomas Nilsson (joekickass). All rights reserved.
#


from logging.handlers import BufferingHandler


class KinesisHandler(BufferingHandler):
    """
    Sends logs in batches to Kinesis

    Uses a queue to dispatch batched data to worker thread
    """
    def __init__(self, capacity, queue):
        """
        Initialize the handler with buffer size and queue
        """
        BufferingHandler.__init__(self, capacity)
        self.queue = queue

    def prepare(self, records):
        """
        Prepare data for queuing

        TODO: Is self.format() keeping all info? what about errors?
        """
        return [self.format(record) for record in records]

    def flush(self):
        """
        Put buffered data in queue and zap buffer.
        """
        self.acquire()
        try:
            self.queue.put(self.prepare(self.buffer))
            self.buffer = []
        finally:
            self.release()