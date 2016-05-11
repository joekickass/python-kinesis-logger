# python-kinesis-logger

The KinesisHandler is a [BufferingHandler](https://docs.python.org/2.7/library/logging.handlers.html#logging.handlers.BufferingHandler) that sends logging output to a AWS Kinesis stream.

It offloads work to a worker thread decoupled by a queue, inspired by [QueueHandler](https://docs.python.org/3.5/library/logging.handlers.html#queuehandler).
