# python-kinesis-logger

[KinesisHandler](kinesishandler/kinesishandler.py) is a [BufferingHandler](https://docs.python.org/2.7/library/logging.handlers.html#logging.handlers.BufferingHandler) that sends logging output to a AWS Kinesis stream.

It offloads work to a worker thread decoupled by a queue, inspired by [QueueHandler](https://docs.python.org/3.5/library/logging.handlers.html#queuehandler).

## Installing

Pip:

    pip install git+https://github.com/joekickass/python-kinesis-logger#egg=kinesishandler

Manual:

    python setup.py install

## Usage

```python
import logging
import kinesishandler
import queue

# get root logger
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

# create kinesis handler
q = queue.Queue()
handler = kinesishandler.KinesisHandler(10, q)
worker = kinesishandler.Worker(q, "exampleStreamName", region="eu-west-1")
worker.start()

# create formatter and add to handler
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

# add handler to logger
logger.addHandler(handler)

# log
logger.debug('info message')

# quit
worker.stop()
```