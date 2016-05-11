from setuptools import setup

setup(name='kinesishandler',
      version='0.1',
      description='The KinesisHandler is a BufferingHandler that sends logging output to a AWS Kinesis stream',
      url='https://github.com/joekickass/python-kinesis-logger',
      author='joekickass',
      author_email='tnilsson.x@gmail.com',
      license='MIT',
      packages=['kinesishandler'],
      zip_safe=False)