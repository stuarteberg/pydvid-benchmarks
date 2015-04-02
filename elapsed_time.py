import time
import contextlib
import logging

class ElapsedTime(object):
    def __init__(self):
        self.seconds = 0

@contextlib.contextmanager
def elapsed_time(logger=None, msg="Elapsed time: {} seconds", level=logging.INFO):
    start = time.time()
    result = ElapsedTime()
    yield result
    result.seconds = time.time() - start
    if logger:
        logger.log( level, msg.format( result.seconds ) )    
