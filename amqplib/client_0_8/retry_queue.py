from Queue import Queue as OldQueue
import errno

def retry_on_eintr(function, *args, **kw):
    while True:
        try:
            return function(*args, **kw)
        except IOError, e:            
            if e.errno == errno.EINTR:
                continue
            else:
                raise    

class Queue(OldQueue):
    """Queue which will retry if interrupted with EINTR."""
    def get(self, block=True, timeout=None):
        return retry_on_eintr(OldQueue.get, self, block, timeout)

