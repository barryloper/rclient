import os, time
import threading, queue
import pyRserve

DEFAULT_THREADCOUNT_SCALE = 5
DEFAULT_WAITING_JOBS_SCALE = 10
RESULT_QUEUE_SCALE = 100
DEFAULT_SUBMIT_TIMEOUT = .5

class RConnectorThread(threading.Thread):
    """
        adapted from http://eli.thegreenplace.net/2011/12/27/python-threads-communication-and-stopping
    """
    def __init__(self, in_q, out_q):
        super().__init__()
        self.in_q = in_q
        self.out_q = out_q
        self.stoprequest = threading.Event()

    def run(self):
        """
             As long as we weren't asked to stop, try to take new tasks from the
             queue. The tasks are taken with a blocking 'get', so no CPU
             cycles are wasted while waiting.
             Also, 'get' is given a timeout, so stoprequest is always checked,
             even if there's nothing in the queue.
        :return:
        """

        r = pyRserve.connect()  # todo: args for connection?
        # todo: reset rconnection sometimes?
        while not self.stoprequest.isSet():
            try:
                requestor, job = self.in_q.get(block=True, timeout=0.05)  # will this work when queue contains tuples?
                result = r.eval(job)
                self.out_q.put((requestor, result))
            except queue.Empty:
                """ todo: can we use this exception to shutdown the pool like some kind of timeout? """
                continue
            except queue.Full:
                """ should we handle this?"""
                raise

    def join(self, timeout=None):
        self.stoprequest.set()
        super().join(timeout)


class RPool:
    """
        Manages a pool of threads that handle Rserve connections

        creates an interface to a threadpool of R connections

        initialize:
        rp = RPool(max_waiting=100, workers=10)
        starts 10 threads and doesn't allow more than 100 jobs to be waiting. User must handle queue.Full exception
        when submitting

        user can either watch rp.results, or call rp.get_result(timeout)

    """

    def __init__(self, max_waiting=None, workers=None):
        if workers is None:
            # Use this number because ThreadPoolExecutor is often
            # used to overlap I/O instead of CPU work.
            workers = (os.cpu_count() or 1) * DEFAULT_THREADCOUNT_SCALE
        if max_waiting is None:
            max_waiting = workers * DEFAULT_WAITING_JOBS_SCALE
        if workers <= 0 or max_waiting < 0:
            raise ValueError

        self._workers = workers
        self._max_waiting = max_waiting
        self._job_queue = queue.Queue(maxsize=max_waiting)
        self._results_queue = queue.Queue(maxsize=max_waiting*RESULT_QUEUE_SCALE)
        self._threads = set(RConnectorThread(in_q=self.jobs, out_q=self.results) for _ in range(workers))
        self._shutdown = False
        self._shutdown_lock = threading.Lock()

    @property
    def jobs(self):
        return self._job_queue

    @property
    def results(self):
        return self._results_queue

    def submit(self, caller, job, timeout=None):
        """ adds job to queue and annotates it as from 'caller'
            raises queue.Full if queue is full
        """
        with self._shutdown_lock:  # is this a lot of overhead?
            if self._shutdown is False:
                self.jobs.put((caller, job), block=True, timeout=timeout)
            else:
                raise RuntimeError("Pool is shutting down. No more jobs accepted.")

    def get_result(self, timeout=None):
        _res = self.results.get(block=True, timeout=timeout)
        return _res

    def start(self):
        for _t in self._threads:
            _t.start()

    def stop(self, flush=False, wait=True):
        """ Stops further submission.
            Completes all pending jobs if flush is True
            Joins all threads if wait is True
            join pooled threads. wait for queue to complete if flush==True
        """
        with self._shutdown_lock:
            self._shutdown = True
        if flush is True:
            self.jobs.join()
        if wait is True:
            for _t in self._threads:
                _t.join()


if __name__ == '__main__':
    """
        todo:
        thread pool should be lazily created, then time out after a while
    """

    rpool = RPool(max_waiting=10, workers=5)
