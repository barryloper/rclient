import shutil, threading
from concurrent.futures import ThreadPoolExecutor

import pyRserve
from pyRserve import rexceptions

from tornado.concurrent import run_on_executor

class RPoolTornado:
    """ An interface to a concurrent.futures.ThreadPoolExecutor of R connections

        Should have one of these per (Model-version, Rserve) pair

        initialize:
        rp = RPool(max_workers=10)

        evaluate code:
        future_eval = rp.r_eval("some r code")
        result = future_eval.result()

        or pass a callback:
        def some_callback(result):
            print(result)

        rp.r_eval("some r code", some_callback)

    """

    def __init__(self, max_workers=None, initializer=None, support_files=None, *args, **kwargs):
        self._t_local = threading.local()
        self._pool = ThreadPoolExecutor(max_workers=max_workers)
        self.initializer = initializer
        self.support_files = support_files
        self._r_conn_args = args
        self._r_conn_kwargs = kwargs

    @run_on_executor(executor='_pool')
    def r_eval(self, code, callback=None):
        """ Evaluate R Code on the pool of R servers
            Initialize the connection if it is not currently connected

            :param code: String of R code
            :param callback: optional function to be called with return value
            :return: Future
        """

        # fixme: this is rather ugly
        try:
            result = self._t_local.rconn.eval(code)
        except AttributeError:
            # connection not yet made
            self._connect_and_init()
            result = self._t_local.rconn.eval(code)
        except rexceptions.PyRserveClosed:
            # need to re-initialize connection
            self._connect_and_init()
            result = self._t_local.rconn.eval(code)
        #print("returning ", result)
        return result

    @staticmethod
    def _upload(file, dest_dir):
        """ If Rserve isn't on the local host, we'll need to update this function """
        shutil.copy(file, dest_dir)

    def _prepare_support_files(self):
        """ Copy any support files into working dir """
        if self.support_files and self._t_local.wd:
            for f in self.support_files:
                self._upload(f, self._t_local.wd)

    def _prepare_initializer(self):
        """ copy initializer to R working dir and source """
        if self.initializer and self._t_local.wd:
            self._upload(self.initializer, self._t_local.wd)
            self._t_local.rconn.r.source(self.initializer)

    def _initialize_rconn(self):
        """ Connect to R and get working directory """
        self._t_local.rconn = pyRserve.connect(*self._r_conn_args, **self._r_conn_kwargs)
        self._t_local.wd = self._t_local.rconn.r.getwd()

    def _connect_and_init(self):
        self._initialize_rconn()
        self._prepare_support_files()
        self._prepare_initializer()




if __name__ == '__main__':
    """

    """

    from tornado.ioloop import IOLoop
    from tornado import gen
    import time

    io = IOLoop.current()

    def main():
        rp = RPoolTornado(5)
        for i in range(50):
            print_result(rp, i)

    @gen.coroutine
    def print_result(rp, i):
        result = yield rp.r_eval("""Sys.sleep(5); getwd()""")
        print(i, result)

    io.add_callback(main)
    io.start()


