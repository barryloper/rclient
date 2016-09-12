import shutil, threading, tempfile, re
from concurrent.futures import ThreadPoolExecutor

import execnet

from tornado.concurrent import run_on_executor

# todo: make abstract base class for these things
# todo: call Group.terminate explicitly to close the gateways out?

DEFAULT_TIMEOUT = 10


def _exception_handler(message):
    """ todo """
    print(message)


class ExecnetTornado:
    """ An interface to a concurrent.futures.ThreadPoolExecutor of execnet jobs

        Should have one of these per (Model-version, Plugin) pair

        Requires Tornado I/O Loop

        Example:

        def main():
            executor = ExecnetTornado(max_workers=20, python='/usr/local/anaconda3/envs/py27/bin/python', initializer="py2_test_init.py")
            for i in range(50):
                print_sleepies(executor, i, random.randint(1, 10))

        @gen.coroutine
        def print_sleepies(ep, i, r):
            result = yield ep.call_function("sleepy_randnum", r)
            print("sleepy iteration", i, "result", result)


        ioLoop.add_callback(main)

    """

    def __init__(self, max_workers=None, initializer="", support_files=None, python="python3", *args, **kwargs):
        self._t_local = threading.local()
        self._pool = ThreadPoolExecutor(max_workers=max_workers)

        self.initializer = initializer
        self.initializer_module_name = re.sub(r"\.py$", "", initializer)
        self.support_files = support_files
        self._r_conn_args = args
        self._r_conn_kwargs = kwargs
        self._python = python

    @run_on_executor(executor='_pool')
    def call_function(self, function, *args, **kwargs):
        print("calling", function, "on", threading.current_thread().ident, "with", args)
        try:
            self._t_local.channel.send((function, args, kwargs))
        except AttributeError:
            # connection not yet made
            self._connect_and_init()
            self._t_local.channel.send((function, args, kwargs))
        except self._t_local.channel.RemoteError:
            # need to re-initialize connection
            self._connect_and_init()
            self._t_local.channel.send((function, args, kwargs))
            # print("returning ", result)
        return self._t_local.channel.receive()

    def _connect_and_init(self):
        """ These need to be called inside a worker thread, as they rely on thread local storage """
        self._initialize_gateway()
        self._prepare_support_files()
        self._prepare_initializer()
        self._start_channel_responder()

    @staticmethod
    def _upload(file, dest_dir):
        """ If gateway isn't on the local host, we'll need to update this function
            todo: maybe we can do this with execnet.RSync(), but this requires a dir rather than a file
        """
        print("uploading", file, "to", dest_dir)
        shutil.copy(file, dest_dir)

    def _prepare_support_files(self):
        """ Copy any support files into working dir """
        if self.support_files and self._t_local.wd:
            for f in self.support_files:
                self._upload(f, self._t_local.wd.name)

    def _prepare_initializer(self):
        """ copy initializer working dir and import as a module """
        if self.initializer and self._t_local.wd:
            self._upload(self.initializer, self._t_local.wd.name)

    def _initialize_gateway(self):
        """ Create a execnet gateway """
        self._t_local.wd = tempfile.TemporaryDirectory()
        self._t_local.gateway = execnet.makegateway("popen//python={}//chdir={}".format(self._python, self._t_local.wd.name))

    def _start_channel_responder(self):
        self._t_local.channel = self._t_local.gateway.remote_exec("""
            from {initializer} import *

            while not channel.isclosed():
                try:
                    fn_name, args, kwargs = channel.receive(timeout={TIMEOUT})

                    try:
                        fn = globals()[fn_name]
                    except KeyError:
                        fn = globals()['__builtins__'][fn_name]
                    except KeyError:
                        raise NameError("function not found")

                    result = fn(*args, **kwargs)
                    # fixme: maybe a better way to do this. this seems limited
                    channel.send(result)
                except channel.TimeoutError:
                    pass

        """.format(TIMEOUT=DEFAULT_TIMEOUT, initializer=self.initializer_module_name))

if __name__ == '__main__':
    """ Demo

    """

    from tornado.ioloop import IOLoop
    from tornado import gen
    import time
    import random

    io = IOLoop.current()

    def main():
        ep = ExecnetTornado(max_workers=5, python='/usr/local/anaconda3/envs/py27/bin/python', initializer="py2_test_init.py")
        #ep3 = ExecnetTornado(max_workers=20, python='python3.5')
        for i in range(10):
            #print_result(ep, i)
            print_sleepies(ep, i, random.randint(1, 20))
            #print_builtin(ep, i)

    @gen.coroutine
    def print_result(ep, i):
        result = yield ep.call_function("randnum")
        print("       iteration", i, "result", result)

    @gen.coroutine
    def print_sleepies(ep, i, r):
        result = yield ep.call_function("sleepy_randnum", r)
        print("sleepy iteration", i, "result", result)

    @gen.coroutine
    def print_builtin(ep, i):
        r = random.randint(1, 10)
        result = yield ep.call_function("sum", [r, 100])
        print("result of sum({}, 100)".format(r), result)

    io.add_callback(main)
    io.start()


