"""
RServe Connection Pool
Based on RConnectionPool
http://icbtools.med.cornell.edu/javadocs/RUtils/edu/cornell/med/icb/R/RConnectionPool.html

# Usage:

rpool = Pool(
    pool_size = 10,
    realtime = False
)

result = rpool.eval('''some r code''')

# Interactive usage:

c = rpool.connect()
r1 = c.eval('''some r code''')
r2 = c.eval('''some other r code''')
c.close()

# Context usage:

with rpool.connect() as c:
    r1 = c.eval('''some r code''')
    r2 = c.eval('''some other r code''')

todo:

upload a shared model bundle that all pool processes have access to
Perhaps as interpreters boot up, they source some file(s) in the bundle? For the purpose of loading functions.
Problem would arise if sourcing the files runs the model.

clean up temp folders

possible other methods to implement: map, map_async, star_map

don't need inter-process communication or synchronization. just need to maintain a pool of processes on which
a model may be evaluated.

what if we can add a model bundle to the pool, so that each connection has access to a set of files.
don't want each connection to be able to write to the model bundle. Each connection gets their own copy of the bundle.

handle if we lose connection for some reason. pyRserve has a decorator

fixme: don't allow adding processes to pool to block the tornado loop
actually, i/o blocking might not be a problem since we're in a thread

don't want to just make a pool of python processes, but want do do something similar.
the pool will be of rserve connections

"""

import logging
from functools import wraps

import pyRserve

__all__ = ['RPool']

logger = logging.getLogger(__name__)

RSERVEPORT = pyRserve.rconn.RSERVEPORT

_defaultOOBCallback = pyRserve.rconn._defaultOOBCallback


class PoolEmpty(KeyError):
    pass


class DisconnectedError(BaseException):
    pass


class MethodNotAllowed(BaseException):
    pass


def if_pool_is_not_full(wrapped_method):
    @wraps(wrapped_method)
    def wrapper(self, *args, **kw):
        if len(self.pool) < self._pool_size:
            return wrapped_method(self, *args, **kw)

    return wrapper


class RServeConnection(object):

    def __init__(self, pool_size=1, realtime=False, *cargs, **ckwargs):
        """
        todo: :param model_dir: read-only location where uploads are stored
        todo: in lieu of save_files=False, have a periodic cleanup routine

        fixme: wouldn't it make sense for each model to have its own pool and model directory?
               then, RServe's tmp directory would truly be isolated tmp.
               then we could call pool.eval("some r expression") and it would just work.
               Any setup/teardown R code would need to be called when the connection is made.
               Such code should be minimal, as it is going to cause latency in getting a pool up, and for overflow connections.
               We don't need to evaluate multiple expressions, just some 'main' entrypoint to their model.

        todo: :param constructor: function/class that will return a connection
        :param cargs: args passed to constructor: constructor(*cargs, **ckwargs)
        :param ckwargs: see cargs
        :param pool_size: minimum number of connections. This is the initial size of the pool
        :param realtime: realtime pools will not close connections before returning to pool

        """

        self._cargs = cargs
        self._ckwargs = ckwargs

        self._pool_size = pool_size
        self._realtime = realtime

        self._init_pool()

    def __del__(self):
        try:
            self._close_all()
        except pyRserve.rexceptions.PyRserveClosed:
            pass

    def eval(self, expression):
        """ checkout a connection, execute an expression, then close the connection
            connection will check itself back in
        """
        c = self._checkout()
        retval = c.eval(expression)
        c.close()
        return retval

    def _init_pool(self):
        # todo: can we parallelize this?
        self.pool = [self._new_connection() for _ in range(self._pool_size)]

    def _new_connection(self):
        """ creates the connections for storage in the pool.
            don't use this for interactive connections
        """
        return _PooledPyRserve(*self._cargs, **self._ckwargs)

    def _close_all(self):
        """ clean up each connection """
        for c in self.pool:
            logger.debug("closing ", id(c))
            c.close()

    def _checkout(self):
        """ pulls a connection from the pool, or creates a new one.
            returns an wrapper for the connection which knows how to check itself back in
        """
        try:
            c = self.pool.pop()
        except IndexError:
            c = self._new_connection()
        return _PooledConnectionInteractor(pool=self, connection=c)

    connect = _checkout

    @if_pool_is_not_full
    def _checkin(self, c):
        """ Returns the connection to the pool
            If pool is full, allow the connection to be garbage collected. (do nothing)
            If we aren't real-time, reset the connection.

        """

        if self._realtime is False:
            c.reset()

        self.pool.append(c)

    checkin = _checkin

    def terminate(self, c):
        """kill a connection harshly"""
        raise NotImplementedError

    def upload(self, archive):
        raise NotImplementedError


def only_if_open(wrapped_method):
    """ method decorator that only runs the decorated function if the connection is open
    """

    @wraps(wrapped_method)
    def wrapper(self, *args, **kw):
        if self.connection is not None:
            return wrapped_method(self, *args, **kw)

    return wrapper


class _PooledConnectionInteractor(object):
    """ A wrapper for Rserve connection that you've taken out of the pool.

        knows what pool it came from and how to check itself back in.

        todo: look at out-of-bounds messages https://pythonhosted.org/pyRserve/manual.html#out-of-bounds-messages-oob
    """

    def __init__(self, pool, connection):
        """"""
        self._pool = pool
        self._connection = connection

    def __del__(self):
        try:
            self.close()
        except pyRserve.rexceptions.PyRserveClosed:
            pass

    @only_if_open
    def __getattr__(self, item):
        return getattr(self.connection, item)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    @property
    def connection(self):
        return self._connection

    @property
    def pool(self):
        return self._pool

    @only_if_open
    def _checkin(self):
        """ check our connection back into the pool """
        try:
            self.pool.checkin(self.connection)
        except AttributeError:
            """pool is probably already None"""
            pass

        self._pool = None

    @only_if_open
    def close(self):
        """ return our connection to the pool, and remove our reference to it """
        self._checkin()
        self._connection = None

    @staticmethod
    def shutdown():
        """ don't allow shutting down of rserve
            self.connection.shutdown may still be used, but it's not recommended
        """
        raise MethodNotAllowed('''Please don't shut down the RServe from here''')


class _PooledPyRserve(pyRserve.rconn.RConnector):
    """ extends pyRserve's RConnector with default arguments

        prevents shutting down the R server from an individual connection

        adds the ability to reset the connection, starting a new R interpreter
    """

    def __init__(self, host='', port=RSERVEPORT, atomicArray=False, defaultVoid=False,
                 oobCallback=_defaultOOBCallback):
        super().__init__(host, port, atomicArray, defaultVoid, oobCallback)

    def __del__(self):
        """ prevent stale RServe handles, since the parent class doesn't do this. """
        try:
            self.close()
        except pyRserve.rexceptions.PyRserveClosed:
            pass

    @property
    def wd(self):
        return self.r.getwd()

    def reset(self):
        """ make a new connection
        """
        self.close()
        self.connect()

    def shutdown(self):
        """" don't want a connection shutting down the r server. """
        raise MethodNotAllowed('''Shutting down the R server is not allowed from pooled connections.''')




