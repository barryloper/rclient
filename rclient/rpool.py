"""
RServe Connection Pool
Based on RConnectionPool
http://icbtools.med.cornell.edu/javadocs/RUtils/edu/cornell/med/icb/R/RConnectionPool.html

This module creates and manages a pool of connections to an RServe process. It uses the idea that Python modules are
are only imported once, sys.modules registry is thread safe, and a module can maintain state to maintain a context
for a persistent pool of RServe connections.

fixme: or should i use a factory function to return a connection instance?

todo:
make sure each connection gets cleaned up
allow users to specify number of connections

python has built-in queue modules i can use
need to start with N processes in a queue. when one finishes, clean up, then add to back of queue
If the queue is full, add more processes (this could be an interesting optimization target. how fast to add processes to keep up with demand)
Clean up idle processes in excess of N if they've been idle for "some time"
Actually, how bout an idle target config? When number of idle processes gets below target, start some more in a non-blocking way



since connections will be reused, the idea of saving uploaded files doesn't work. If we want to do this later, the cleanup
should move files to some archive location.

not using asynchio.queue, since we aren't sending commands to subscribers

rserve is already started. we are pooling connections. Each new connection, rserve will start another R process

usage:
import rpool # initializes the pool with config if first import

with rpool.connect() as conn:
    conn.upload('myModel.zip')
    conn.eval('''some R code''')

"""

import logging
from .rservecontext import RContext

logger = logging.getLogger(__name__)

# fixme: don't allow adding processes to pool to block the tornado loop
# actually, i/o blocking might not be a problem since we're in a thread

#don't want to just make a pool of python processes, but want do do something similar.
#the pool will be of rserve connections

"""
rpool = Pool(
    connections=10,
    initargs={'save_files': False},
    maxtasksperchild=None
)

todo:

possible other methods to implement: map, map_async, star_map

don't need inter-process communication or synchronization. just need to maintain a pool of processes on which
a model may be evaluated.

what if we can add a model bundle to the pool, so that each connection has access to a set of files.
don't want each connection to be able to write to the model bundle. Each connection gets their own copy of the bundle.

would be nice to have each connection context-managed, so its __exit__ would be called when necessary

handle if we lose connection for some reason. pyRserve has a decorator

"""


class OutOfResources(BaseException):
    pass


class PoolEmpty(KeyError):
    pass


class OverflowNotAvailable(BaseException):
    pass


class RPool(object):

    def __init__(self, constructor, *cargs, pool_size=1, max_overflow=-1, realtime=False, save_files=True, **ckwargs):
        """
        todo: :param min_sleeping: attempt to keep a minimum buffer of N connections ready for

        fixme: wouldn't it make sense for each model to have its own pool and model directory?
               then, RServe's tmp directory would truly be isolated tmp.
               then we could call pool.eval("some r expression") and it would just work.
               Any setup/teardown R code would need to be called when the connection is made.
               Such code should be minimal, as it is going to cause latency in getting a pool up, and for overflow connections.
               We don't need to evaluate multiple expressions, just some 'main' entrypoint to their model.

        :param constructor: function/class that will return a connection
        :param cargs: args passed to constructor: constructor(*cargs, **ckwargs)
        :param ckwargs: see cargs
        :param pool_size: minimum number of connections
        :param max_overflow: maximum additional connections to allow once the pool_size is exhausted
        :param realtime: realtime pools will not close connections before returning to pool
        :param save_files: passed to constructor

        Other args and kwargs passed to constructor
        """
        if not issubclass(constructor, RContext):
            raise TypeError("constructor must be a subclass of RContext")
        else:
            self._constructor = constructor
            self._cargs = cargs
            self._ckwargs = ckwargs

        self._pool_size = pool_size
        self._overflow_used = 0
        self._max_overflow = max_overflow
        self._realtime = realtime
        self._save_files = save_files

        self._pool = []
        self._checked_out = []
        self._repopulate_pool()

    def connect(self):
        """ Return a connection from the pool, and note connection as checked out
            fixme: what happens when we connect, but don't assign it to a reference that we can close later??
              if the refcount of something that is in checked_out == 1, check it back in?

            disconnect is called from the checked out item

        """
        try:
            c = self._checkout()
        except PoolEmpty:
            logger.debug("""Pool of {} empty. Creating overflow {} of {}.""".format(self._pool_size,
                len(self._checked_out) - self._pool_size, self._max_overflow))
            c = self._try_overflow()

        return c

    def close(self, c): # how does the client call this?
        # pool.close() is that's right?
        """Return a checked-out connection to the pool. Restart it if realtime is False"""
        if self._realtime is False:
            c.close()
            self._repopulate_pool()
        else:
            self._checkin(c)

    def terminate(self, c):
        """kill a connection harshly"""
        raise NotImplementedError

    def _overflow(self):
        """Create and check out an overflow connection if """
        if self._resources_are_available():
            c = self._make_connection()
            self._checkout(c)
            return c
        else:
            # fixme: can do this better. maybe a timeout
            raise OutOfResources("No more connections currently available. Try again later.")

    def _checkout(self):
        """ used by self.connect() to mark a connection checked out """
        try:
            c = self._pool.pop()
        except KeyError:
            raise PoolEmpty("No available connections in pool")
        else:
            self._checked_out.append(c)
            return c

    def checkin(self, c): # sqlalchemy puts these methods on the connection proxy object.
        """ usesd by RContext.close() to check the connection back into the pool """
        if self._realtime is False:
            c.reconnect()

        self._checked_out.remove(c)
        self._pool.append(c)

    def _make_connection(self):
        return self._constructor(pool=self, save_files=self._save_files, *self._cargs, **self._ckwargs)

    def _repopulate_pool(self):
        while len(self._pool) < self._pool_size:
            self._pool.append(self._make_connection())
            #todo: can _pool be something that has context management?
            #todo: can this be done async?

    def _resources_are_available(self):
        if self._max_overflow > -1:
            return len(self._checked_out) < self._pool_size + self._max_overflow
        else:
            return len(self._checked_out) < self._pool_size

