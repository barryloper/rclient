
from . import rpool, rservecontext

RServeConnection = rpool.RServeConnection
RContext = rservecontext.RContext

RPool = rpool.RServeConnection  # for backwards compatibility