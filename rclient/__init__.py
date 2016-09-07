
from . import connector, rservecontext, tornado_executor

RServeConnection = connector.RServeConnection
RContext = rservecontext.RContext

RPool = connector.RServeConnection  # for backwards compatibility
rpool = connector

RPoolTornado = tornado_executor.RPoolTornado