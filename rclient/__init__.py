
from . import connector, rservecontext

RServeConnection = connector.RServeConnection
RContext = rservecontext.RContext

RPool = connector.RServeConnection  # for backwards compatibility
rpool = connector