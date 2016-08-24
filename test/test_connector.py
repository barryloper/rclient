import sys

sys.path.append('../rclient')

from rclient import RServeConnection

r = None

def test_rpool_init():
    global r  # fixme: make this a fixture? or wrap it in a class
    r = RServeConnection(pool_size=5, realtime=False)
    assert len(r.pool) == 5, "Failed to add 5 processes to the pool"

def test_rpool_eval():
    result = r.eval("2*2")
    assert result == 4, "Failed to run simple calculation"

