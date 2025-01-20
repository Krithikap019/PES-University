import logging

from kazoo.client import KazooClient
from kazoo.client import KazooState
from kazoo.exceptions import ConnectionLossException
from kazoo.exceptions import NoAuthException
from kazoo.recipe.watchers import ChildrenWatch
from kazoo.recipe.watchers import DataWatch
from kazoo.handlers.gevent import SequentialGeventHandler

logging.basicConfig()

zk = KazooClient(hosts='zoo1:2181',handler=SequentialGeventHandler())
event=zk.start_async()
event.wait(timeout=30)
if not zk.connected:
    # Not connected, stop trying to connect
    print("ummm")
    zk.stop()
    raise Exception("Unable to connect.")
def my_callback(async_obj):
    try:
        children = async_obj.get()
        print("hi")
    except (ConnectionLossException, NoAuthException):
        sys.exit(1)

# Both these statements return immediately, the second sets a callback
# that will be run when get_children_async has its return value
zk.create_async("/master",b'mas')
zk.create_async("/master/node_1",b'c1')
print("gdgd")
async_obj = zk.get_children_async("/master")
async_obj.rawlink(my_callback)
zk.create_async("/master/node_2",b'c2')
