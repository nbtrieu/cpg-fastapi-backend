# put in services
# # Connect to localhost
# from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
# from gremlin_python.process.anonymous_traversal import traversal


# def get_gremlin_client():
#     connection = DriverRemoteConnection("ws://localhost:8183/gremlin", "g")
#     g = traversal().withRemote(connection)
#     return g

# Connect to Neptune
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from gremlin_python.process.anonymous_traversal import traversal
import ssl
import certifi

gremlin_client = None


def init_gremlin_client():
    global gremlin_client
    if gremlin_client is None:
        gremlin_url = "wss://db-bio-annotations.cluster-cu9wyuyqqen8.ap-southeast-1.neptune.amazonaws.com:8182/gremlin"
        ssl_context = ssl.create_default_context(cafile=certifi.where())
        gremlin_client = DriverRemoteConnection(gremlin_url, "g", ssl_context=ssl_context)
        g = traversal().withRemote(gremlin_client)
        return g


def get_gremlin_client():
    if gremlin_client is None:
        init_gremlin_client()
    return traversal().withRemote(gremlin_client)


def close_gremlin_client():
    global gremlin_client
    if gremlin_client:
        gremlin_client.close()
        gremlin_client = None
