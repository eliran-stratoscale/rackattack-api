import os
from rackattack.tcp import client


_VAR_NAME = "RACKATTACK_PROVIDER"


def factory(connectionString=None):
    if connectionString is None:
        if _VAR_NAME not in os.environ:
            raise Exception(
                "The environment variable '%s' must be defined properly" % _VAR_NAME)
        connectionString = os.environ[_VAR_NAME]
    request, subscribe, http = connectionString.split("@@")
    return client.Client(
        providerRequestLocation=request,
        providerSubscribeLocation=subscribe,
        providerHTTPLocation=http)
