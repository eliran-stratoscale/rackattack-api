import os
from rackattack.tcp import client


_VAR_NAME = "RACKATTACK_PROVIDER"


def factory():
    if _VAR_NAME not in os.environ:
        raise Exception(
            "The environment variable '%s' must be defined properly" % _VAR_NAME)
    request, subscribe, http = os.environ[_VAR_NAME].split("@@")
    return client.Client(
        providerRequestLocation=request,
        providerSubscribeLocation=subscribe,
        providerHTTPLocation=http)
