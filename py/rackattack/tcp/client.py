from rackattack import api
import threading
from rackattack.tcp import allocation
from rackattack.tcp import heartbeat
from rackattack.tcp import subscribe
from rackattack.tcp import suicide
from rackattack.tcp import transport
import urllib2


class Client(api.Client):
    def __init__(self,
                 providerRequestLocation,
                 providerSubscribeLocation,
                 providerHTTPLocation):
        self._providerHTTPLocation = providerHTTPLocation
        self._request = transport.Transport(providerRequestLocation)
        self._lock = threading.Lock()
        self._closed = False
        self._activeAllocations = []
        self.call("handshake", versionInfo=dict(RACKATTACK_VERSION=api.VERSION))
        self._subscribe = subscribe.Subscribe(amqpURL=providerSubscribeLocation)
        self._connectionToProviderInterrupted = suicide.killSelf
        self._heartbeat = heartbeat.HeartBeat(self)

    def allocate(self, requirements, allocationInfo):
        assert len(requirements) > 0
        jsonableRequirements = {
            name: requirement.__dict__ for name, requirement in requirements.iteritems()}
        allocationID = self.call(
            cmd='allocate',
            requirements=jsonableRequirements,
            allocationInfo=allocationInfo.__dict__)
        allocationInstance = allocation.Allocation(
            id=allocationID, requirements=requirements, ipcClient=self,
            subscribe=self._subscribe, heartbeat=self._heartbeat)
        self._activeAllocations.append(allocationInstance)
        return allocationInstance

    def call(self, cmd, ipcTimeout=50, ** kwargs):
        try:
            with self._lock:
                if self._closed:
                    raise Exception("Already closed")
                return self._call(cmd, ipcTimeout, kwargs)
        except:
            self._notifyAllActiveAllocationsThatConnectionToProviderInterrupted()
            raise

    def urlopen(self, path):
        url = self._providerHTTPLocation.rstrip("/") + "/" + path.lstrip("/")
        return urllib2.urlopen(url)

    def _call(self, cmd, ipcTimeout, arguments):
        self._request.sendJSON(dict(cmd=cmd, arguments=arguments))
        result = self._request.receiveJSON(timeout=ipcTimeout)
        if isinstance(result, dict) and 'exceptionType' in result:
            if result['exceptionType'] == 'NotEnoughResourcesForAllocation':
                raise api.NotEnoughResourcesForAllocation(result['exceptionString'])
            else:
                raise Exception("IPC command '%s' failed: %s: '%s'" % (
                    cmd, result['exceptionType'], result['exceptionString']))
        return result

    def close(self):
        with self._lock:
            assert len(self._activeAllocations) == 0
            self._closeLocked()

    def _closeLocked(self):
        if self._closed:
            return
        self._closed = True
        if hasattr(self, '_subscribe'):
            self._subscribe.close()
        self._request.close()

    def heartbeatFailed(self):
        self._notifyAllActiveAllocationsThatConnectionToProviderInterrupted()
        self.close()
        self._connectionToProviderInterrupted()

    def _notifyAllActiveAllocationsThatConnectionToProviderInterrupted(self):
        for allocationInstance in list(self._activeAllocations):
            allocationInstance.connectionToProviderInterrupted()
        assert len(self._activeAllocations) == 0

    def setConnectionToProviderInterruptedCallback(self, callback):
        self._connectionToProviderInterrupted = callback

    def allocationClosed(self, allocationInstance):
        self._activeAllocations.remove(allocationInstance)
