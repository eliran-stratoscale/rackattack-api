from rackattack import api


class Node(api.Node):
    def __init__(self, ipcClient, allocation, name, info):
        assert 'id' in info
        assert 'primaryMACAddress' in info
        assert 'secondaryMACAddress' in info
        assert 'ipAddress' in info
        self._ipcClient = ipcClient
        self._allocation = allocation
        self._name = name
        self._info = info
        self._id = info['id']

    def rootSSHCredentials(self):
        return self._ipcClient.call(
            "node__rootSSHCredentials", allocationID=self._allocation._idForNodeIPC(), nodeID=self._id)

    def id(self):
        return self._id

    def name(self):
        return self._name

    def primaryMACAddress(self):
        return self._info['primaryMACAddress']

    def secondaryMACAddress(self):
        return self._info['secondaryMACAddress']

    def NICBondingPairs(self):
        return self._info['NICBondingPairs']

    def getMacAddress(self, macName):
        return self._info[macName]

    def ipAddress(self):
        return self._info['ipAddress']

    def coldRestart(self):
        return self._ipcClient.call(
            'node__coldRestart', allocationID=self._allocation._idForNodeIPC(), nodeID=self._id)

    def fetchSerialLog(self):
        connection = self._ipcClient.urlopen("/host/%s/serialLog" % self._id)
        try:
            return connection.read()
        finally:
            connection.close()

    def networkInfo(self):
        return self._info

    def answerDHCP(self, shouldAnswer):
        return self._ipcClient.call(
            'node__answerDHCP', allocationID=self._allocation._idForNodeIPC(),
            nodeID=self._id, shouldAnswer=shouldAnswer)
