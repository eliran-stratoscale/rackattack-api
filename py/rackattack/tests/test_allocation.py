import mock
import unittest
from rackattack.tcp import publish
from rackattack.tcp import suicide
from rackattack.tcp import allocation
from rackattack.tests import mock_pika
from rackattack.tests import fake_subscribe
from rackattack.tests import one_threaded_publish


class FakeIPCClient:
    def __init__(self, hostsByAllocations):
        self.hostsByAllocations = hostsByAllocations
        self.isAllocationDone = {allocationID: False for allocationID in hostsByAllocations.keys()}
        self.allocationDeath = {allocationID: None for allocationID in hostsByAllocations.keys()}

    def call(self, method, *args, **kwargs):
        method = getattr(self, method)
        return method(*args, **kwargs)

    def allocation__inauguratorsIDs(self, id):
        return self.hostsByAllocations[id]

    def allocation__done(self, id):
        return self.isAllocationDone[id]

    def allocation__dead(self, id):
        return self.allocationDeath[id]


class Suicide(Exception):
    pass


class Test(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.allocationID = 1
        cls.hostsByAllocations = {cls.allocationID: dict(nodeAlpha="serverAlpha",
                                                         nodeBravo="serverBravo",
                                                         nodeCharlie="serverCharlie",
                                                         nodeDelta="serverDelta")}
        cls.requirements = dict(nodeAlpha=None, nodeBravo=None, nodeCharlie=None, nodeDelta=None)
        cls.heartbeat = mock.Mock()
        suicide.killSelf = mock.Mock(side_effect=Suicide())

    @classmethod
    def tearDownClass(cls):
        mock_pika.disableMockedPika(modules=[publish])

    def setUp(self):
        mock_pika.enableMockedPika(modules=[publish])
        self.publish = one_threaded_publish.OneThreadedPublish(mock_pika.DEFAULT_AMQP_URL)
        self.ipcClient = FakeIPCClient(self.hostsByAllocations)
        self.subscribe = fake_subscribe.SubscribeMock(amqpURL=mock_pika.DEFAULT_AMQP_URL)
        self._continueWithServer()
        self.tested = allocation.Allocation(self.allocationID,
                                            self.requirements,
                                            self.ipcClient,
                                            self.subscribe,
                                            self.heartbeat)

    def test_RegisterForAllocation(self):
        self.assertIn(self.allocationID, self.subscribe.allocationsCallbacks)

    def test_ReceiveAllocationDoneMessage(self):
        self.publish.allocationDone(self.allocationID)
        self._continueWithServer()
        self.tested.wait(timeout=0)
        self.ipcClient.isAllocationDone[self.allocationID] = True
        self.assertTrue(self.tested.done())

    def test_AllocationProviderMessage(self):
        self.publish.allocationProviderMessage(self.allocationID, "'sup")
        self._continueWithServer()

    def test_ReceiveAllocationDeathMessage(self):
        self.publish.allocationDied(self.allocationID, reason="freed", message="hi")
        self._continueWithServer()
        self.ipcClient.allocationDeath[self.allocationID] = "freed"
        self.assertRaises(Exception, self.tested.wait, timeout=0)

    def test_ReceiveAllocationWithdrawlMessage(self):
        self.publish.allocationDied(self.allocationID, reason="withdrawn", message="hi")
        self.assertRaises(Suicide, self._continueWithServer)

    def _continueWithServer(self):
        self.publish.continueWithServer()
        self.subscribe.continue_with_thread()

if __name__ == "__main__":
    unittest.main()
