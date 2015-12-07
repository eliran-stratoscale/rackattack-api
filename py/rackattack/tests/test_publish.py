import os
import mock
import json
import pika
import Queue
import logging
import greenlet
import unittest
import threading
from rackattack.tcp import publish
from rackattack.tcp import subscribe
from rackattack.tests import mock_pika
from rackattack.tests import one_threaded_publish


handler = logging.StreamHandler()
handler.setLevel(logging.INFO)
logging.getLogger().addHandler(handler)
logging.getLogger().setLevel(logging.INFO)


class Host:
    def id(self):
        return "myID"


class FakeHostStateMachine:
    def hostImplementation(self):
        return Host()


class Test(unittest.TestCase):
    def setUp(self):
        mock_pika.enableMockedPika(modules=[publish])
        self.tested = one_threaded_publish.OneThreadedPublish(mock_pika.DEFAULT_AMQP_URL)
        self.consumer = mock_pika.getBlockingConnectionToFakeBroker().channel()

    def tearDown(self):
        mock_pika.disableMockedPika(modules=[publish])

    def test_ThreadStarted(self):
        self.tested.threadStartMock.assert_called_once_with(self.tested)

    def test_BadURL(self):
        killMock = mock.Mock()
        origKill = os.kill
        try:
            os.kill = killMock
            publishInstance = greenlet.greenlet(
                one_threaded_publish.OneThreadedPublish('invalid amqp url').run)
            publishInstance.switch()
        finally:
            os.kill = origKill
        self.assertEquals(killMock.call_count, 1)

    def test_AllocationProviderMessage(self):
        self.tested.continueWithServer()
        allocationID = 2
        message = 'alpha bravo tango'
        self.tested.allocationProviderMessage(allocationID, message)
        self.tested.continueWithServer()
        expectedMessage = dict(event='providerMessage', allocationID=allocationID, message=message)
        expectedExchange = publish.PublishSpooler.allocationExchange(allocationID)
        actualMessage = self._consume(expectedExchange)
        self.assertEquals(actualMessage, expectedMessage)

    def test_AllocationRequested(self):
        self.tested.continueWithServer()
        allocationID = 1
        fields = dict(requirements=dict(theseAre="myRequirements"),
                      allocationInfo="whatACoolAllocation")
        self.tested.allocationRequested(**fields)
        self.tested.continueWithServer()
        expectedMessage = dict(fields, event='requested')
        expectedExchange = publish.Publish.ALL_HOSTS_ALLOCATIONS_EXCHANGE_NAME
        actualMessage = self._consume(expectedExchange)
        self.assertEquals(actualMessage, expectedMessage)

    def test_AllocationRejected(self):
        self.tested.continueWithServer()
        allocationID = 1
        self.tested.allocationRejected(reason="No Resources")
        self.tested.continueWithServer()
        expectedMessage = dict(event='rejected', reason="No Resources")
        expectedExchange = publish.Publish.ALL_HOSTS_ALLOCATIONS_EXCHANGE_NAME
        actualMessage = self._consume(expectedExchange)
        self.assertEquals(actualMessage, expectedMessage)

    def test_AllocationCreated(self):
        self.tested.continueWithServer()
        allocationID = 1
        hostStateMachine = FakeHostStateMachine()
        allocated = {"node0": hostStateMachine}
        self.tested.allocationCreated(allocationID=allocationID, allocated=allocated)
        self.tested.continueWithServer()
        expectedAllocated = {"node0": hostStateMachine.hostImplementation().id()}
        expectedMessage = dict(event='created', allocated=expectedAllocated, allocationID=allocationID)
        expectedExchange = publish.Publish.ALL_HOSTS_ALLOCATIONS_EXCHANGE_NAME
        actualMessage = self._consume(expectedExchange)
        self.assertEquals(actualMessage, expectedMessage)

    def test_AllocationDone(self):
        self.tested.continueWithServer()
        allocationID = 1
        self.tested.allocationDone(allocationID)
        self.tested.continueWithServer()
        expectedExchange = publish.PublishSpooler.allocationExchange(allocationID)
        message = self._consume(expectedExchange)
        self.assertEquals(message["event"], "changedState")
        expectedMessage = dict(event="done", allocationID=allocationID)
        expectedExchange = publish.Publish.ALL_HOSTS_ALLOCATIONS_EXCHANGE_NAME
        actualMessage = self._consume(expectedExchange)
        self.assertEquals(actualMessage, expectedMessage)

    def test_AllocationDied_NoWithdrawl(self):
        self.tested.continueWithServer()
        allocationID = 1
        client = self.tested.allocationDied(allocationID=allocationID, reason="freed", message="'sup")
        self.tested.continueWithServer()
        expectedExchange = publish.PublishSpooler.allocationExchange(allocationID)
        message = self._consume(expectedExchange)
        self.assertEquals(message["event"], "changedState")
        expectedMessage = dict(event="dead", allocationID=allocationID, reason="freed", moreInfo="'sup")
        expectedExchange = publish.Publish.ALL_HOSTS_ALLOCATIONS_EXCHANGE_NAME
        actualMessage = self._consume(expectedExchange)
        self.assertEquals(actualMessage, expectedMessage)

    def test_AllocationDied_Withdrawn(self):
        self.tested.continueWithServer()
        allocationID = 1
        self.tested.allocationDied(allocationID=allocationID, reason="withdrawn", message="'sup")
        self.tested.continueWithServer()
        expectedExchange = publish.PublishSpooler.allocationExchange(allocationID)
        message = self._consume(expectedExchange)
        self.assertEquals(message["event"], "withdrawn")
        expectedMessage = dict(event="dead", allocationID=allocationID, reason="withdrawn", moreInfo="'sup")
        expectedExchange = publish.Publish.ALL_HOSTS_ALLOCATIONS_EXCHANGE_NAME
        actualMessage = self._consume(expectedExchange)
        self.assertEquals(actualMessage, expectedMessage)

    def test_CleanupAllocationPublishResources(self):
        self.tested.continueWithServer()
        allocationID = 1
        self.tested.allocationDone(allocationID)
        self.tested.continueWithServer()
        allocationExchange = publish.PublishSpooler.allocationExchange(allocationID)
        self._consume(allocationExchange)
        expectedExchanges = ['',
                             allocationExchange,
                             publish.Publish.ALL_HOSTS_ALLOCATIONS_EXCHANGE_NAME]
        for expectedExchange in expectedExchanges:
            self.assertIn(expectedExchange, self.consumer.exchanges)
        self.assertIn(expectedExchange, self.consumer.exchanges)
        self.tested.cleanupAllocationPublishResources(allocationID)
        self.tested.continueWithServer()
        expectedExchanges = ['', publish.Publish.ALL_HOSTS_ALLOCATIONS_EXCHANGE_NAME]
        self.assertEquals(self.consumer.exchanges.keys(), expectedExchanges)

    def test_TryCleaningUpResourcesForANonExistingAllocationDoesNotCrashServer(self):
        self.tested.continueWithServer()
        allocationID = 1
        self.tested.cleanupAllocationPublishResources(allocationID)
        self.tested.continueWithServer()

    def test_CleaningUpResourcesForADeadAllocationDoesNotCrashServer(self):
        self.tested.continueWithServer()
        allocationID = 1
        self.tested.allocationDone(allocationID)
        self.tested.continueWithServer()
        allocationExchange = publish.PublishSpooler.allocationExchange(allocationID)
        self._consume(allocationExchange)
        expectedExchanges = ['',
                             allocationExchange,
                             publish.Publish.ALL_HOSTS_ALLOCATIONS_EXCHANGE_NAME]
        for expectedExchange in expectedExchanges:
            self.assertIn(expectedExchange, self.consumer.exchanges)
        self.assertIn(expectedExchange, self.consumer.exchanges)
        self.tested.cleanupAllocationPublishResources(allocationID)
        self.tested.continueWithServer()
        expectedExchanges.remove(allocationExchange)
        self.assertEquals(self.consumer.exchanges.keys(), expectedExchanges)
        self.tested.cleanupAllocationPublishResources(allocationID)
        self.tested.continueWithServer()
        self.assertEquals(self.consumer.exchanges.keys(), expectedExchanges)

    def _consume(self, exchange):
        message = self.consumer.basic_consume(exchange=exchange)
        return json.loads(message)

if __name__ == '__main__':
    unittest.main()
