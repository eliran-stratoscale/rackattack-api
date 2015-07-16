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
from inaugurator.server import config


handler = logging.StreamHandler()
handler.setLevel(logging.INFO)
logging.getLogger().addHandler(handler)
logging.getLogger().setLevel(logging.INFO)


rabbitMQBrokers = {}


class RabbitMQBroker(object):
    def __init__(self):
        self.exchanges = dict()
        # The following is RabbitMQ's default exchange
        self.exchanges[''] = ExchangeMock(type='fanout')

    @classmethod
    def _brokerKeyByURL(cls, url):
        return "%(host)s:%(port)s" % dict(host=url.host, port=url.port)

    @classmethod
    def startFakeRabbitMQBroker(cls, url):
        global rabbitMQBrokers
        rabbitMQBrokers[cls._brokerKeyByURL(url)] = RabbitMQBroker()

    @classmethod
    def getBrokerInstanceFromURL(cls, url):
        global rabbitMQBrokers
        return rabbitMQBrokers[cls._brokerKeyByURL(url)]


class ExchangeMock(object):
    def __init__(self, type):
        if type != 'fanout':
            raise ValueError("Mock does not support exchange types other than 'fanout'")
        self.exchange_type = type
        self.messages = Queue.Queue()

    def publish(self, message):
        self.messages.put(message, block=True)

    def consume(self):
        return self.messages.get(block=True)


class BlockingChannelMock(object):
    def __init__(self):
        self._broker = None
        self.exchanges = None

    def setBroker(self, broker):
        self.exchanges = broker.exchanges

    def exchange_declare(self, exchange, type):
        if exchange in self.exchanges:
            existing = self.exchanges[exchange]
            self._validateExchangePropertyIsEqual(type, existing.type)
        self.exchanges[exchange] = ExchangeMock(type=type)

    def exchange_delete(self, exchange):
        assert exchange is not None
        del self.exchanges[exchange]

    def basic_publish(self, exchange, routing_key, body):
        if exchange not in self.exchanges:
            raise ValueError(exchange, "Not a declared exchange")
        self.exchanges[exchange].publish(body)

    def basic_consume(self, exchange):
        return self.exchanges[exchange].consume()

    def _validateExchangePropertyIsTheSame(self, actual, expected):
        if actual != expected:
            raise pika.ChannelClosed()


class BlockingConnectionMock(object):
    def __init__(self, urlParams):
        global rabbitmqBrokers
        try:
            self._broker = RabbitMQBroker.getBrokerInstanceFromURL(urlParams)
        except KeyError:
            raise pika.exceptions.AMQPConnectionError()

    def channel(self):
        channel = BlockingChannelMock()
        channel.setBroker(self._broker)
        return channel

    def process_data_events(self):
        pass


class Test(unittest.TestCase):
    def setUp(self):
        pika.BlockingConnection = BlockingConnectionMock
        pika.channel = BlockingChannelMock
        RabbitMQBroker.startFakeRabbitMQBroker(pika.URLParameters(config.AMQP_URL))
        self.threadStartMock = mock.Mock()

        def waitWrapper(self):
            return greenlet.getcurrent().parent.switch()

        threading._Event.wait = waitWrapper
        self.tested = self._generatePublishInstance(config.AMQP_URL)
        self.testedServerContext = greenlet.greenlet(self.tested.run)
        self.consumer = self._generateRabbitMQConsumer(config.AMQP_URL)

    def test_ThreadStarted(self):
        self.threadStartMock.assert_called_once_with(self.tested)

    def test_BadURL(self):
        killMock = mock.Mock()
        origKill = os.kill
        try:
            os.kill = killMock
            publishInstance = greenlet.greenlet(self._generatePublishInstance('invalid amqp url').run)
            publishInstance.switch()
        finally:
            os.kill = origKill
        self.assertEquals(killMock.call_count, 1)

    def test_AllocationChangedState(self):
        self._continueWithServer()
        allocationID = 1
        client = greenlet.greenlet(lambda: self.tested.allocationChangedState(allocationID))
        client.switch()
        self._continueWithServer()
        client.switch()
        expectedMessage = dict(event='changedState', allocationID=allocationID, message=None)
        expectedExchange = publish.PublishSpooler.allocationExchange(allocationID)
        actualMessage = self._consume(expectedExchange)
        self.assertEquals(actualMessage, expectedMessage)

    def test_AllocationProviderMessage(self):
        self._continueWithServer()
        allocationID = 2
        message = 'alpha bravo tango'
        client = greenlet.greenlet(lambda: self.tested.allocationProviderMessage(allocationID, message))
        client.switch()
        self._continueWithServer()
        client.switch()
        expectedMessage = dict(event='providerMessage', allocationID=allocationID, message=message)
        expectedExchange = publish.PublishSpooler.allocationExchange(allocationID)
        actualMessage = self._consume(expectedExchange)
        self.assertEquals(actualMessage, expectedMessage)

    def test_AllocationWithdraw(self):
        self._continueWithServer()
        allocationID = 2
        message = 'echo foxtrot golf'
        client = greenlet.greenlet(lambda: self.tested.allocationWithdraw(allocationID, message))
        client.switch()
        self._continueWithServer()
        client.switch()
        expectedMessage = dict(event='withdrawn', allocationID=allocationID, message=message)
        expectedExchange = publish.PublishSpooler.allocationExchange(allocationID)
        actualMessage = self._consume(expectedExchange)
        self.assertEquals(actualMessage, expectedMessage)

    def test_CleanupAllocationPublishResources(self):
        self._continueWithServer()
        allocationID = 1
        client = greenlet.greenlet(lambda: self.tested.allocationChangedState(allocationID))
        client.switch()
        self._continueWithServer()
        client.switch()
        expectedMessage = dict(event='changedState', allocationID=allocationID, message=None)
        expectedExchange = publish.PublishSpooler.allocationExchange(allocationID)
        actualMessage = self._consume(expectedExchange)
        self.assertEquals(actualMessage, expectedMessage)
        self.assertEquals(len(self.consumer.exchanges), 2)
        self.assertIn('', self.consumer.exchanges)
        self.assertIn(expectedExchange, self.consumer.exchanges)
        client = greenlet.greenlet(lambda: self.tested.cleanupAllocationPublishResources(allocationID))
        client.switch()
        self._continueWithServer()
        client.switch()
        self.assertEquals(self.consumer.exchanges.keys(), [''])

    def test_TryCleaningUpResourcesForANonExistingAllocationDoesNotCrashServer(self):
        self._continueWithServer()
        allocationID = 1
        client = greenlet.greenlet(lambda: self.tested.cleanupAllocationPublishResources(allocationID))
        client.switch()
        self._continueWithServer()
        client.switch()

    def test_TryCleaningUpResourcesForAnAlreadyDeadAllocationDoesNotCrashServer(self):
        self._continueWithServer()
        allocationID = 1
        client = greenlet.greenlet(lambda: self.tested.allocationChangedState(allocationID))
        client.switch()
        self._continueWithServer()
        client.switch()
        expectedMessage = dict(event='changedState', allocationID=allocationID, message=None)
        expectedExchange = publish.PublishSpooler.allocationExchange(allocationID)
        actualMessage = self._consume(expectedExchange)
        self.assertEquals(actualMessage, expectedMessage)
        self.assertEquals(len(self.consumer.exchanges), 2)
        self.assertIn('', self.consumer.exchanges)
        self.assertIn(expectedExchange, self.consumer.exchanges)
        client = greenlet.greenlet(lambda: self.tested.cleanupAllocationPublishResources(allocationID))
        client.switch()
        self._continueWithServer()
        client.switch()
        self.assertEquals(self.consumer.exchanges.keys(), [''])
        client = greenlet.greenlet(lambda: self.tested.cleanupAllocationPublishResources(allocationID))
        client.switch()
        self._continueWithServer()
        client.switch()
        self.assertEquals(self.consumer.exchanges.keys(), [''])

    def _continueWithServer(self):
        if self.tested._queue.qsize() > 0:
            item = self.originalGet(block=True, timeout=10)
            self.testedServerContext.switch(item)
        else:
            self.testedServerContext.switch()

    def _consume(self, exchange):
        message = self.consumer.basic_consume(exchange=exchange)
        return json.loads(message)

    def _generateRabbitMQConsumer(self, amqpURL):
        return BlockingConnectionMock(pika.URLParameters(amqpURL)).channel()

    def _generatePublishInstance(self, amqpURL):
        origStart = threading.Thread.start
        threading.Thread.start = self.threadStartMock
        publishInstance = publish.Publish(amqpURL)
        threading.Thread.start = origStart

        def queueGetWrapper(*args, **kwargs):
            item = greenlet.getcurrent().parent.switch()
            return item

        self.originalGet = publishInstance._queue.get
        publishInstance._queue.get = queueGetWrapper
        return publishInstance

if __name__ == '__main__':
    unittest.main()
