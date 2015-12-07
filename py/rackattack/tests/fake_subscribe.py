import json
from rackattack.tcp import publish
from rackattack.tests import mock_pika


class SubscribeMock:
    instances = []

    def __init__(self, amqpURL):
        self.allocationsCallbacks = dict()
        self.allAllocationsCallback = None
        self.inaugurationsCallbacks = dict()
        self.inaugurationsCallbackHistory = list()
        self.instances.append(self)
        self.consumer = mock_pika.getBlockingConnectionToFakeBroker(amqpURL).channel()

    def registerForAllocation(self, id, callback):
        self.allocationsCallbacks[id] = callback

    def registerForAllAllocations(self, callback):
        self.allAllocationsCallback = callback

    def registerForInagurator(self, host_id, callback):
        self.inaugurationsCallbacks[host_id] = callback
        self.inaugurationsCallbackHistory.append(callback)

    def unregisterForInaugurator(self, host_id):
        del self.inaugurationsCallbacks[host_id]

    def continue_with_thread(self):
        pendingMessagesByExchanges = self.consumer.broker.sendOrderByExchanges
        while pendingMessagesByExchanges:
            exchange = pendingMessagesByExchanges[0]
            message = self.consumer.basic_consume(exchange)
            message = json.loads(message)
            if exchange == publish.Publish.ALL_HOSTS_ALLOCATIONS_EXCHANGE_NAME:
                if self.allAllocationsCallback is not None:
                    self.allAllocationsCallback(message)
            elif exchange.startswith("allocation_status__"):
                allocationID = int(exchange.split("__")[1])
                if allocationID in self.allocationsCallbacks:
                    self.allocationsCallbacks[allocationID](message)
