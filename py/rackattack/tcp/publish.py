import os
import signal
import logging
import pika
import simplejson
import threading
import Queue


class PublishSpooler(threading.Thread):
    def __init__(self, amqpURL):
        super(PublishSpooler, self).__init__()
        self.daemon = True
        self._queue = Queue.Queue()
        self._amqpURL = amqpURL
        self._declaredExchanges = set()
        threading.Thread.start(self)

    def run(self):
        try:
            self._connect()
        except:
            logging.exception("Could not connect to RabbitMQ broker. Commiting suicide.")
            os.kill(os.getpid(), signal.SIGTERM)
        while True:
            try:
                command, kwargs = self._queue.get(block=True, timeout=10)
            except Queue.Empty:
                self._connection.process_data_events()
                continue
            try:
                command(**kwargs)
            except Exception as e:
                logging.exception("Got an exception while handling command.")

    def _executeCommand(self, function, **kwargs):
        self._queue.put((function, kwargs), block=True)

    def _publish(self, exchange, message):
        if exchange not in self._declaredExchanges:
            self._channel.exchange_declare(exchange=exchange, type='fanout')
            self._declaredExchanges.add(exchange)
        self._channel.basic_publish(exchange=exchange, routing_key='', body=simplejson.dumps(message))

    def _cleanupAllocationPublishResources(self, allocationID):
        exchange = self.allocationExchange(allocationID)
        if exchange not in self._declaredExchanges:
            logging.warn("Tried to delete an unfamiliar exchange %(exchange)s.", dict(exchange=exchange))
            return
        self._declaredExchanges.remove(exchange)
        self._channel.exchange_delete(exchange=exchange)

    def _connect(self):
        logging.info("Rackattack event publisher connects to rabbit MQ %(url)s", dict(url=self._amqpURL))
        parameters = pika.URLParameters(self._amqpURL)
        self._connection = pika.BlockingConnection(parameters)
        self._channel = self._connection.channel()

    @classmethod
    def allocationExchange(cls, id):
        return "allocation_status__%s" % id


class Publish(PublishSpooler):
    ALL_HOSTS_ALLOCATIONS_EXCHANGE_NAME = 'allHostsAllocations'

    def __init__(self, amqpURL):
        super(Publish, self).__init__(amqpURL)

    def cleanupAllocationPublishResources(self, allocationID):
        self._executeCommand(self._cleanupAllocationPublishResources, allocationID=allocationID)

    def allocationProviderMessage(self, allocationID, message):
        self._publishAllocationStatus('providerMessage', allocationID, message)

    def allocationRequested(self, requirements, allocationInfo):
        self._publishMessage(self.ALL_HOSTS_ALLOCATIONS_EXCHANGE_NAME,
                             event='requested', requirements=requirements, allocationInfo=allocationInfo)

    def allocationRejected(self, reason):
        self._publishMessage(self.ALL_HOSTS_ALLOCATIONS_EXCHANGE_NAME, event='rejected', reason=reason)

    def allocationCreated(self, allocationID, allocated):
        allocatedIDs = {name: stateMachine.hostImplementation().id()
                        for name, stateMachine in allocated.iteritems()}
        self._publishMessage(self.ALL_HOSTS_ALLOCATIONS_EXCHANGE_NAME, event='created',
                             allocationID=allocationID, allocated=allocatedIDs)

    def allocationDone(self, allocationID):
        self._publishAllocationStatus('changedState', allocationID)
        message = dict(event="done", allocationID=allocationID)
        self._publishMessage(self.ALL_HOSTS_ALLOCATIONS_EXCHANGE_NAME, **message)

    def allocationDied(self, allocationID, reason, message=None):
        if reason == "withdrawn":
            self._publishAllocationStatus(reason, allocationID, message)
        else:
            self._publishAllocationStatus('changedState', allocationID)
        message = dict(event="dead", allocationID=allocationID, reason=reason, moreInfo=message)
        self._publishMessage(self.ALL_HOSTS_ALLOCATIONS_EXCHANGE_NAME, **message)

    def _publishMessage(self, exchange, **message):
        self._executeCommand(self._publish, exchange=exchange, message=message)

    def _publishAllocationStatus(self, event, allocationID, message=None):
        exchange = self.allocationExchange(allocationID)
        self._publishMessage(exchange, event=event, allocationID=allocationID, message=message)
