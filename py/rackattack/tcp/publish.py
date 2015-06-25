from inaugurator.server import config
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
        threading.Thread.start(self)

    def run(self):
        self._declaredExchanges = set()
        self._connect()
        while True:
            try:
                finishedEvent, command, kwargs, returnValue = self._queue.get(block=True, timeout=10)
            except Queue.Empty:
                self._connection.process_data_events()
                continue
            try:
                returnValue.data = command(**kwargs)
                self._connection.process_data_events()
            except Exception as e:
                returnValue.exception = e
                logging.exception("Got an exception while handling command.")
            finally:
                finishedEvent.set()

    def _executeCommand(self, function, **kwargs):
        class ReturnValue(object):
            def __init__(self):
                self.data = None
                self.exception = None

        finishedEvent = threading.Event()
        returnValue = ReturnValue()
        self._queue.put((finishedEvent, function, kwargs, returnValue), block=True)
        finishedEvent.wait()
        if returnValue.exception is not None:
            raise returnValue.exception
        return returnValue.data

    def _publish(self, exchange, message):
        if exchange not in self._declaredExchanges:
            self._channel.exchange_declare(exchange=exchange, type='fanout')
            self._declaredExchanges.add(exchange)
        self._channel.basic_publish(exchange=exchange, routing_key='', body=simplejson.dumps(message))

    def _cleanupAllocationPublishResources(self, allocationID):
        exchange = self.allocationExchange(allocationID)
        if exchange not in self._declaredExchanges:
            logger.warn("Tried to delete an unfamiliar exchange %(exchange)s.", dict(excahnge=exchange))
            return
        self._channel.exchange_delete(exchange=exchange)

    def _connect(self):
        logging.info("Rackattack event publisher connects to rabbit MQ %(url)s", dict(url=config.AMQP_URL))
        parameters = pika.URLParameters(config.AMQP_URL)
        self._connection = pika.BlockingConnection(parameters)
        self._channel = self._connection.channel()

    @classmethod
    def allocationExchange(cls, id):
        return "allocation_status__%s" % id


class Publish(PublishSpooler):
    ALL_HOSTS_ALLOCATIONS_EXCHANGE_NAME = 'allHostsAllocations'

    def __init__(self, amqpURL):
        super(Publish, self).__init__(amqpURL)

    def allocationChangedState(self, allocationID):
        self._publishAllocationStatus('changedState', allocationID)

    def cleanupAllocationPublishResources(self, allocationID):
        self._executeCommand(self._cleanupAllocationPublishResources, allocationID=allocationID)

    def allocationProviderMessage(self, allocationID, message):
        self._publishAllocationStatus('providerMessage', allocationID, message)

    def allocationWithdraw(self, allocationID, message):
        self._publishAllocationStatus('withdrawn', allocationID, message)

    def allocationRequested(self, requirements, allocationInfo):
        self._publishMessage(self.ALL_HOSTS_ALLOCATIONS_EXCHANGE_NAME,
                             event='requested', requirements=requirements, allocationInfo=allocationInfo)

    def allocationCreated(self, allocationID, requirements, allocationInfo, allocated):
        allocatedIDs = {name: stateMachine.hostImplementation().id()
                        for name, stateMachine in allocated.iteritems()}
        self._publishMessage(self.ALL_HOSTS_ALLOCATIONS_EXCHANGE_NAME,
                             event='created', allocationID=allocationID, requirements=requirements,
                             allocationInfo=allocationInfo, allocated=allocatedIDs)

    def _publishMessage(self, exchange, **message):
        self._executeCommand(self._publish, exchange=exchange, message=message)

    def _publishAllocationStatus(self, event, allocationID, message=None):
        exchange = self.allocationExchange(allocationID)
        self._publishMessage(exchange, event=event, allocationID=allocationID, message=message)
