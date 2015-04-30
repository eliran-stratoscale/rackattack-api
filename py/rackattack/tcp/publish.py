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
                finishedEvent, exchange, message = self._queue.get(block=True, timeout=10)
                if exchange not in self._declaredExchanges:
                    self._declaredExchanges.add(exchange)
                    self._channel.exchange_declare(exchange=exchange, type='fanout')
                self._channel.basic_publish(exchange=exchange, routing_key='',
                                            body=simplejson.dumps(message))
                finishedEvent.set()
            except Queue.Empty:
                self._connection.process_data_events()

    def _publish(self, exchange, message):
        finishedEvent = threading.Event()
        self._queue.put((finishedEvent, exchange, message), block=True)
        finishedEvent.wait()

    def _connect(self):
        logging.info("Rackattack event publisher connects to rabbit MQ %(url)s", dict(url=config.AMQP_URL))
        parameters = pika.URLParameters(config.AMQP_URL)
        self._connection = pika.BlockingConnection(parameters)
        self._channel = self._connection.channel()


class Publish(PublishSpooler):
    ALL_HOSTS_ALLOCATIONS_EXCHANGE_NAME = 'allHostsAllocations'

    def __init__(self, amqpURL):
        super(Publish, self).__init__(amqpURL)

    def _allocationEvent(self, event, allocationID, message=None):
        exchange = self.allocationExchange(allocationID)
        self._publish(exchange, dict(event=event, allocationID=allocationID, message=message))

    def allocationChangedState(self, allocationID):
        self._allocationEvent('changedState', allocationID)

    def allocationProviderMessage(self, allocationID, message):
        self._allocationEvent('providerMessage', allocationID, message)

    def allocationWithdraw(self, allocationID, message):
        self._allocationEvent('withdrawn', allocationID, message)

    def allocationRequested(self, requirements, allocationInfo):
        self._publish(self.ALL_HOSTS_ALLOCATIONS_EXCHANGE_NAME, dict(
            event='requested', requirements=requirements, allocationInfo=allocationInfo))

    def allocationCreated(self, allocationID, requirements, allocationInfo, allocated):
        allocatedIDs = {name: stateMachine.hostImplementation().id()
                        for name, stateMachine in allocated.iteritems()}
        self._publish(self.ALL_HOSTS_ALLOCATIONS_EXCHANGE_NAME, dict(
            event='created', allocationID=allocationID, requirements=requirements,
            allocationInfo=allocationInfo, allocated=allocatedIDs))

    @classmethod
    def allocationExchange(cls, id):
        return "allocation_status__%s" % id
