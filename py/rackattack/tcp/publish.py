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
    def __init__(self, amqpURL):
        super(Publish, self).__init__(amqpURL)

    def allocationChangedState(self, allocationID):
        exchange = self.allocationExchange(allocationID)
        self._publish(exchange, dict(event='changedState', allocationID=allocationID))

    def allocationProviderMessage(self, allocationID, message):
        exchange = self.allocationExchange(allocationID)
        self._publish(exchange, dict(event='providerMessage', allocationID=allocationID,
                                     message=message))

    def allocationWithdraw(self, allocationID, message):
        exchange = self.allocationExchange(allocationID)
        self._publish(exchange, dict(event='withdrawn', allocationID=allocationID,
                                     message=message))

    @classmethod
    def allocationExchange(cls, id):
        return "allocation_status__%s" % id
