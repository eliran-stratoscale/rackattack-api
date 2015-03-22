import threading
import logging
import pika
import simplejson
from rackattack.tcp import suicide
from rackattack.tcp import publish

_logger = logging.getLogger(__name__)


class Subscribe(threading.Thread):
    def __init__(self, amqpURL):
        self._connection = None
        self._channel = None
        self._amqpURL = amqpURL
        self._registered = dict()
        self._readyEvent = threading.Event()
        self._closed = False
        threading.Thread.__init__(self)
        self.daemon = True
        threading.Thread.start(self)

    def registerForInagurator(self, id, callback):
        exchange = self._exchangeForInaugurator(id)
        assert exchange not in self._registered
        self._listen(exchange, callback)

    def unregisterForInaugurator(self, id):
        exchange = self._exchangeForInaugurator(id)
        assert exchange in self._registered
        self._channel.basic_cancel(lambda *args: None, self._registered[exchange])
        del self._registered[exchange]

    def registerForAllocation(self, id, callback):
        exchange = publish.Publish.allocationExchange(id)
        assert exchange not in self._registered
        self._listen(exchange, callback)

    def unregisterForAllocation(self, id):
        exchange = publish.Publish.allocationExchange(id)
        assert exchange in self._registered
        self._channel.basic_cancel(lambda *args: None, self._registered[exchange])
        del self._registered[exchange]

    def close(self):
        _logger.info("Closing connection")
        self._closed = True
        self._channel.close()
        self._connection.close()

    def _exchangeForInaugurator(self, id):
        return "inaugurator_status__%s" % id

    def _onConnectionClosed(self, connection, reply_code, reply_text):
        self._channel = None
        if self._closed:
            self._connection.ioloop.stop()
        else:
            _logger.error("Connection closed, committing suicide: %(replyCode)s %(replyText)s", dict(
                replyCode=reply_code, replyText=reply_text))
            suicide.killSelf()

    def _onConnectionOpen(self, unused_connection):
        _logger.info('Connection opened')
        self._connection.add_on_close_callback(self._onConnectionClosed)
        self._connection.channel(on_open_callback=self._onChannelOpen)

    def _onChannelClosed(self, channel, reply_code, reply_text):
        _logger.error('Channel %(channel)i was closed: (%(replyCode)s) %(replyText)s', dict(
            channel=channel, replyCode=reply_code, replyText=reply_text))
        self._connection.close()

    def _onChannelOpen(self, channel):
        self._channel = channel
        self._channel.add_on_close_callback(self._onChannelClosed)
        self._readyEvent.set()

    def _listen(self, exchangeName, callback):
        def onMessage(channel, basicDeliver, properties, body):
            self._channel.basic_ack(basicDeliver.delivery_tag)
            callback(simplejson.loads(body))

        def onBind(queueName, consumerTag):
            self._channel.basic_consume(onMessage, queueName, consumer_tag=consumerTag)
            _logger.debug("Listening on exchange %(exchange)s", dict(exchange=exchangeName))

        def onQueueDeclared(methodFrame):
            queue = methodFrame.method.queue
            consumerTag = queue + "__consumer"
            self._registered[exchangeName] = consumerTag
            self._channel.queue_bind(
                lambda *args: onBind(queue, consumerTag), queue, exchangeName, routing_key='')

        def onExchangeDeclared(frame):
            self._channel.queue_declare(callback=onQueueDeclared, exclusive=True)

        self._readyEvent.wait(5)
        if not self._readyEvent.isSet():
            raise Exception("Timeout waiting for subscriber to connect to rabbitMQ")
        self._channel.exchange_declare(onExchangeDeclared, exchangeName, 'fanout')

    def run(self):
        _logger.info('Connecting to %(amqpURL)s', dict(amqpURL=self._amqpURL))
        self._connection = pika.SelectConnection(
            pika.URLParameters(self._amqpURL),
            self._onConnectionOpen,
            stop_ioloop_on_close=False)
        self._connection.ioloop.start()
