from inaugurator.server import config
import logging
import pika
import simplejson


class Publish:
    def __init__(self, amqpURL):
        self._declaredExchanges = set()
        self._amqpURL = amqpURL
        self._connect()

    def allocationChangedState(self, allocationID):
        self._publish(self.allocationExchange(allocationID), dict(
            event='changedState', allocationID=allocationID))

    def allocationProviderMessage(self, allocationID, message):
        self._publish(self.allocationExchange(allocationID), dict(
            event='providerMessage', allocationID=allocationID, message=message))

    def allocationWithdraw(self, allocationID, message):
        self._publish(self.allocationExchange(allocationID), dict(
            event='withdrawn', allocationID=allocationID, message=message))

    def _publish(self, exchange, message):
        if exchange not in self._declaredExchanges:
            self._declaredExchanges.add(exchange)
            self._channel.exchange_declare(exchange=exchange, type='fanout')
        self._channel.basic_publish(exchange=exchange, routing_key='', body=simplejson.dumps(message))

    def _connect(self):
        logging.info("Rackattack event publisher connects to rabbit MQ %(url)s", dict(url=config.AMQP_URL))
        parameters = pika.URLParameters(config.AMQP_URL)
        self._connection = pika.BlockingConnection(parameters)
        self._channel = self._connection.channel()

    @classmethod
    def allocationExchange(cls, id):
        return "allocation_status__%s" % id
