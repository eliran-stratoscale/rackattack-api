import pika
from rackattack.tests import fake_pika


origBlockingConnection = pika.BlockingConnection
origChannel = pika.channel


DEFAULT_AMQP_URL = "amqp://guest:guest@localhost:9999/%2F"


def enableMockedPika(modules, fakeBrokersURLs=None):
    if fakeBrokersURLs is None:
        fakeBrokersURLs = [DEFAULT_AMQP_URL]
    for module in modules:
        module.pika.BlockingConnection = fake_pika.BlockingConnectionMock
        module.pika.channel = fake_pika.BlockingChannelMock
    fake_pika.removeAllBrokers()
    for fakeBrokerURL in fakeBrokersURLs:
        fake_pika.startFakeRabbitMQBroker(pika.URLParameters(fakeBrokerURL))


def disableMockedPika(modules):
    for module in modules:
        module.pika.BlockingConnection = origBlockingConnection
        module.pika.Channel = origChannel
    fake_pika.removeAllBrokers()


def getBlockingConnectionToFakeBroker(brokerURL=DEFAULT_AMQP_URL):
    return fake_pika.BlockingConnectionMock(pika.URLParameters(brokerURL))
