import contextlib
import logging
import time
import os


logger = logging.getLogger('network')


@contextlib.contextmanager
def logNetwork(message):
    transaction = Transaction(message)
    yield
    transaction.finished()


class Transaction:
    TRANSACTION_PERIOD_MAX = 0.3
    def __init__(self, message):
        self._message = message
        self._before = time.time()
        unique = _generateUnique()
        self._uniqueStrRepr = "'%(message)s' unique '%(unique)s'" %  dict(message=self._message,
                unique=unique)
        logger.debug("Starting %(transaction)s", dict(transaction=self._uniqueStrRepr))

    def reportState(self, state):
        logger.debug("%(state)s %(transaction)s", dict(transaction=self._uniqueStrRepr, state=state))

    def finished(self):
        took = time.time() - self._before
        msg = "Finished %(transaction)s took %(took)s" % dict(transaction=self._uniqueStrRepr, took=took)
        logger.debug(msg)
        if took > self.TRANSACTION_PERIOD_MAX:
            logger.error(msg)
            logging.error(msg)


def _generateUnique():
    return os.urandom(10).encode('hex')
