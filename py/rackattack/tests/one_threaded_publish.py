import mock
import greenlet
import threading
from rackattack.tcp import publish


class OneThreadedPublish(publish.Publish):
    def __init__(self, amqpURL):
        self.threadStartMock = mock.Mock()

        threading.Thread.__init__ = mock.Mock()
        threading.Thread.daemon = mock.Mock()
        threading.Thread.Event = mock.Mock()

        publish.threading.Thread.__init__ = mock.Mock()
        publish.threading.Thread.start = self.threadStartMock
        publish.threading.Thread.daemon = mock.Mock()
        publish.threading.Thread.Event = mock.Mock()

        def waitWrapper(self):
            return greenlet.getcurrent().parent.switch()

        origWait = threading._Event.wait
        threading._Event.wait = waitWrapper
        super(OneThreadedPublish, self).__init__(amqpURL)
        self.testedServerContext = greenlet.greenlet(self.run)
        self.hasServerContestStarted = False

        def queueGetWrapper(*args, **kwargs):
            item = greenlet.getcurrent().parent.switch()
            return item

        self.originalGet = self._queue.get
        self._queue.get = queueGetWrapper
        threading._Event.wait = origWait

    def continueWithServer(self):
        if self.hasServerContestStarted:
            while self._queue.qsize() > 0:
                item = self.originalGet(block=False)
                self.testedServerContext.switch(item)
        else:
            self.hasServerContestStarted = True
            self.testedServerContext.switch()
