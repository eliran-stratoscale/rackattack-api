import logging
import unittest
import subprocess
import os
import test
import time
import contextlib
import socket
import threading
import thread
from rackattack import clientfactory
from rackattack.ssh import connection
from rackattack import api
from rackattack.tcp import transport
from rackattack.tcp import subscribe


SIMPLE_USE_CASE = os.path.join(os.path.dirname(test.__file__), "simpleusecase.py")
HINT = 'rootfs-basic'
LABEL = subprocess.check_output([
    "solvent", "printlabel", "--product", "rootfs", "--repositoryBasename",
    "rootfs-basic"]).strip()
TCPSERVER_SENDMESSAGE = os.path.join(os.path.dirname(__file__), "tcpserver_sendmessage.py")
TCPSERVER_READEVERYTHING = os.path.join(os.path.dirname(__file__), "tcpserver_readeverything.py")


logging.getLogger('pika').setLevel(logging.INFO)
connection.discardParamikoLogs()


class Test(unittest.TestCase):
    def test_singleNodeAllocation(self):
        popen = subprocess.Popen(["python", SIMPLE_USE_CASE], stdin=subprocess.PIPE)
        popen.stdin.write("c\n")
        popen.stdin.close()
        result = popen.wait()
        if result != 0:
            raise Exception("simpleusecase.py failed %d" % result)

    def test_singleNodeAllocation_PDBDoesNotCauseAllocationToDie(self):
        popen = subprocess.Popen(["python", SIMPLE_USE_CASE], stdin=subprocess.PIPE)
        print "Sleeping for 180 seconds, to make sure heartbeat timeout occours, if pdb stops"
        time.sleep(180)
        print "Done Sleeping for 180 seconds, resuming PDB"
        popen.stdin.write("c\n")
        popen.stdin.close()
        result = popen.wait()
        if result != 0:
            raise Exception("simpleusecase.py failed %d" % result)

    def test_LocalForwardingTunnel(self):
        with self._allocateOne() as (node, ssh, allocation):
            ssh.ftp.putFile("/tmp/server.py", TCPSERVER_SENDMESSAGE)
            ssh.run.backgroundScript("python /tmp/server.py 7788 hello")
            port = ssh.tunnel.localForward(7788)
            port2 = ssh.tunnel.localForward(7789)
            port3 = ssh.tunnel.localForward(7787)
            self.assertEquals(ssh.tunnel.localForwardMap()[7788], port)
            self.assertEquals(ssh.tunnel.localForwardMap()[7789], port2)
            self.assertEquals(ssh.tunnel.localForwardMap()[7787], port3)
            self.assertIn('hello', self._receiveAll(port))
            self.assertIn('hello', self._receiveAll(port))
            self.assertIn('hello', self._receiveAll(port))
            ssh.ftp.putFile("/tmp/server.py", TCPSERVER_READEVERYTHING)
            ssh.run.backgroundScript("python /tmp/server.py 7789 > /tmp/output")
            self._send(port2, "wassup")
            self.assertIn('wassup', ssh.ftp.getContents("/tmp/output"))
            print "Closing just one tunnel server"
            ssh.tunnel.stopLocalForward(7787)
            time.sleep(1)
            print "Stopping all tunnels"
            ssh.tunnel.stopAll()
            with self.assertRaises(Exception):
                self._receiveAll(port)
            print "Closing tunnel"
            ssh.tunnel.close()
            print "Sleeping for few seconds, watch for exceptions from other threads"
            time.sleep(3)
            print "Done Sleeping for few seconds"

    def _flood_server(self, port, message):
        s = socket.socket()
        try:
            s.connect(("localhost", port))
            while True:
                s.sendall(message)
                time.sleep(0.1)
        except:
            pass
        finally:
            s.close()

    def test_LocalForwardingTunnelBrutalClose(self):
        with self._allocateOne() as (node, ssh, allocation):
            # Test close server brutally
            ssh.ftp.putFile("/tmp/server.py", TCPSERVER_READEVERYTHING)
            ssh.run.backgroundScript("python /tmp/server.py 8888 > /tmp/output.test")
            port3 = ssh.tunnel.localForward(8888)
            thread.start_new_thread(self._flood_server, (port3, "testtest"))
            time.sleep(5)
            ssh.run.execute("ps -ef | grep -i [p]ython | grep -i server | awk '{print $2}' "
                            "| xargs kill -9 || true")
            time.sleep(5)
            ssh.ftp.putFile("/tmp/server.py", TCPSERVER_READEVERYTHING)
            ssh.run.backgroundScript("python /tmp/server.py 7789 > /tmp/output")
            port2 = ssh.tunnel.localForward(7789)
            self._send(port2, "wassup")
            self.assertIn('wassup', ssh.ftp.getContents("/tmp/output"))

    @contextlib.contextmanager
    def _allocateOne(self):
        client = clientfactory.factory()
        try:
            requirement = api.Requirement(imageLabel=LABEL, imageHint=HINT)
            info = api.AllocationInfo(user='rackattack-api test', purpose='integration test')
            allocation = client.allocate(dict(it=requirement), info)
            print "Created allocation, waiting for node inauguration"
            try:
                allocation.wait(timeout=7 * 60)
                print "Allocation successfull, waiting for ssh"
                nodes = allocation.nodes()
                assert len(nodes) == 1, nodes
                it = nodes['it']
                it.fetchSerialLog()
                allocation.fetchPostMortemPack()
                print "SSH credentials:", it.rootSSHCredentials()
                ssh = connection.Connection(**it.rootSSHCredentials())
                ssh.waitForTCPServer()
                ssh.connect()
                print "SSH connected"
                yield it, ssh, allocation
            finally:
                allocation.free()
        finally:
            client.close()

    def _receiveAll(self, port):
        result = []
        s = socket.socket()
        try:
            s.connect(("localhost", port))
            while True:
                data = s.recv(4096)
                if len(data) == 0:
                    return "".join(result)
                result.append(data)
        finally:
            s.close()

    def _send(self, port, message):
        s = socket.socket()
        try:
            s.connect(("localhost", port))
            s.sendall(message)
        finally:
            s.close()

    def test_SerialAndPostMortemPack(self):
        with self._allocateOne() as (node, ssh, allocation):
            node.fetchSerialLog()
            allocation.fetchPostMortemPack()

    def test_TransportSimpleEcho(self):
        port = self._freeTCPPort()
        popen = subprocess.Popen(["python", "test/twistedserver_echojson.py", str(port)])
        self._waitForServerToBeReady(port)
        try:
            tested = transport.Transport("tcp://localhost:%d" % port)
            try:
                tested.sendJSON(dict(cmd='echo', arguments=dict(arg1="eran", arg2="shlomp")))
                result = tested.receiveJSON(3)
                self.assertEquals(result, ["Echoing", dict(
                    cmd='echo', arguments=dict(arg1="eran", arg2="shlomp"))])
            finally:
                tested.close()
        finally:
            popen.terminate()

    def test_TransportPublish(self):
        port = self._freeTCPPort()
        popen = subprocess.Popen(["python", "test/twistedserver_publishperiodically.py", str(port)])
        self._waitForServerToBeReady(port)
        global testEvent
        testEvent = threading.Event()

        def setEvent(arguments):
            if arguments != dict(data="fake data"):
                raise Exception("Bad arguments: %s" % (arguments,))
            global testEvent
            testEvent.set()
        try:
            tested = subscribe.Subscribe("tcp://localhost:%d" % port)
            try:
                tested.register(setEvent)
                testEvent.wait(1)
                self.assertTrue(testEvent.isSet())
            finally:
                tested.close()
        finally:
            popen.terminate()

    def _freeTCPPort(self):
        sock = socket.socket()
        try:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind(("", 0))
            return sock.getsockname()[1]
        finally:
            sock.close()

    def _waitForServerToBeReady(self, port):
        for i in xrange(10):
            sock = socket.socket()
            try:
                sock.connect(("localhost", port))
                return
            except:
                time.sleep(0.1)
            finally:
                sock.close()
        raise Exception("Frontend did not start")

    def test_DisablingDHCP(self):
        with self._allocateOne() as (node, ssh, allocation):
            try:
                ssh.run.script("dhclient -r; dhclient eth0")
                self.assertIn(node.ipAddress(), ssh.run.script("ifconfig"))
                ssh.run.script("echo still works")
                node.answerDHCP(False)
                ssh.run.script("echo still works")
                ssh.run.backgroundScript("sleep 2; dhclient -r; dhclient eth0; (echo IFCONFIGLINE;"
                                         "ifconfig -a; echo DHCPDONE) > /dev/console 2>&1")
                ssh.close()
                time.sleep(3)
                try:
                    ssh.connect()
                except:
                    pass
                else:
                    raise Exception("Connect must not succeed")
                before = time.time()
                while time.time() < before + 120:
                    serialLog = node.fetchSerialLog()
                    if 'DHCPDONE' in serialLog:
                        break
                    time.sleep(0.2)
                self.assertIn('DHCPDONE', serialLog)
                ifconfig = serialLog.split("IFCONFIGLINE")[1]
                self.assertNotIn(node.ipAddress(), ifconfig)
            except:
                import traceback
                traceback.print_exc()
                raise


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
