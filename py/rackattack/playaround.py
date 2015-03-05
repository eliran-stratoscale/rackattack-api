import os
from rackattack.ssh import connection
from rackattack import clientfactory
from rackattack import api
connection.discardParamikoLogs()
connection.discardSSHDebugMessages()
import argparse
import time

parser = argparse.ArgumentParser()
parser.add_argument("--label", required=True)
parser.add_argument("--user", required=True, help="User to report to provider")
parser.add_argument("--nice", type=float, default=1)
parser.add_argument("--noSSH", action='store_true', default=False)
parser.add_argument("--disk1SizeGB", type=int, default=None)
args = parser.parse_args()

print "Connecting"
client = clientfactory.factory()
print "Allocating"
hardwareConstraints = dict()
if args.disk1SizeGB is not None:
    hardwareConstraints['disk1SizeGB'] = args.disk1SizeGB
print "Hardware Constraints:", hardwareConstraints
requirement = api.Requirement(
    imageLabel=args.label,
    imageHint="playaround",
    hardwareConstraints=hardwareConstraints)
allocation = client.allocate(
    requirements={'node': requirement},
    allocationInfo=api.AllocationInfo(user=args.user, purpose="playaround", nice=args.nice))
allocation.wait(timeout=8 * 60)
assert allocation.done(), "Allocation failed"
print "Done allocating, Waiting for boot to finish"
try:
    node = allocation.nodes()['node']
    credentials = dict(port=22)
    credentials.update(node.rootSSHCredentials())
    print "ROOT SSH Credentials:"
    print credentials
    if not args.noSSH:
        ssh = connection.Connection(**credentials)
        ssh.waitForTCPServer()
        ssh.connect()
        ssh.close()
        try:
            ssh = connection.Connection(**credentials)
            ssh.waitForTCPServer()
            ssh.connect()
            ssh.close()
        except:
            try:
                log = node.fetchSerialLog()
                open("/tmp/serial.log", "w").write(log)
                print "serial log stored in /tmp/serial.log"
            except Exception as e:
                print "Unable to fetch serial log: %s" % e
            raise
        print "Opening ssh session. Close it to free up allocation"
        os.system(
            "sshpass -p %(password)s ssh -o ServerAliveInterval=5 -o ServerAliveCountMax=1 "
            "-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -p %(port)d "
            "%(username)s@%(hostname)s" % credentials)
    else:
        print "Not connecting to machine via SSH. Hit Ctrl-C to close the machine."
        time.sleep(1000000000)
finally:
    allocation.free()
