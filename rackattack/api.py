class Client:
    """
    To create a client:
    from rackattack import clientfactory
    client = clientfactory.factory()

    Open a virtual or physical provisioning client depending on the
    variables defined in the environment. Look inside clientfactory
    for more details
    """
    def allocate(self, requirements, allocationInfo):
        """
        This method receives a dict from names of nodes, to their requirements,
        and returns an allocation object.
        throws NotEnoughResourcesForAllocation.
        """
        assert False, "Deriving class must implement"


class Requirement:
    def __init__(self, imageLabel, imageHint=None, hardwareConstraints=None):
        """
        Describes what is expected of the allocated node.
        imageLabel is the name of the label to osmos into the node, or None
        to skip osmosis.
        imageHint is a string that allows the allocator to attempt to
        find a close matching image to shorten the image osmosis process.
        """
        self.imageLabel = imageLabel
        self.imageHint = imageHint
        self.hardwareConstraints = dict(
            minimumCPUs=1, minimumRAMGB=2,
            minimumDisk1SizeGB=16, minimumDisk2SizeGB=16)
        if hardwareConstraints is not None:
            self.hardwareConstraints.update(hardwareConstraints)


class AllocationInfo:
    def __init__(self, user, purpose, nice=0, comment=""):
        """
        This object represents what is this allocation for, by whom.
        This is important only for scheduling of shared resources

        user can be a name, or 'continuous integration', or 'QA'

        purpose can be:
        - 'build'
        - 'bare metal host for rack test'
        - 'vm runner for virtual rack test' (==slave)
        note: the last option is used to provision a CI slave, which
        means a virtual rack provider will be running on it.

        nice value: if you are writing a gready system, for example,
        a system for using as many free nodes as possible to run as
        many concurrent tests as possible, make sure to use increment
        this value for each additional allocation.
        """
        self.user = user
        self.purpose = purpose
        self.nice = nice
        self.comment = comment


class Allocation:
    """
    This object is returned from the Client.allocate method
    """
    def done(self):
        """
        This method tests if the allocation is done. If true,
        the client application may start using the nodes.
        """
        assert False, "Deriving class must implement"

    def dead(self):
        """
        This method returns true if the allocation is still in progress
        or done. If the allocation failed, or was previously freed,
        this method will return a string with the reason for the allocation
        death
        """
        assert False, "Deriving class must implement"

    def wait(self, timeout=None):
        """
        wait until allocation is either done (and return), or dead,
        in which case an exception will be raised with the death
        reason
        """
        assert False, "Deriving class must implement"

    def nodes(self):
        """
        the client application may only call this method if
        done returned True, and dead returns None. It will return
        a dictionary from the requirement names, as provided to
        allocate, to a Node object
        """
        assert False, "Deriving class must implement"

    def free(self):
        """
        free the allocation. After this was called, the application
        may not use any of the nodes returned previously by the nodes
        method, or call nodes again
        """
        assert False, "Deriving class must implement"

    def setForceReleaseCallback(self, callback):
        assert False, "Deriving class must implement"


class Node:
    def rootSSHCredentials(self):
        """
        returns a dictionary with: hostname, username == root, either password or key,
        and port == 22. Useful to pass as **kwargs to SSH class
        """
        assert False, "Deriving class must implement"

    def name(self):
        assert False, "Deriving class must implement"

    def primaryMACAddress(self):
        "Returns the MAC address out of which the node will DHCP from (e.g., eth0)"
        assert False, "Deriving class must implement"

    def secondaryMACAddress(self):
        "Returns the MAC address of the second NIC (e.g., eth1)"
        assert False, "Deriving class must implement"

    def ipAddress(self):
        "IP address assigned at DHCP to the primary NIC"
        assert False, "Deriving class must implement"

    def setPXEParameters(self, answerDHCP=True, assimilatorParameters=None):
        assert False, "Deriving class must implement"

    def coldRestart(self):
        """
        cold reboot the host, for testing purposes. This "pulls the switch", does not
        allow orderly shutdown. Note: physical servers sometimes take as much as 5 minutes
        to reboot, due long bios wakeup times.
        """
        assert False, "Deriving class must implement"