"""
This is a high availability implementation that uses `Raft <https://raft.github.io/>`_
for `active` node failover capabilities and a multi-level tree approach to
monitor `passive` and `thin` nodes.

Overview
--------

Active node
^^^^^^^^^^^

We form a Raft cluster where the `leader` maps to the `active` node and
`passive` nodes to `followers`. Following the Raft protocol where the `leader`
is responsible for sending heartbeats to the followers which are in turn
monitored by the `followers` we are able to detect loss of the `leader`.

Passive nodes
^^^^^^^^^^^^^

Since the `active` as the `leader` is already sending out heartbeats we
can utilize this mechanism to detect loss of `followers`, i.e., `passive`
nodes.

.. note::

    For scalability reasons the `active` node only directly monitors
    `passive` nodes

Thin nodes
^^^^^^^^^^

While `thin` nodes are technically also considered Raft `followers` we treat
them differently in two ways:
1. they do not have the ability to become `leaders`
2. they are monitored by the `passive` nodes

Functionality
-------------
"""

from __future__ import division

import ctypes
import datetime
import logging
import multiprocessing
import time

from c4.messaging.base import Envelope
from c4.messaging.zeromqMessaging import RouterClient
from c4.system.configuration import Configuration, Roles, States
from c4.system.deviceManager import (DeviceManagerImplementation, DeviceManagerStatus)
from c4.utils.enum import Enum
from c4.utils.logutil import ClassLogger
from c4.utils.util import SharedDictWithLock


log = logging.getLogger(__name__)

@ClassLogger
class Election(object):
    """
    Raft election information

    :param term: term
    :type term: int
    """
    def __init__(self, term=0):
        self._eligibleVoters = multiprocessing.Value(ctypes.c_int, 0)
        self._term = multiprocessing.Value(ctypes.c_int, term)
        self._voteCount = multiprocessing.Value(ctypes.c_int, 0)
        self._votedFor = multiprocessing.Value(ctypes.c_char_p, "")

    @property
    def eligibleVoters(self):
        """
        Eligible voters
        """
        return self._eligibleVoters.value

    @eligibleVoters.setter
    def eligibleVoters(self, value):
        if isinstance(value, int):
            with self._eligibleVoters.get_lock():
                self._eligibleVoters.value = value
        else:
            self.log.error("'%s' does not match type '%s'", value, int)

    def incrementTerm(self):
        """
        Increment term
        """
        with self._term.get_lock():
            self._term.value += 1

    def incrementVoteCount(self):
        """
        Increment vote count
        """
        with self._voteCount.get_lock():
            self._voteCount.value += 1

    def resetVoteCount(self):
        """
        Reset vote count
        """
        with self._voteCount.get_lock():
            self._voteCount.value = 0

    def startNewElection(self, node, eligibleVoters):
        """
        Start a new election

        :param node: node that called for the election
        :type node: str
        :param eligibleVoters: number of eligible voters
        :type eligibleVoters: int
        """
        self.incrementTerm()
        self.eligibleVoters = eligibleVoters
        self.resetVoteCount()

        # vote for node which called the election
        self.votedFor = node
        self.incrementVoteCount()

    @property
    def term(self):
        """
        Term
        """
        return self._term.value

    @term.setter
    def term(self, value):
        if isinstance(value, int):
            with self._term.get_lock():
                self._term.value = value
        else:
            self.log.error("'%s' does not match type '%s'", value, int)

    @property
    def voteCount(self):
        """
        Vote count
        """
        return self._voteCount.value

    @property
    def votedFor(self):
        """
        Node that was voted for
        """
        return self._votedFor.value

    @votedFor.setter
    def votedFor(self, value):
        if isinstance(value, (str, unicode)):
            with self._votedFor.get_lock():
                self._votedFor.value = value
        else:
            self.log.error("'%s' does not match type '%s'", value, (str, unicode))

class Heartbeat(Envelope):
    """
    Raft heartbeat

    :param From: From address
    :type From: str
    :param To: To address
    :type To: str
    :param leader: current leader
    :type leader: str
    :param term: current term
    :type term: int
    :param nodesToWatch: nodes to be watched
    :type nodesToWatch: dict
    :param systemManagerAddress: system manager address
    :type systemManagerAddress: str
    """
    def __init__(self, From, To, leader, term, nodesToWatch=None, systemManagerAddress=None):
        super(Heartbeat, self).__init__(From, To)
        self.Message = {
            "leader": leader,
            "term": term
        }
        if nodesToWatch:
            self.Message["nodesToWatch"] = nodesToWatch
        if systemManagerAddress:
            self.Message["systemManagerAddress"] = systemManagerAddress

@ClassLogger
class Raft(DeviceManagerImplementation):
    """
    A Raft based high availability device manager

    :param clusterInfo: cluster information
    :type clusterInfo: :class:`~c4.system.configuration.DBClusterInfo`
    :param name: name
    :type name: str
    :param properties: optional properties
    :type properties: dict
    """
    def __init__(self, clusterInfo, name, properties=None):
        super(Raft, self).__init__(clusterInfo, name, properties=properties)
        self.raftProcess = None

    def handleForwardHeartbeatResponse(self, message, envelope):
        """
        Handle `ForwardHeartbeatResponse` messages, that is heart beats that
        the `active` node needs to forward to a `passive` node.

        :param message: message
        :type message: dict
        :param envelope: envelope
        :type envelope: :class:`~c4.system.messages.Envelope`
        """
        self.log.debug("'%s' received forward heart beat: %s", self.node, message)

        client = RouterClient(self.node)
        heartBeat = Heartbeat(message["from"],
                              envelope.From,
                              self.node,
                              self.raftProcess.election.term,
                              )
        heartBeat.toResponse(message["response"])
        client.forwardMessage(heartBeat)

    def handleHeartbeat(self, message, envelope):
        """
        Handle :class:`~Heartbeat` messages

        :param message: message
        :type message: dict
        :param envelope: envelope
        :type envelope: :class:`~c4.system.messages.Envelope`
        """
        self.log.debug("'%s' received heart beat from '%s': %s", self.node, envelope.From, message)

        # update system manager address
        if "systemManagerAddress" in message:
            self.clusterInfo.systemManagerAddress = message["systemManagerAddress"]

        # update nodes to watch
        if "nodesToWatch" in message:
            # add new nodes to watch
            now = time.time()
            for node, raftDeviceManagerAddress in message["nodesToWatch"].items():
                if raftDeviceManagerAddress not in self.raftProcess.heartbeats.keys():
                    self.log.info("adding node '%s' to be watched", node)
                    self.raftProcess.heartbeats[raftDeviceManagerAddress] = now

            # remove nodes we are not watching anymore
            for raftDeviceManagerAddress in set(self.raftProcess.heartbeats.keys()).difference(set(message["nodesToWatch"].values())):
                self.log.info("removing '%s' from watch list", raftDeviceManagerAddress)
                del self.raftProcess.heartbeats[raftDeviceManagerAddress]

        # TODO: check heartbeat message term and leader and compare to my information
        response = {
            "leader": self.raftProcess.election.votedFor,
            "term": self.raftProcess.election.term
        }

        # add information on unavailable nodes
        if self.raftProcess.unavailable.keys():
            response["unavailable"] = {}
            for node in self.raftProcess.unavailable.keys():
                response["unavailable"][node] = self.raftProcess.unavailable[node]

        self.raftProcess.lastHeartbeat = time.time()

        # TODO: include the nodesToWatch and their states, heart beats, etc.

        # FIXME: instead of this redirection we should extend the cluster info implementation to support a secondary address cache in getNodeAddress
        if self.clusterInfo.role == Roles.THIN:
            # we might not have the address for the requesting node if this came from a passive node, so respond via system manager
            client = RouterClient(self.node)
            forwardHeartbeat = Envelope(envelope.To, "system-manager/ha", "ForwardHeartbeatResponse", isRequest=False)
            forwardHeartbeat.Message["response"] = response
            forwardHeartbeat.Message["from"] = envelope.From
            client.forwardMessage(forwardHeartbeat)
        else:
            return response

    def handleHeartbeatResponse(self, message, envelope):
        """
        Handle :class:`~HeartbeatResponse` messages

        :param message: message
        :type message: dict
        :param envelope: envelope
        :type envelope: :class:`~c4.system.messages.Envelope`
        """
        self.log.debug("'%s' received heart beat response from '%s': %s", self.node, envelope.From, message)

        if "error" in message:
            self.log.error("%s: %s", envelope.From, message["error"])

        # check on nodes that became unavailable indirectly
        for raftDeviceManagerAddress, detected in message.get("unavailable", {}).items():
            if raftDeviceManagerAddress not in self.raftProcess.unavailable.keys():
                log.info("detected unavailable node %s", raftDeviceManagerAddress)
                self.raftProcess.unavailable[raftDeviceManagerAddress] = detected

        # TODO: handle nodes that become available again

        # update heart beat dictionary
        self.raftProcess.heartbeats[envelope.From] = time.time()

    def handleLocalStartDeviceManager(self, message, envelope):
        """
        Handle :class:`~c4.system.messages.LocalStartDeviceManager` messages

        :param message: message
        :type message: dict
        :param envelope: envelope
        :type envelope: :class:`~c4.system.messages.Envelope`
        """
        response = super(Raft, self).handleLocalStartDeviceManager(message, envelope)
        self.raftProcess = RaftProcess(self.clusterInfo, self.name, self.properties)
        self.raftProcess.start()
        return response

    def handleLocalStopDeviceManager(self, message, envelope):
        """
        Handle :class:`~c4.system.messages.LocalStopDeviceManager` messages

        :param message: message
        :type message: dict
        :param envelope: envelope
        :type envelope: :class:`~c4.system.messages.Envelope`
        """
        response = super(Raft, self).handleLocalStopDeviceManager(message, envelope)
        self.raftProcess.stopFlag.set()
        self.raftProcess.join()
        return response

    def handleRequestVote(self, message, envelope):
        """
        Handle :class:`~RequestVote` messages

        :param message: message
        :type message: dict
        :param envelope: envelope
        :type envelope: :class:`~c4.system.messages.Envelope`
        """
        self.log.debug("'%s' received request vote for '%s': %s", self.node, envelope.From, message)

        # go through voting process for this node
        requestingNode = envelope.From.split("/")[0]
        if message["term"] > self.raftProcess.election.term:
            # new term, so move up to new term and vote
            self.raftProcess.election.term = message["term"]
            message["votedFor"] = requestingNode
            self.raftProcess.election.votedFor = requestingNode
        elif message["term"] == self.raftProcess.election.term and not self.raftProcess.election.voted:
            # current term but not voted yet, so vote
            message["votedFor"] = requestingNode
            self.raftProcess.election.votedFor = requestingNode
        else:
            # already voted, so send back who we voted for
            message["votedFor"] = self.raftProcess.election.votedFor

        # reset election timeout
        self.raftProcess.lastHeartbeat = time.time()

        return message

    def handleRequestVoteResponse(self, message):
        """
        Handle :class:`~RequestVoteResponse` messages

        :param message: message
        :type message: dict
        :param envelope: envelope
        :type envelope: :class:`~c4.system.messages.Envelope`
        """
        self.log.debug("'%s' received request vote response: %s", self.node, message)

        # TODO: need to check term
        if message["votedFor"] == self.node:
            self.raftProcess.election.incrementVoteCount()

    def handleStatus(self, message):
        """
        Handle :class:`~c4.system.messages.Status` messages

        :param message: message
        :type message: dict
        """
        status = RaftStatus(self.raftProcess.raftRole)

        # add information on unavailable nodes
        if self.raftProcess.unavailable.keys():
            status.unavailable = {}
            for node in self.raftProcess.unavailable.keys():
                detected = datetime.datetime.utcfromtimestamp(self.raftProcess.unavailable[node])
                status.unavailable[node] = "{:%Y-%m-%d %H:%M:%S}.{:03d}".format(detected, detected.microsecond // 1000)

        return status

@ClassLogger
class RaftProcess(multiprocessing.Process):
    """
    The Raft monitoring process performing the steps of the Raft protocol for
    `active` node and the multi-level tree approach for `passive` and `thin`
    node failure detection

    :param clusterInfo: cluster information
    :type clusterInfo: :class:`~c4.system.configuration.DBClusterInfo`
    :param deviceManagerName: Raft device manager name
    :type deviceManagerName: str
    :param properties: optional properties
    :type properties: dict
    """
    def __init__(self, clusterInfo, deviceManagerName, properties):
        super(RaftProcess, self).__init__(name="RaftProcess {node}".format(node=clusterInfo.node))
        self.clusterInfo = clusterInfo
        self.raftDeviceManagerName = deviceManagerName
        self.raftDeviceManagerAddress = "{node}/{name}".format(node=clusterInfo.node, name=deviceManagerName)
        self.properties = properties
        self.stopFlag = multiprocessing.Event()

        self._lastHeartbeat = multiprocessing.Value(ctypes.c_double, time.time())

        if self.clusterInfo.role == Roles.ACTIVE:
            self._raftRole = multiprocessing.Value(ctypes.c_char_p, RaftRoles.LEADER.name)
        else:
            self._raftRole = multiprocessing.Value(ctypes.c_char_p, RaftRoles.FOLLOWER.name)

        self.election = Election()

        self.heartbeats = SharedDictWithLock()
        self.unavailable = SharedDictWithLock()

    def checkHeartbeats(self):
        """
        Check heartbeats for the nodes that this node is responsible
        """
        now = time.time()
        for raftDeviceManagerAddress in self.heartbeats.keys():
            if self.heartbeats[raftDeviceManagerAddress] + self.properties.get("heartbeat.timeout", 10) < now:
                if raftDeviceManagerAddress not in self.unavailable:
                    self.log.info("node '%s' detected node '%s' heart beat time out", self.node, raftDeviceManagerAddress)
                    self.unavailable[raftDeviceManagerAddress] = now
            else:
                if raftDeviceManagerAddress in self.unavailable:
                    self.log.info("node '%s' detected node '%s' heart beat again", self.node, raftDeviceManagerAddress)
                    del self.unavailable[raftDeviceManagerAddress]

    def getHAEnabledNodes(self):
        """
        Get nodes with Raft device managers

        :returns: passive nodes and thin nodes with device manager information
        :rtype: (dict, dict)
        """
        configuration = Configuration()

        passiveNodes = {}
        thinNodes = {}
        for node in configuration.getNodeNames():
            nodeInfo = configuration.getNode(node, includeDevices=True, flatDeviceHierarchy=True)
            if nodeInfo.state == States.RUNNING:

                # check for raft device manager
                nodeRaftDeviceManagers = {
                    fullName: device
                    for fullName, device in nodeInfo.devices.items()
                    if device.type == "c4.devices.ha.raft.Raft"
                }
                if nodeRaftDeviceManagers:
                    if len(nodeRaftDeviceManagers) == 1:

                        nodeRaftDeviceManagerAddress = "{node}/{fullName}".format(node=node, fullName=nodeRaftDeviceManagers.keys()[0])

                        if nodeInfo.role == Roles.PASSIVE:
                            passiveNodes[node] = nodeRaftDeviceManagerAddress
                        elif nodeInfo.role == Roles.THIN:
                            thinNodes[node] = nodeRaftDeviceManagerAddress

                    else:
                        self.log.error("node '%s' should only have one Raft device manager", node)
                else:
                    self.log.error("node '%s' is missing the Raft device manager", node)

            else:
                self.log.debug("node '%s' currently not running but '%s' instead", node, nodeInfo.state.name)

        return passiveNodes, thinNodes

    def getNodesToWatch(self, passiveNodes, thinNodes):
        """
        Get a distribution of `thin` nodes and the respective `passive` nodes
        that are responsible for watching them

        :param passiveNodes: node to Raft device manager mapping
        :type passiveNodes: dict
        :param thinNodes: node to Raft device manager mapping
        :type thinNodes: dict
        :returns: node to responsible nodes mapping
        :rtype: dict
        """
        if not passiveNodes:
            return {}

        passiveNodeNames = sorted(passiveNodes.keys())
        thinNodeNames = sorted(thinNodes.keys())

        # distribute thin nodes in round-robin fashion to passive nodes
        nodesToWatch = {
            node: {}
            for node in passiveNodeNames
        }
        for index, thinNode in enumerate(thinNodeNames):
            passiveNode = passiveNodeNames[index % len(passiveNodeNames)]
            nodesToWatch[passiveNode][thinNode] = thinNodes[thinNode]

        return nodesToWatch

    @property
    def lastHeartbeat(self):
        """
        Time of last hearbeat received from `leader`
        """
        return self._lastHeartbeat.value

    @lastHeartbeat.setter
    def lastHeartbeat(self, value):
        if isinstance(value, float):
            with self._lastHeartbeat.get_lock():
                self._lastHeartbeat.value = value
        else:
            self.log.error("'%s' does not match enum of type '%s'", value, float)

    @property
    def node(self):
        """
        Node name
        """
        return self.clusterInfo.node

    def performCandidateRole(self):
        """
        Perform `candidate` role
        """
        self.log.info("'%s' is a candidate with '%d' out of '%d' votes", self.node, self.election.voteCount, self.election.eligibleVoters)

        if self.election.voteCount > self.election.eligibleVoters // 2:
            self.log.info("'%s' has been elected new leader", self.node)

            # perform failover
            configuration = Configuration()
            oldActiveSystemManager = configuration.getSystemManagerNodeName()
            configuration.changeRole(oldActiveSystemManager, Roles.PASSIVE)
            configuration.changeRole(self.node, Roles.ACTIVE)
            configuration.changeAlias("system-manager", self.node)
            self.clusterInfo.role = Roles.ACTIVE
            self.clusterInfo.systemManagerAddress = configuration.getAddress(self.node)
            self.raftRole = RaftRoles.LEADER

            passiveNodes, _ = self.getHAEnabledNodes()
            for passiveNode, passiveNodeRaftDeviceManagerAddress in passiveNodes.items():
                self.log.info("new leader '%s' appending entries to '%s'", self.node, passiveNode)
                heartBeat = Heartbeat(self.raftDeviceManagerAddress,
                                      passiveNodeRaftDeviceManagerAddress,
                                      self.node,
                                      self.election.term,
                                      systemManagerAddress=self.clusterInfo.systemManagerAddress)
                self.raftDeviceManagerClient.forwardMessage(heartBeat)

    def performFollowerRole(self):
        """
        Perform `follower` role
        """
        if (self.clusterInfo.role == Roles.PASSIVE):

            self.checkHeartbeats()

            # send out heart beats to thin nodes
            for raftDeviceManagerAddress in self.heartbeats.keys():
                heartBeat = Heartbeat(self.raftDeviceManagerAddress,
                                      raftDeviceManagerAddress,
                                      self.node,
                                      self.election.term)
                self.raftDeviceManagerClient.forwardMessage(heartBeat)

            # check on active node
            if self.lastHeartbeat + self.properties.get("election.timeout", 10) < time.time():
                self.log.info("node '%s' detected active system manager heart beat time out", self.node)

                # start new election
                self.raftRole = RaftRoles.CANDIDATE
                passiveNodes, _ = self.getHAEnabledNodes()
                self.election.startNewElection(self.node, len(passiveNodes) + 1)

                for passiveNode, passiveNodeRaftDeviceManagerAddress in passiveNodes.items():
                    if passiveNode != self.node:
                        requestVote = RequestVote(self.raftDeviceManagerAddress,
                                                  passiveNodeRaftDeviceManagerAddress,
                                                  self.election.term)
                        self.raftDeviceManagerClient.forwardMessage(requestVote)

    def performLeaderRole(self):
        """
        Perform `leader` role
        """
        self.checkHeartbeats()

        passiveNodes, thinNodes = self.getHAEnabledNodes()

        if passiveNodes:

            # adjust the passive nodes we are watching
            now = time.time()
            for passiveNodeRaftDeviceManagerAddress in passiveNodes.values():
                if passiveNodeRaftDeviceManagerAddress not in self.heartbeats:
                    self.heartbeats[passiveNodeRaftDeviceManagerAddress] = now

            nodesToWatch = self.getNodesToWatch(passiveNodes, thinNodes)
            self.log.debug("nodes to watch: %s", nodesToWatch)

            for passiveNode, passiveNodeRaftDeviceManagerAddress in passiveNodes.items():
                self.log.debug("passive node Raft device manager '%s' responsible for '%s'", passiveNodeRaftDeviceManagerAddress, nodesToWatch[passiveNode])
                heartBeat = Heartbeat(self.raftDeviceManagerAddress,
                                      passiveNodeRaftDeviceManagerAddress,
                                      self.node,
                                      self.election.term,
                                      nodesToWatch=nodesToWatch[passiveNode])
                self.raftDeviceManagerClient.forwardMessage(heartBeat)

        else:
            # FIXME: handle case where we do not have any passive nodes
            self.log.error("need to check thin nodes directly")

    @property
    def raftRole(self):
        """
        Raft role
        """
        return RaftRoles.valueOf(self._raftRole.value)

    @raftRole.setter
    def raftRole(self, role):
        if isinstance(role, RaftRoles):
            with self._raftRole.get_lock():
                self._raftRole.value = role.name
        else:
            self.log.error("'%s' does not match enum of type '%s'", role, RaftRoles)

    def run(self):
        """
        The implementation of the Raft monitoring process
        """
        self.raftDeviceManagerClient = RouterClient(self.node)

        time.sleep(self.properties.get("heartbeat.initial", 0))

        heartBeatCounter = self.properties.get("heartbeat.interval", 5)
        while not self.stopFlag.is_set():

            if self.raftRole == RaftRoles.LEADER:
                if heartBeatCounter <= 0:
                    self.performLeaderRole()
                    heartBeatCounter = self.properties.get("heartbeat.interval", 5)
                heartBeatCounter -= 1

            elif self.raftRole == RaftRoles.CANDIDATE:
                self.performCandidateRole()
                heartBeatCounter = 0

            else:
                self.performFollowerRole()

            time.sleep(1)

class RaftRoles(Enum):
    """
    Enumeration of Raft roles
    """
    CANDIDATE = "candidate"
    FOLLOWER = "follower"
    LEADER = "leader"

class RaftStatus(DeviceManagerStatus):
    """
    Raft device manager status

    :param role: role
    :type role: :class:`~RaftRoles`
    """
    def __init__(self, role):
        super(RaftStatus, self).__init__()
        self.role = role

class RequestVote(Envelope):
    """
    Raft voting request

    :param From: from field
    :type From: str
    :param To: to field
    :type To: str
    :param term: term
    :type term: int
    """
    def __init__(self, From, To, term):
        super(RequestVote, self).__init__(From, To)
        self.Message = {
            "term": term
        }
