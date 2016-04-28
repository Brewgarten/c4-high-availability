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

from abc import ABCMeta
import ctypes
import datetime
import inspect
import logging
import multiprocessing
import time
import types

from c4.messaging import (Envelope, RouterClient)
from c4.system.backend import Backend
from c4.system.configuration import (Roles, States)
from c4.system.deviceManager import (DeviceManagerImplementation, DeviceManagerStatus)
from c4.utils.enum import Enum
from c4.utils.jsonutil import JSONSerializable
from c4.utils.logutil import ClassLogger
from c4.utils.util import (SharedDictWithLock,
                           getFullModuleName)


log = logging.getLogger(__name__)

class Event(object):
    """
    Base Raft event class
    """
    __metaclass__ = ABCMeta

class CheckHeartbeatEvent(Event):
    """
    Detailed heartbeat check event

    :param clusterInfo: cluster information of the node that is checking
    :type clusterInfo: :class:`~c4.system.configuration.DBClusterInfo`
    :param node: name of the node from which the details originated
    :type node: str
    :param details: details to be checked
    :type details: dict
    """
    def __init__(self, clusterInfo, node, details):
        self.clusterInfo = clusterInfo
        self.node = node
        self.details = details

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

@ClassLogger
class EventHandler(object):
    """
    A Raft event handler implementation
    """
    __metaclass__ = ABCMeta

    def checkHeartbeat(self, event):
        """
        Check heartbeat detailed results

        :param event: check heartbeat event
        :type event: :class:`~CheckHeartbeatEvent`
        :returns: `True` if heartbeat is ok, `False` otherwise
        :rtype: bool
        """
        return True

    def handleNewLeaderElected(self, event):
        """
        Handle new `leader` elected event

        :param event: new `leader` elected event
        :type event: :class:`~NewLeaderElectedEvent`
        """

    def handleNodeJoin(self, event):
        """
        Handle node join event

        :param event: node join event
        :type event: :class:`~NodeJoinEvent`
        """

    def handleNodeLeave(self, event):
        """
        Handle node leave event

        :param event: node leave event
        :type event: :class:`~NodeLeaveEvent`
        """

    def handleNodeRejoin(self, event):
        """
        Handle node rejoin event

        :param event: node rejoin event
        :type event: :class:`~NodeRejoinEvent`
        """

    def performHeartbeat(self, event):
        """
        Perform detailed heartbeat and store results in details

        :param event: perform heartbeat event
        :type event: :class:`~PerformHeartbeatEvent`
        """

class EventHandlerInfo(JSONSerializable):
    """
    Event handler information that contains references to
    - handlers: list of :class:`~EventHandler` implementations
    - events: mappings of events to functional event handlers
    """
    def __init__(self):
        self.handlers = []
        self.events = {}

    def getEventHandlerWrapper(self):
        """
        Get event handler wrapper for the functional event mappings

        :returns: event handler wrapper
        :rtype: :class:`~EventHandlerWrapper`
        """
        return EventHandlerWrapper(self.events)

    def getHandlers(self):
        """
        Get a list of :class:`~EventHandler` implementations

        :returns: list of event handler implementations
        :rtype: [:class:`~EventHandler`]
        """
        handlers = []
        for rawHandler in self.handlers:
            handler = load(rawHandler)
            if handler:
                handlers.append(handler())
        return handlers

@ClassLogger
class EventHandlerProxy(EventHandler):
    """
    A Raft event handler proxy implementation that passes event calls to the
    established event handler implementations and the individual event functional handlers

    :param info: event handler information
    :type info: :class:`~EventHandlerInfo`
    """
    def __init__(self, info):
        super(EventHandlerProxy, self).__init__()
        self.handlers = info.getHandlers()
        self.eventHandlerWrapper = info.getEventHandlerWrapper()
        # dynamically attach event handler proxies for the known events
        for name, _ in inspect.getmembers(EventHandler, inspect.ismethod):
            if not name.startswith("_"):
                self.updateHandler(name)

    def updateHandler(self, eventName):
        """
        Dynamically attach a handler proxy method to this instance for the specified event

        :param eventName: event name
        :type eventName: str
        """
        def handle(self, event):
            results = {}
            for handler in self.handlers:
                name = "{module}.{name}".format(module=getFullModuleName(handler), name=handler.__class__.__name__)
                try:
                    results[name] = getattr(handler, eventName)(event)
                except Exception as e:
                    log.error("could not call '%s.%s' because '%s'", name, eventName, e)
            try:
                wrapperResults = getattr(self.eventHandlerWrapper, eventName)(event)
                results.update(wrapperResults)
            except Exception as e:
                log.error("could not call 'eventHandlerWrapper.%s' because '%s'", eventName, e)
            return results
        setattr(self, eventName, types.MethodType(handle, self))

@ClassLogger
class EventHandlerWrapper(EventHandler):
    """
    A Raft event handler wrapper implementation for individual events

    :param events: event to functional handler mapping
    :type events: dict
    """
    def __init__(self, events):
        super(EventHandlerWrapper, self).__init__()
        knownEvents = set()
        # dynamically attach event handler proxies for the known events
        for name, _ in inspect.getmembers(EventHandler, inspect.ismethod):
            if not name.startswith("_"):
                knownEvents.add(name)
                rawHandlers = events.get(name, [])

                # make sure we have a list
                if isinstance(rawHandlers, (str, unicode)):
                    rawHandlers = [rawHandlers]

                # load actual handler implementations
                handlers = []
                for rawHandler in rawHandlers:
                    handler = load(rawHandler)
                    if handler:
                        handlers.append(handler)

                self.updateHandler(name, handlers)

        unknownEvents = set(events.keys()).discard(knownEvents)
        if unknownEvents:
            log.warn("found unknown events '%s'", unknownEvents)

    def updateHandler(self, eventName, handlers):
        """
        Dynamically attach a handler proxy method to this instance for the specified event

        :param eventName: event name
        :type eventName: str
        :param handlers: list of handler functions to be called
        :type handlers: [func]
        """
        def handle(self, event):
            results = {}
            for handler in handlers:
                name = "{module}.{name}".format(module=getFullModuleName(handler), name=handler.__name__)
                try:
                    results[name] = handler(event)
                except Exception as e:
                    log.error("could not call '%s' for '%s' because '%s'", name, eventName, e)
            return results
        setattr(self, eventName, types.MethodType(handle, self))

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
class HeartbeatProcess(multiprocessing.Process):
    """
    The extended heartbeat process executing hooks for performing a more
    detailed assessment of the node/system.

    :param clusterInfo: cluster information
    :type clusterInfo: :class:`~c4.system.configuration.DBClusterInfo`
    :param eventHandlers: event handler proxy
    :type eventHandlers: :class:`~EventHandlerProxy`
    :param properties: properties
    :type properties: dict
    """
    def __init__(self, clusterInfo, eventHandlers, properties=None):
        self.clusterInfo = clusterInfo
        super(HeartbeatProcess, self).__init__(name="RaftHeartbeatProcess {node}".format(node=clusterInfo.node))
        self.eventHandlers = eventHandlers
        self.properties = properties if properties else {}
        self.stopFlag = multiprocessing.Event()
        self.details = SharedDictWithLock()

    def run(self):
        """
        The implementation of the heartbeat monitoring process
        """
        time.sleep(self.properties.get("heartbeat.perform.initial", 0))

        counter = self.properties.get("heartbeat.perform.interval", 5)
        while not self.stopFlag.is_set():
            if counter <= 0:
                self.details.lock.acquire()
                for key in self.details.keys():
                    del self.details[key]
                self.details.lock.release()
                self.eventHandlers.performHeartbeat(
                    PerformHeartbeatEvent(
                        self.clusterInfo,
                        self.details
                    )
                )
                counter = self.properties.get("heartbeat.perform.interval", 5)
            counter -= 1
            time.sleep(1)

class NewLeaderElectedEvent(Event):
    """
    New leader elected event

    :param oldLeader: old `leader` node
    :type oldLeader: str
    :param newLeader: new `leader` node
    :type newLeader: str
    """
    def __init__(self, oldLeader, newLeader):
        self.oldLeader = oldLeader
        self.newLeader = newLeader

class NodeJoinEvent(Event):
    """
    New node joining the cluster event

    :param node: node that joined
    :type node: str
    """
    def __init__(self, node):
        self.node = node

class NodeLeaveEvent(Event):
    """
    Node leaving the cluster event

    :param node: node that left
    :type node: str
    """
    def __init__(self, node):
        self.node = node

class NodeRejoinEvent(Event):
    """
    Node rejoining after leaving previously leaving the cluster event

    :param node: node that rejoined
    :type node: str
    """
    def __init__(self, node):
        self.node = node

class PerformHeartbeatEvent(Event):
    """
    Performing detailed heartbeat event

    :param clusterInfo: cluster information of the node that is performing the heartbeat
    :type clusterInfo: :class:`~c4.system.configuration.DBClusterInfo`
    :param details: details to be filled with detailed heartbeat information
    :type details: dict
    """
    def __init__(self, clusterInfo, details):
        self.clusterInfo = clusterInfo
        self.details = details

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
        self.heartbeatProcess = None

        # set up event handlers
        info = EventHandlerInfo()
        eventHandlers = self.properties.get("event.handlers", {})
        info.events = eventHandlers.get("events", {})
        info.handlers = eventHandlers.get("handlers", [])
        self.eventHandlers = EventHandlerProxy(info)

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
                              self.raftProcess.election.term)
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
            # add details from extended heartbeat
            "details": {
                key: self.heartbeatProcess.details[key]
                for key in self.heartbeatProcess.details.keys()
            },
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
                # note that because of our approach where passive node respond to the active node
                # it is implied that event handling only happens on the active node
                self.eventHandlers.handleNodeLeave(
                    NodeLeaveEvent(raftDeviceManagerAddress.split("/")[0])
                )

        # TODO: handle nodes that become available again

        # check if this is a new node joining
        nodeJoinEvent = None
        if envelope.From not in self.raftProcess.heartbeats:
            nodeJoinEvent = NodeJoinEvent(envelope.From.split("/")[0])

        # check on whether additional details check is necessary
        if message.get("details", {}):
            checkResults = self.eventHandlers.checkHeartbeat(
                CheckHeartbeatEvent(
                    self.clusterInfo,
                    envelope.From.split("/")[0],
                    message["details"]
                )
            )
            if all(checkResults.values()):
                # update heart beat dictionary
                self.raftProcess.heartbeats[envelope.From] = time.time()
                if nodeJoinEvent:
                    self.eventHandlers.handleNodeJoin(nodeJoinEvent)
        else:
            # update heart beat dictionary
            self.raftProcess.heartbeats[envelope.From] = time.time()
            if nodeJoinEvent:
                self.eventHandlers.handleNodeJoin(nodeJoinEvent)

    def handleLocalStartDeviceManager(self, message, envelope):
        """
        Handle :class:`~c4.system.messages.LocalStartDeviceManager` messages

        :param message: message
        :type message: dict
        :param envelope: envelope
        :type envelope: :class:`~c4.system.messages.Envelope`
        """
        response = super(Raft, self).handleLocalStartDeviceManager(message, envelope)
        self.raftProcess = RaftProcess(self.clusterInfo,
                                       self.name,
                                       self.eventHandlers,
                                       self.properties)
        self.raftProcess.start()
        self.heartbeatProcess = HeartbeatProcess(self.clusterInfo,
                                                 self.eventHandlers,
                                                 properties=self.properties)
        self.heartbeatProcess.start()
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
        self.heartbeatProcess.stopFlag.set()
        self.heartbeatProcess.join()
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
        elif message["term"] == self.raftProcess.election.term and not self.raftProcess.election.votedFor:
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
            for raftDeviceManagerAddress in self.raftProcess.unavailable.keys():
                detected = datetime.datetime.utcfromtimestamp(self.raftProcess.unavailable[raftDeviceManagerAddress])
                status.unavailable[raftDeviceManagerAddress.split("/")[0]] = "{:%Y-%m-%d %H:%M:%S}.{:03d}".format(detected, detected.microsecond // 1000)
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
    :param eventHandlers: event handlers
    :type eventHandlers: :class:`~EventHandlerProxy`
    :param properties: optional properties
    :type properties: dict
    """
    def __init__(self, clusterInfo, deviceManagerName, eventHandlers, properties):
        super(RaftProcess, self).__init__(name="RaftProcess {node}".format(node=clusterInfo.node))
        self.clusterInfo = clusterInfo
        self.raftDeviceManagerName = deviceManagerName
        self.raftDeviceManagerAddress = "{node}/{name}".format(node=clusterInfo.node, name=deviceManagerName)
        self.raftDeviceManagerClient = None
        self.eventHandlers = eventHandlers
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
                    if self.raftRole == RaftRoles.LEADER:
                        self.eventHandlers.handleNodeLeave(
                            NodeLeaveEvent(raftDeviceManagerAddress.split("/")[0])
                        )
            else:
                if raftDeviceManagerAddress in self.unavailable:
                    self.log.info("node '%s' detected node '%s' heart beat again", self.node, raftDeviceManagerAddress)
                    del self.unavailable[raftDeviceManagerAddress]
                    if self.raftRole == RaftRoles.LEADER:
                        self.eventHandlers.handleNodeRejoin(
                            NodeRejoinEvent(raftDeviceManagerAddress.split("/")[0])
                        )

    def getHAEnabledNodes(self):
        """
        Get nodes with Raft device managers

        :returns: passive nodes and thin nodes with device manager information
        :rtype: (dict, dict)
        """
        configuration = Backend().configuration

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
            configuration = Backend().configuration
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

            self.eventHandlers.handleNewLeaderElected(
                NewLeaderElectedEvent(
                    oldActiveSystemManager,
                    self.node
                )
            )

    def performFollowerRole(self):
        """
        Perform `follower` role
        """
        if self.clusterInfo.role == Roles.PASSIVE:

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
        self.unavailable = {}

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

def load(fullName):
    """
    Based of the specified fully qualified name, load a function or class

    :param fullName: fully qualified function or class name
    :param fullName: str
    :returns: function or class
    """
    try:
        # get function or class info
        info = fullName.split(".")
        name = info.pop()
        moduleName = ".".join(info)
        # load function or class from module
        module = __import__(moduleName, fromlist=[name])
        return getattr(module, name)
    except Exception:
        log.error("could not load '%s'", fullName)
    return None
