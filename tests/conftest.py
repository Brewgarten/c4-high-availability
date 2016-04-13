import ctypes
import logging
import multiprocessing
import os
import shutil
import tempfile

import pytest

from c4.system.configuration import (Configuration,
                                     DBClusterInfo, DeviceInfo,
                                     NodeInfo,
                                     PlatformInfo,
                                     Roles)
from c4.system.manager import SystemManager


logging.basicConfig(format='%(asctime)s [%(levelname)s] <%(processName)s> [%(name)s(%(filename)s:%(lineno)d)] - %(message)s', level=logging.INFO)
log = logging.getLogger(__name__)

@pytest.fixture()
def cleandir(request):
    """
    Create a new temporary directory and change the current working directory to it
    """
    oldCurrentWorkingDirectory = os.getcwd()
    newCurrentWorkingDirectory = tempfile.mkdtemp(dir="/dev/shm")
#     newCurrentWorkingDirectory = tempfile.mkdtemp(dir="/tmp")
    os.chdir(newCurrentWorkingDirectory)

    def removeTemporaryDirectory():
        os.chdir(oldCurrentWorkingDirectory)
        shutil.rmtree(newCurrentWorkingDirectory)
    request.addfinalizer(removeTemporaryDirectory)
    return newCurrentWorkingDirectory

@pytest.fixture
def system(request, temporaryDatabasePaths, cleandir, temporaryIPCPath, systemManagerAvailability):
    """
    Set up a basic system configuration
    """
    parameters = request.param if hasattr(request, "param") else {}
    passiveNodes = parameters.get("passiveNodes", 2)
    thinNodes = parameters.get("thinNodes", 0)
    electionTimeout = parameters.get("election.timeout", 5)
    heartbeatInitial = parameters.get("heartbeat.initial", 0)
    heartbeatInterval = parameters.get("heartbeat.interval", 2)
    heartbeatTimeout = parameters.get("heartbeat.timeout", 5)

    configuration = Configuration()
    platform = PlatformInfo("ha-test", "c4.system.platforms.ha.HA")
    configuration.addPlatform(platform)

    haDeviceInfo = DeviceInfo("ha", "c4.devices.ha.raft.Raft")
    haDeviceInfo.properties = {
        "election.timeout": electionTimeout,
        "heartbeat.initial": heartbeatInitial,
        "heartbeat.interval": heartbeatInterval,
        "heartbeat.timeout": heartbeatTimeout
    }
    if "event.handlers" in parameters:
        # TODO: adjust serialization in configuration.addNode() and other places to properly handle details
        haDeviceInfo.properties["event.handlers"] = {
            "events": parameters["event.handlers"].events,
            "handlers": parameters["event.handlers"].handlers
        }
    log.info(haDeviceInfo.toJSON(includeClassInfo=True, pretty=True))

    node1 = NodeInfo("node1", "ipc://node1-peer.ipc", role=Roles.ACTIVE)
    node1.addDevice(haDeviceInfo)

    configuration.addNode(node1)
    # TODO: this should automatically set role of the node to active
    configuration.addAlias("system-manager", "node1")

    nodeNumber = 2
    for passiveNodeNumber in range(passiveNodes):
        nodeName = "node{0}".format(nodeNumber + passiveNodeNumber)
        nodeAddress = "ipc://{0}-peer.ipc".format(nodeName)
        passiveNode = NodeInfo(nodeName, nodeAddress, role=Roles.PASSIVE)
        passiveNode.addDevice(haDeviceInfo)
        configuration.addNode(passiveNode)
    nodeNumber += passiveNodes

    for thinNodeNumber in range(thinNodes):
        nodeName = "node{0}".format(nodeNumber + thinNodeNumber)
        nodeAddress = "ipc://{0}-peer.ipc".format(nodeName)
        thinNode = NodeInfo(nodeName, nodeAddress, role=Roles.THIN)
        thinNode.addDevice(haDeviceInfo)
        configuration.addNode(thinNode)

    log.debug(configuration.toInfo().toJSON(pretty=True))

    systemSetup = {}
    systemManagerNodeInfo = configuration.getNode(configuration.getSystemManagerNodeName(), includeDevices=False)

    for node in configuration.getNodeNames():
        nodeInfo = configuration.getNode(node, includeDevices=False)
        clusterInfo = DBClusterInfo(nodeInfo.name, nodeInfo.address, systemManagerNodeInfo.address, role=nodeInfo.role)
        systemSetup[nodeInfo.name] = SystemManager(clusterInfo)

    def systemTeardown():
        log.info("stopping test")

        systemManagerNode = Configuration().getSystemManagerNodeName()
        activeSystemManager = systemSetup.pop(systemManagerNode)

        for node, systemManager in reversed(sorted(systemSetup.items())):
            log.debug("stopping %s", node)
            systemManager.setAvailable()
            systemManager.stop()

        log.debug("stopping active system manager %s", systemManagerNode)
        activeSystemManager.setAvailable()
        activeSystemManager.stop()

    request.addfinalizer(systemTeardown)

#     logging.getLogger("c4.devices.ha").setLevel(logging.DEBUG)
#     logging.getLogger("c4.messaging.zeromqMessaging.Peer").setLevel(logging.DEBUG)

    for node, systemManager in sorted(systemSetup.items()):
        log.debug("starting %s", node)
        systemManager.start()
    log.info("finished nodes setup, starting test")

    return systemSetup

@pytest.fixture
def systemManagerAvailability(monkeypatch):
    """
    Patch system manager to allow simulating availability issues
    """
    import c4.system.manager

    originalInit = c4.system.manager.SystemManager.__init__
    def __init__(self, clusterInfo, name="SM"):
        originalInit(self, clusterInfo, name="SM")
        self._available = multiprocessing.Value(ctypes.c_bool, True)
    monkeypatch.setattr("c4.system.manager.SystemManager.__init__", __init__)

    originalRouteMessage = c4.system.manager.SystemManager.routeMessage
    def routeMessage(self, To, envelopeString, upstreamComponent, downstreamComponent):
        if self._available.value:
            return originalRouteMessage(self, To, envelopeString, upstreamComponent, downstreamComponent)
        else:
            log.debug("ignoring message to '%s' because '%s' is currently unavailable", To, self.clusterInfo.node)
    monkeypatch.setattr("c4.system.manager.SystemManager.routeMessage", routeMessage)

    def setAvailable(self):
        with self._available.get_lock():
            self._available.value = True
    monkeypatch.setattr("c4.system.manager.SystemManager.setAvailable", setAvailable, raising=False)

    def setUnavailable(self):
        with self._available.get_lock():
            self._available.value = False
    monkeypatch.setattr("c4.system.manager.SystemManager.setUnavailable", setUnavailable, raising=False)

@pytest.fixture
def temporaryDatabasePaths(request, monkeypatch):
    """
    Create a new temporary directory and set c4.system.db.BACKUP_PATH
    and c4.system.db.DATABASE_PATH to it
    """
    newpath = tempfile.mkdtemp(dir="/dev/shm")
#     newpath = tempfile.mkdtemp(dir="/tmp")
    monkeypatch.setattr("c4.system.db.BACKUP_PATH", newpath)
    monkeypatch.setattr("c4.system.db.DATABASE_PATH", newpath)

    def removeTemporaryDirectory():
        shutil.rmtree(newpath)
    request.addfinalizer(removeTemporaryDirectory)
    return newpath

@pytest.fixture
def temporaryIPCPath(request, monkeypatch):
    """
    Create a new temporary directory and set c4.messaging.zeromqMessaging.DEFAULT_IPC_PATH to it
    """
    newpath = tempfile.mkdtemp(dir="/dev/shm")
#     newpath = tempfile.mkdtemp(dir="/tmp")
    monkeypatch.setattr("c4.messaging.zeromqMessaging.DEFAULT_IPC_PATH", newpath)

    def removeTemporaryDirectory():
        shutil.rmtree(newpath)
    request.addfinalizer(removeTemporaryDirectory)
    return newpath
