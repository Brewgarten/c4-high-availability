import logging
import time

import pytest

from c4.devices.ha.raft import (CheckHeartbeatEvent,
                                EventHandler, EventHandlerInfo, EventHandlerProxy,
                                HeartbeatProcess, PerformHeartbeatEvent)
from c4.system.backend import Backend
from c4.system.configuration import Roles, SharedClusterInfo, States
from c4.utils.util import getFullModuleName


log = logging.getLogger(__name__)

@pytest.fixture
def testEventHandlerInfo():
    """
    Event handler test information
    """
    info = EventHandlerInfo()
    info.handlers = [getFullModuleName(SampleEventHandler) + ".SampleEventHandler"]
    info.events = {
        "checkHeartbeat": [
            getFullModuleName(checkHeartbeat) + ".checkHeartbeat",
            getFullModuleName(checkHeartbeat2) + ".checkHeartbeat2"
        ],
        "handleNewLeaderElected": getFullModuleName(handleNewLeaderElected) + ".handleNewLeaderElected",
        "handleNodeLeave": getFullModuleName(handleNodeLeave) + ".handleNodeLeave",
        "performHeartbeat": getFullModuleName(performHeartbeat) + ".performHeartbeat"
    }
    return info

class SampleEventHandler(EventHandler):

    def performHeartbeat(self, event):
        event.details["SampleEventHandler.performHeartbeat"] = True

    def checkHeartbeat(self, event):
        return event.details.get("SampleEventHandler.performHeartbeat", False) == True

def checkHeartbeat(event):
    return event.details.get("performHeartbeat", False) == True

def checkHeartbeat2(event):
    return event.details.get("performHeartbeat", False) == True

def handleNewLeaderElected(event):
    log.info("event handling new leader '%s' elected", event.newLeader)

def handleNodeLeave(event):
    log.info("event handling node '%s' leave", event.node)

def performHeartbeat(event):
    event.details["performHeartbeat"] = True

def test_EventHandlerProxy(testEventHandlerInfo, temporaryBackend):

    details = {}
    handlers = EventHandlerProxy(testEventHandlerInfo)
    handlers.performHeartbeat(PerformHeartbeatEvent("testClusterInfoPlaceholder", details))
    assert details["SampleEventHandler.performHeartbeat"]
    assert details["performHeartbeat"]

    clusterInfo = SharedClusterInfo(Backend(), "testNode", "testAddress", "testSystemManagerAddress", Roles.ACTIVE, States.RUNNING)

    results = handlers.checkHeartbeat(CheckHeartbeatEvent(clusterInfo, "testNode", details))
    assert results["test_handlers.SampleEventHandler"]
    assert results["test_handlers.checkHeartbeat"]
    assert results["test_handlers.checkHeartbeat2"]

def test_HeartbeatProcess(testEventHandlerInfo, temporaryBackend):

    clusterInfo = SharedClusterInfo(Backend(), "testNode", "testAddress", "testSystemManagerAddress", Roles.ACTIVE, States.RUNNING)

    handlers = EventHandlerProxy(testEventHandlerInfo)
    heartbeatProcess = HeartbeatProcess(clusterInfo, handlers, {"heartbeat.perform.interval": 1})
    heartbeatProcess.start()

    time.sleep(2)

    heartbeatProcess.stopFlag.set()
    heartbeatProcess.join()
    assert heartbeatProcess.details["SampleEventHandler.performHeartbeat"]
    assert heartbeatProcess.details["performHeartbeat"]

