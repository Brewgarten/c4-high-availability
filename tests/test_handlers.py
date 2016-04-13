import logging
import time

import pytest

from c4.devices.ha.raft import (EventHandler, EventHandlerInfo, EventHandlerProxy,
                                HeartbeatProcess)
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

    def performHeartbeat(self, details):
        details["SampleEventHandler.performHeartbeat"] = True

    def checkHeartbeat(self, details):
        return details.get("SampleEventHandler.performHeartbeat", False) == True

def checkHeartbeat(details):
    return details.get("performHeartbeat", False) == True

def checkHeartbeat2(details):
    return details.get("performHeartbeat", False) == True

def handleNewLeaderElected(name):
    log.info("event handling new leader '%s' elected", name)

def handleNodeLeave(name):
    log.info("event handling node '%s' leave", name)

def performHeartbeat(details):
    details["performHeartbeat"] = True

def test_EventHandlerProxy(testEventHandlerInfo):

    details = {}
    handlers = EventHandlerProxy(testEventHandlerInfo)
    handlers.performHeartbeat(details)
    assert details["SampleEventHandler.performHeartbeat"]
    assert details["performHeartbeat"]

    results = handlers.checkHeartbeat(details)
    assert results["test_handlers.SampleEventHandler"]
    assert results["test_handlers.checkHeartbeat"]
    assert results["test_handlers.checkHeartbeat2"]

def test_HeartbeatProcess(testEventHandlerInfo):

    handlers = EventHandlerProxy(testEventHandlerInfo)
    heartbeatProcess = HeartbeatProcess("test", handlers, {"heartbeat.perform.interval": 1})
    heartbeatProcess.start()

    time.sleep(2)

    heartbeatProcess.stopFlag.set()
    heartbeatProcess.join()
    assert heartbeatProcess.details["SampleEventHandler.performHeartbeat"]
    assert heartbeatProcess.details["performHeartbeat"]

