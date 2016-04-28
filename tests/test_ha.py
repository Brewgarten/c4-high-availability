import logging
import time

import pytest

from c4.devices.ha.raft import RaftRoles, EventHandlerInfo
from c4.messaging.zeromqMessaging import RouterClient
from c4.system.backend import Backend
from c4.system.messages import Status
from c4.utils.util import getFullModuleName, SharedDictWithLock


log = logging.getLogger(__name__)

eventHandlerResults = SharedDictWithLock()

@pytest.fixture
def cleanEventHandlerResults():
    eventHandlerResults.lock.acquire()
    for key in eventHandlerResults.keys():
        del eventHandlerResults[key]
    eventHandlerResults.lock.release()

def handleNewLeaderElected(event):
    log.info("event handling new leader '%s' elected", event.newLeader)
    eventHandlerResults["handleNewLeaderElected"] = event.newLeader

def handleNodeJoin(event):
    log.info("event handling node '%s' join", event.node)

def handleNodeLeave(event):
    log.info("event handling node '%s' leave", event.node)
    eventHandlerResults["handleNodeLeave"] = event.node

eventHandlers = EventHandlerInfo()
eventHandlers.events = {
    "handleNewLeaderElected": getFullModuleName(handleNewLeaderElected) + ".handleNewLeaderElected",
    "handleNodeJoin": getFullModuleName(handleNodeJoin) + ".handleNodeJoin",
    "handleNodeLeave": getFullModuleName(handleNodeLeave) + ".handleNodeLeave",
}

class TestActiveNode():

    @pytest.mark.parametrize("system", [{"passiveNodes": 2, "event.handlers": eventHandlers}], indirect=True)
    def test_basic(self, system, cleanEventHandlerResults):

        # wait for at least one heart beat before making active node unavailable
        time.sleep(5)
        system["node1"].setUnavailable()

        time.sleep(10)

        configuration = Backend().configuration
        activeNode = configuration.getSystemManagerNodeName()
        assert activeNode != "node1"
        assert eventHandlerResults["handleNewLeaderElected"] != "node1"

        client = RouterClient(activeNode)
        # TODO: verify other properties: term, responsible for, etc.
        status = client.sendRequest(Status("{activeNode}/ha".format(activeNode=activeNode)))
        assert status.role == RaftRoles.LEADER

class TestPassiveNode():

    @pytest.mark.parametrize("system", [{"passiveNodes": 2, "event.handlers": eventHandlers}], indirect=True)
    def test_basic(self, system, cleanEventHandlerResults):

        # wait for at least one heart beat before making passive node unavailable
        time.sleep(5)
        system["node2"].setUnavailable()

        time.sleep(10)

        configuration = Backend().configuration
        activeNode = configuration.getSystemManagerNodeName()

        client = RouterClient(activeNode)
        # TODO: verify other properties: term, responsible for, etc.
        status = client.sendRequest(Status("{activeNode}/ha".format(activeNode=activeNode)), timeout=1)
        log.info(status.toJSON(pretty=True))

        assert "node2" in status.unavailable
        assert eventHandlerResults["handleNodeLeave"] == "node2"

class TestThinNode():

    @pytest.mark.parametrize("system", [{
                                "passiveNodes": 2,
                                "thinNodes": 1,
                                "event.handlers": eventHandlers
                             }],
                             indirect=True)
    def test_basic(self, system):

        # wait for at least one heart beat before making passive node unavailable
        time.sleep(5)
        system["node4"].setUnavailable()

        time.sleep(10)

        configuration = Backend().configuration
        activeNode = configuration.getSystemManagerNodeName()

        client = RouterClient(activeNode)
        # TODO: verify other properties: term, responsible for, etc.
        status = client.sendRequest(Status("{activeNode}/ha".format(activeNode=activeNode)), timeout=1)
        log.info(status.toJSON(pretty=True))

        assert "node4" in status.unavailable
        assert eventHandlerResults["handleNodeLeave"] == "node4"

