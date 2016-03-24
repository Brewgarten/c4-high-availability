import logging
import time

import pytest

from c4.devices.ha.raft import RaftRoles
from c4.messaging.zeromqMessaging import RouterClient
from c4.system.configuration import Configuration
from c4.system.messages import Status


log = logging.getLogger(__name__)

class TestActiveNode():

    @pytest.mark.parametrize("system", [{"passiveNodes": 2}], indirect=True)
    def test_basic(self, system):

        # wait for at least one heart beat before making active node unavailable
        time.sleep(5)
        system["node1"].setUnavailable()

        time.sleep(10)

        configuration = Configuration()
        activeNode = configuration.getSystemManagerNodeName()
        assert activeNode != "node1"

        client = RouterClient(activeNode)
        # TODO: verify other properties: term, responsible for, etc.
        status = client.sendRequest(Status("{activeNode}/ha".format(activeNode=activeNode)))
        assert status.role == RaftRoles.LEADER

class TestPassiveNode():

    @pytest.mark.parametrize("system", [{"passiveNodes": 2}], indirect=True)
    def test_basic(self, system):

        # wait for at least one heart beat before making passive node unavailable
        time.sleep(5)
        system["node2"].setUnavailable()

        time.sleep(10)

        configuration = Configuration()
        activeNode = configuration.getSystemManagerNodeName()

        client = RouterClient(activeNode)
        # TODO: verify other properties: term, responsible for, etc.
        status = client.sendRequest(Status("{activeNode}/ha".format(activeNode=activeNode)), timeout=1)
        log.info(status.toJSON(pretty=True))

        # TODO: for now include failed node in status but eventually implement callbacks
        assert "node2/ha" in status.unavailable

class TestThinNode():

    @pytest.mark.parametrize("system", [{"passiveNodes": 2, "thinNodes": 1}], indirect=True)
    def test_basic(self, system):

        # wait for at least one heart beat before making passive node unavailable
        time.sleep(5)
        system["node4"].setUnavailable()

        time.sleep(10)

        configuration = Configuration()
        activeNode = configuration.getSystemManagerNodeName()

        client = RouterClient(activeNode)
        # TODO: verify other properties: term, responsible for, etc.
        status = client.sendRequest(Status("{activeNode}/ha".format(activeNode=activeNode)), timeout=1)
        log.info(status.toJSON(pretty=True))

        # TODO: for now include failed node in status but eventually implement callbacks
        assert "node4/ha" in status.unavailable
