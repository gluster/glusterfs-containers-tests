from cnslibs.common.baseclass import BaseClass
from cnslibs.common import heketi_ops
from cnslibs.common import heketi_version
from cnslibs.common import openshift_ops


class TestHeketiServerStateExamineGluster(BaseClass):

    def setUp(self):
        self.node = self.ocp_master_node[0]
        version = heketi_version.get_heketi_version(self.heketi_client_node)
        if version < '8.0.0-7':
            self.skipTest("heketi-client package %s does not support server "
                          "state examine gluster" % version.v_str)

    def test_volume_inconsistencies(self):
        # Examine Gluster cluster and Heketi that there is no inconsistencies
        out = heketi_ops.heketi_examine_gluster(
            self.heketi_client_node, self.heketi_server_url)
        if ("heketi volume list matches with volume list of all nodes"
                not in out['report']):
            self.skipTest(
                "heketi and Gluster are inconsistent to each other")

        # create volume
        vol = heketi_ops.heketi_volume_create(
            self.heketi_client_node, self.heketi_server_url, 1, json=True)
        self.addCleanup(
            heketi_ops.heketi_volume_delete, self.heketi_client_node,
            self.heketi_server_url, vol['id'])

        # delete volume from gluster cluster directly
        openshift_ops.cmd_run_on_gluster_pod_or_node(
            self.node,
            "gluster vol stop %s force --mode=script" % vol['name'])
        openshift_ops.cmd_run_on_gluster_pod_or_node(
            self.node,
            "gluster vol delete %s --mode=script" % vol['name'])

        # verify that heketi is reporting inconsistencies
        out = heketi_ops.heketi_examine_gluster(
            self.heketi_client_node, self.heketi_server_url)
        self.assertNotIn(
            "heketi volume list matches with volume list of all nodes",
            out['report'])
