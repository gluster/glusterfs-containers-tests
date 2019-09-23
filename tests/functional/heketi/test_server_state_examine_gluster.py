import ddt

from openshiftstoragelibs.baseclass import BaseClass
from openshiftstoragelibs import heketi_ops
from openshiftstoragelibs import heketi_version
from openshiftstoragelibs import openshift_ops


@ddt.ddt
class TestHeketiServerStateExamineGluster(BaseClass):

    def setUp(self):
        super(TestHeketiServerStateExamineGluster, self).setUp()
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

    @ddt.data('', 'block')
    def test_compare_real_vol_count_with_db_check_info(self, vol_type):
        """Validate file/block volumes using heketi db check."""

        # Create File/Block volume
        block_vol = getattr(heketi_ops, 'heketi_%svolume_create' % vol_type)(
            self.heketi_client_node, self.heketi_server_url, 1, json=True)
        self.addCleanup(
            getattr(heketi_ops, 'heketi_%svolume_delete' % vol_type),
            self.heketi_client_node, self.heketi_server_url, block_vol["id"])

        # Check Heketi DB using Heketi CLI
        db_result = heketi_ops.heketi_db_check(
            self.heketi_client_node, self.heketi_server_url)
        vol_count = db_result["%svolumes" % vol_type]["total"]
        vol_list = getattr(heketi_ops, 'heketi_%svolume_list' % vol_type)(
            self.heketi_client_node, self.heketi_server_url, json=True)
        count = len(vol_list["%svolumes" % vol_type])
        self.assertEqual(
            count, vol_count,
            "%svolume count doesn't match expected "
            "result %s, actual result is %s" % (vol_type, count, vol_count))

    def test_compare_node_count_with_db_check_info(self):
        """Validate nodes count using heketi db check"""

        # Check heketi db
        db_result = heketi_ops.heketi_db_check(
            self.heketi_client_node, self.heketi_server_url)
        db_nodes_count = db_result["nodes"]["total"]
        nodes_list = heketi_ops.heketi_node_list(
            self.heketi_client_node, self.heketi_server_url, json=True)
        calculated_nodes_count = len(nodes_list)
        self.assertEqual(
            calculated_nodes_count, db_nodes_count,
            "Nodes count from 'DB check' (%s) doesn't match calculated nodes "
            "count (%s)." % (db_nodes_count, calculated_nodes_count))
