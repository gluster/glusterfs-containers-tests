import pytest

from openshiftstoragelibs.baseclass import BaseClass
from openshiftstoragelibs.gluster_ops import (
    get_gluster_vol_status,
)
from openshiftstoragelibs.heketi_ops import (
    heketi_node_disable,
    heketi_node_enable,
    heketi_node_list,
)
from openshiftstoragelibs.openshift_ops import (
    get_gluster_vol_info_by_pvc_name,
)


class TestBrickMux(BaseClass):
    '''
     Class that contains BrickMux test cases.
    '''

    def setUp(self):
        super(TestBrickMux, self).setUp()
        self.node = self.ocp_master_node[0]

    @pytest.mark.tier1
    def test_brick_multiplex_pids_with_diff_vol_option_values(self):
        """Test Brick Pid's should be same when values of vol options are diff
        """
        h_client, h_url = self.heketi_client_node, self.heketi_server_url
        # Disable heketi nodes except first three nodes
        h_nodes_list = heketi_node_list(h_client, h_url)
        for node_id in h_nodes_list[3:]:
            heketi_node_disable(h_client, h_url, node_id)
            self.addCleanup(heketi_node_enable, h_client, h_url, node_id)

        # Create storage class with diff volumeoptions
        sc1 = self.create_storage_class(volumeoptions='user.heketi.abc 1')
        sc2 = self.create_storage_class(volumeoptions='user.heketi.abc 2')
        # Create PVC's with above SC
        pvc1 = self.create_and_wait_for_pvcs(sc_name=sc1)
        pvc2 = self.create_and_wait_for_pvcs(sc_name=sc2)

        # Get vol info and status
        vol_info1 = get_gluster_vol_info_by_pvc_name(self.node, pvc1[0])
        vol_info2 = get_gluster_vol_info_by_pvc_name(self.node, pvc2[0])
        vol_status1 = get_gluster_vol_status(vol_info1['gluster_vol_id'])
        vol_status2 = get_gluster_vol_status(vol_info2['gluster_vol_id'])

        # Verify vol options
        err_msg = ('Volume option "user.heketi.abc %s" did not got match for '
                   'volume %s in gluster vol info')
        self.assertEqual(
            vol_info1['options']['user.heketi.abc'], '1',
            err_msg % (1, vol_info1['gluster_vol_id']))
        self.assertEqual(
            vol_info2['options']['user.heketi.abc'], '2',
            err_msg % (2, vol_info2['gluster_vol_id']))

        # Get the PID's and match them
        pids1 = set()
        for brick in vol_info1['bricks']['brick']:
            host, bname = brick['name'].split(":")
            pids1.add(vol_status1[host][bname]['pid'])

        pids2 = set()
        for brick in vol_info2['bricks']['brick']:
            host, bname = brick['name'].split(":")
            pids2.add(vol_status2[host][bname]['pid'])

        err_msg = ('Pids of both the volumes %s and %s are expected to be'
                   'same. But got the different Pids "%s" and "%s".' %
                   (vol_info1['gluster_vol_id'], vol_info2['gluster_vol_id'],
                    pids1, pids2))
        self.assertEqual(pids1, pids2, err_msg)
