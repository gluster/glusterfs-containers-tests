import pytest

from openshiftstoragelibs.baseclass import BaseClass
from openshiftstoragelibs import command
from openshiftstoragelibs.heketi_ops import (
    heketi_blockvolume_create,
    heketi_blockvolume_delete)
from openshiftstoragelibs.openshift_ops import (
    get_pod_name_from_dc,
    oc_delete,
    wait_for_pod_be_ready,
    wait_for_resource_absence,
)


class TestRestartGlusterBlockPod(BaseClass):

    @pytest.mark.tier1
    def test_restart_gluster_block_provisioner_pod(self):
        """Restart gluster-block provisioner pod."""

        # Get glusterblock provisioner dc name
        cmd = ("oc get dc | awk '{ print $1 }' | "
               "grep -e glusterblock -e provisioner")
        dc_name = command.cmd_run(cmd, self.ocp_master_node[0], True)

        # create heketi block volume
        vol_info = heketi_blockvolume_create(self.heketi_client_node,
                                             self.heketi_server_url,
                                             size=5, json=True)
        self.assertTrue(vol_info, "Failed to create heketi block"
                        "volume of size 5")
        self.addCleanup(heketi_blockvolume_delete, self.heketi_client_node,
                        self.heketi_server_url, vol_info['id'])

        # restart gluster-block-provisioner-pod
        pod_name = get_pod_name_from_dc(self.ocp_master_node[0], dc_name)
        oc_delete(self.ocp_master_node[0], 'pod', pod_name)
        wait_for_resource_absence(self.ocp_master_node[0], 'pod', pod_name)

        # new gluster-pod name
        pod_name = get_pod_name_from_dc(self.ocp_master_node[0], dc_name)
        wait_for_pod_be_ready(self.ocp_master_node[0], pod_name)

        # create new heketi block volume
        vol_info = heketi_blockvolume_create(self.heketi_client_node,
                                             self.heketi_server_url,
                                             size=2, json=True)
        self.assertTrue(vol_info, "Failed to create heketi block"
                        "volume of size 2")
        heketi_blockvolume_delete(self.heketi_client_node,
                                  self.heketi_server_url,
                                  vol_info['id'])
