from jsondiff import diff

from cnslibs.common.heketi_libs import HeketiClientSetupBaseClass
from cnslibs.common.heketi_ops import (
    hello_heketi,
    heketi_volume_create,
    heketi_topology_info)
from cnslibs.common.openshift_ops import (
    get_pod_name_from_dc,
    oc_delete,
    wait_for_pod_be_ready,
    wait_for_resource_absence)


class TestRestartHeketi(HeketiClientSetupBaseClass):

    def test_restart_heketi_pod(self):
        """ CNS-450 Restarting heketi pod """

        # create heketi volume
        vol_info = heketi_volume_create(self.heketi_client_node,
                                        self.heketi_server_url,
                                        size=1, json=True)
        self.assertTrue(vol_info, "Failed to create heketi volume of size 1")
        self.addCleanup(self.delete_volumes, vol_info['id'])
        topo_info = heketi_topology_info(self.heketi_client_node,
                                         self.heketi_server_url,
                                         json=True)

        # get heketi-pod name
        heketi_pod_name = get_pod_name_from_dc(self.ocp_master_node,
                                               self.heketi_dc_name)

        # delete heketi-pod (it restarts the pod)
        oc_delete(self.ocp_master_node, 'pod', heketi_pod_name)
        wait_for_resource_absence(self.ocp_master_node,
                                  'pod', heketi_pod_name)

        # get new heketi-pod name
        heketi_pod_name = get_pod_name_from_dc(self.ocp_master_node,
                                               self.heketi_dc_name)
        wait_for_pod_be_ready(self.ocp_master_node,
                              heketi_pod_name)

        # check heketi server is running
        self.assertTrue(
            hello_heketi(self.heketi_client_node, self.heketi_server_url),
            "Heketi server %s is not alive" % self.heketi_server_url
        )

        # compare the topology
        new_topo_info = heketi_topology_info(self.heketi_client_node,
                                             self.heketi_server_url,
                                             json=True)
        self.assertEqual(new_topo_info, topo_info, "topology info is not same,"
                         " difference - %s" % diff(topo_info, new_topo_info))

        # create new volume
        vol_info = heketi_volume_create(self.heketi_client_node,
                                        self.heketi_server_url,
                                        size=2, json=True)
        self.assertTrue(vol_info, "Failed to create heketi volume of size 20")
        self.delete_volumes(vol_info['id'])
