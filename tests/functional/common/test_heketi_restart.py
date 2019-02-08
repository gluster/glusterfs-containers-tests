from jsondiff import diff

from cnslibs.common.baseclass import BaseClass
from cnslibs.common.heketi_ops import (
    heketi_topology_info,
    hello_heketi,
    heketi_volume_create,
    heketi_volume_delete
)
from cnslibs.common.openshift_ops import (
    get_pod_name_from_dc,
    oc_delete,
    wait_for_pod_be_ready,
    wait_for_resource_absence)


class TestRestartHeketi(BaseClass):

    def test_restart_heketi_pod(self):
        """Validate restarting heketi pod"""

        # create heketi volume
        vol_info = heketi_volume_create(self.heketi_client_node,
                                        self.heketi_server_url,
                                        size=1, json=True)
        self.assertTrue(vol_info, "Failed to create heketi volume of size 1")
        self.addCleanup(
            heketi_volume_delete, self.heketi_client_node,
            self.heketi_server_url, vol_info['id'], raise_on_error=False)
        topo_info = heketi_topology_info(self.heketi_client_node,
                                         self.heketi_server_url,
                                         json=True)

        # get heketi-pod name
        heketi_pod_name = get_pod_name_from_dc(self.ocp_master_node[0],
                                               self.heketi_dc_name)

        # delete heketi-pod (it restarts the pod)
        oc_delete(self.ocp_master_node[0], 'pod', heketi_pod_name)
        wait_for_resource_absence(self.ocp_master_node[0],
                                  'pod', heketi_pod_name)

        # get new heketi-pod name
        heketi_pod_name = get_pod_name_from_dc(self.ocp_master_node[0],
                                               self.heketi_dc_name)
        wait_for_pod_be_ready(self.ocp_master_node[0],
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
        heketi_volume_delete(
            self.heketi_client_node, self.heketi_server_url, vol_info['id'])
