import ddt
from glusto.core import Glusto as g
from glustolibs.gluster.block_libs import (
    get_block_info,
    get_block_list,
)
from glustolibs.gluster.volume_ops import (
    get_volume_info,
    volume_start,
    volume_stop,
)
import pytest

from openshiftstoragelibs.baseclass import GlusterBlockBaseClass
from openshiftstoragelibs import exceptions
from openshiftstoragelibs.gluster_ops import (
    get_block_hosting_volume_name,
)
from openshiftstoragelibs.heketi_ops import (
    get_block_hosting_volume_list,
    get_total_free_space,
    heketi_blockvolume_create,
    heketi_blockvolume_delete,
    heketi_blockvolume_info,
    heketi_blockvolume_list,
    heketi_blockvolume_list_by_name_prefix,
    heketi_node_info,
    heketi_node_list,
    heketi_volume_create,
    heketi_volume_delete,
    heketi_volume_info,
    hello_heketi,
)
from openshiftstoragelibs.openshift_ops import (
    cmd_run_on_gluster_pod_or_node,
    get_default_block_hosting_volume_size,
    oc_rsh,
    restart_service_on_gluster_pod_or_node,
    wait_for_service_status_on_gluster_pod_or_node,
)
from openshiftstoragelibs import podcmd
from openshiftstoragelibs import utils


@ddt.ddt
class TestBlockVolumeOps(GlusterBlockBaseClass):
    """Class to test heketi block volume deletion with and without block
       volumes existing, heketi block volume list, heketi block volume info
       and heketi block volume creation with name and block volumes creation
       after manually creating a Block Hosting volume.
    """

    @pytest.mark.tier1
    def test_create_block_vol_after_host_vol_creation(self):
        """Validate block-device after manual block hosting volume creation
           using heketi
        """
        block_host_create_info = heketi_volume_create(
            self.heketi_client_node, self.heketi_server_url, 5,
            json=True, block=True)
        self.addCleanup(
            heketi_volume_delete, self.heketi_client_node,
            self.heketi_server_url, block_host_create_info["id"])

        block_vol = heketi_blockvolume_create(
            self.heketi_client_node, self.heketi_server_url, 1, json=True)
        self.addCleanup(
            heketi_blockvolume_delete, self.heketi_client_node,
            self.heketi_server_url, block_vol["id"])

    @pytest.mark.tier1
    def test_block_host_volume_delete_without_block_volumes(self):
        """Validate deletion of empty block hosting volume"""
        block_host_create_info = heketi_volume_create(
            self.heketi_client_node, self.heketi_server_url, 1, json=True,
            block=True)

        block_hosting_vol_id = block_host_create_info["id"]
        self.addCleanup(
            heketi_volume_delete, self.heketi_client_node,
            self.heketi_server_url, block_hosting_vol_id, raise_on_error=False)

        heketi_volume_delete(
            self.heketi_client_node, self.heketi_server_url,
            block_hosting_vol_id, json=True)

    @pytest.mark.tier1
    def test_block_volume_delete(self):
        """Validate deletion of gluster-block volume and capacity of used pool
        """
        block_vol = heketi_blockvolume_create(
            self.heketi_client_node, self.heketi_server_url, 1, json=True)
        self.addCleanup(
            heketi_blockvolume_delete, self.heketi_client_node,
            self.heketi_server_url, block_vol["id"], raise_on_error=False)

        heketi_blockvolume_delete(
            self.heketi_client_node, self.heketi_server_url,
            block_vol["id"], json=True)

        volume_list = heketi_blockvolume_list(
            self.heketi_client_node, self.heketi_server_url, json=True)
        self.assertNotIn(block_vol["id"], volume_list["blockvolumes"],
                         "The block volume has not been successfully deleted,"
                         " ID is %s" % block_vol["id"])

    @pytest.mark.tier1
    def test_block_volume_list(self):
        """Validate heketi blockvolume list command works as expected"""
        created_vol_ids = []
        for count in range(3):
            block_vol = heketi_blockvolume_create(
                self.heketi_client_node, self.heketi_server_url, 1, json=True)
            self.addCleanup(
                heketi_blockvolume_delete, self.heketi_client_node,
                self.heketi_server_url, block_vol["id"])

            created_vol_ids.append(block_vol["id"])

        volumes = heketi_blockvolume_list(
            self.heketi_client_node, self.heketi_server_url, json=True)

        existing_vol_ids = list(volumes.values())[0]
        for vol_id in created_vol_ids:
            self.assertIn(vol_id, existing_vol_ids,
                          "Block vol with '%s' ID is absent in the "
                          "list of block volumes." % vol_id)

    @pytest.mark.tier1
    def test_block_host_volume_delete_block_volume_delete(self):
        """Validate block volume and BHV removal using heketi"""
        free_space, nodenum = get_total_free_space(
            self.heketi_client_node,
            self.heketi_server_url)
        if nodenum < 3:
            self.skipTest("Skipping the test since number of nodes"
                          "online are less than 3")
        free_space_available = int(free_space / nodenum)
        default_bhv_size = get_default_block_hosting_volume_size(
            self.heketi_client_node, self.heketi_dc_name)
        if free_space_available < default_bhv_size:
            self.skipTest("Skipping the test since free_space_available %s"
                          "is less than the default_bhv_size %s"
                          % (free_space_available, default_bhv_size))
        h_volume_name = (
            "autotests-heketi-volume-%s" % utils.get_random_str())
        block_host_create_info = self.create_heketi_volume_with_name_and_wait(
            h_volume_name, default_bhv_size, json=True, block=True)

        block_vol_size = block_host_create_info["blockinfo"]["freesize"]
        block_hosting_vol_id = block_host_create_info["id"]
        block_vol_info = {"blockhostingvolume": "init_value"}
        while (block_vol_info['blockhostingvolume'] != block_hosting_vol_id):
            block_vol = heketi_blockvolume_create(
                self.heketi_client_node,
                self.heketi_server_url, block_vol_size,
                json=True, ha=3, auth=True)
            self.addCleanup(heketi_blockvolume_delete,
                            self.heketi_client_node,
                            self.heketi_server_url,
                            block_vol["id"], raise_on_error=True)
            block_vol_info = heketi_blockvolume_info(
                self.heketi_client_node, self.heketi_server_url,
                block_vol["id"], json=True)
        bhv_info = heketi_volume_info(
            self.heketi_client_node, self.heketi_server_url,
            block_hosting_vol_id, json=True)
        self.assertIn(
            block_vol_info["id"], bhv_info["blockinfo"]["blockvolume"])

    @pytest.mark.tier1
    @podcmd.GlustoPod()
    def test_validate_gluster_voloptions_blockhostvolume(self):
        """Validate gluster volume options which are set for
           block hosting volume"""
        options_to_validate = (
            ('performance.quick-read', 'off'),
            ('performance.read-ahead', 'off'),
            ('performance.io-cache', 'off'),
            ('performance.stat-prefetch', 'off'),
            ('performance.open-behind', 'off'),
            ('performance.readdir-ahead', 'off'),
            ('performance.strict-o-direct', 'on'),
            ('network.remote-dio', 'disable'),
            ('cluster.eager-lock', 'enable'),
            ('cluster.quorum-type', 'auto'),
            ('cluster.data-self-heal-algorithm', 'full'),
            ('cluster.locking-scheme', 'granular'),
            ('cluster.shd-max-threads', '8'),
            ('cluster.shd-wait-qlength', '10000'),
            ('features.shard', 'on'),
            ('features.shard-block-size', '64MB'),
            ('user.cifs', 'off'),
            ('server.allow-insecure', 'on'),
        )
        free_space, nodenum = get_total_free_space(
            self.heketi_client_node,
            self.heketi_server_url)
        if nodenum < 3:
            self.skipTest("Skip the test case since number of"
                          "online nodes is less than 3.")
        free_space_available = int(free_space / nodenum)
        default_bhv_size = get_default_block_hosting_volume_size(
            self.heketi_client_node, self.heketi_dc_name)
        if free_space_available < default_bhv_size:
            self.skipTest("Skip the test case since free_space_available %s"
                          "is less than the default_bhv_size %s ."
                          % (free_space_available, default_bhv_size))
        block_host_create_info = heketi_volume_create(
            self.heketi_client_node, self.heketi_server_url, default_bhv_size,
            json=True, block=True)
        self.addCleanup(heketi_volume_delete,
                        self.heketi_client_node,
                        self.heketi_server_url,
                        block_host_create_info["id"],
                        raise_on_error=True)
        bhv_name = block_host_create_info["name"]
        vol_info = get_volume_info('auto_get_gluster_endpoint',
                                   volname=bhv_name)
        self.assertTrue(vol_info, "Failed to get volume info %s" % bhv_name)
        self.assertIn("options", vol_info[bhv_name].keys())
        for k, v in options_to_validate:
            self.assertIn(k, vol_info[bhv_name]["options"].keys())
            self.assertEqual(v, vol_info[bhv_name]
                             ["options"][k])

    @pytest.mark.tier2
    @ddt.data(True, False)
    def test_create_blockvolume_with_different_auth_values(self, auth_value):
        """To validate block volume creation with different auth values"""
        # Block volume create with auth enabled
        block_vol = heketi_blockvolume_create(
            self.heketi_client_node, self.heketi_server_url, 1,
            auth=auth_value, json=True)
        self.addCleanup(
            heketi_blockvolume_delete, self.heketi_client_node,
            self.heketi_server_url, block_vol["id"])

        # Verify username and password are present
        block_vol_info = heketi_blockvolume_info(
            self.heketi_client_node, self.heketi_server_url,
            block_vol["id"], json=True)
        assertion_func = (self.assertNotEqual if auth_value
                          else self.assertEqual)
        assertion_msg_part = "not " if auth_value else ""
        assertion_func(
            block_vol_info["blockvolume"]["username"], "",
            ("Username is %spresent in %s", (assertion_msg_part,
                                             block_vol["id"])))
        assertion_func(
            block_vol_info["blockvolume"]["password"], "",
            ("Password is %spresent in %s", (assertion_msg_part,
                                             block_vol["id"])))

    @pytest.mark.tier1
    def test_block_volume_create_with_name(self):
        """Validate creation of block volume with name"""
        vol_name = "autotests-heketi-volume-%s" % utils.get_random_str()
        block_vol = heketi_blockvolume_create(
            self.heketi_client_node, self.heketi_server_url, 1,
            name=vol_name, json=True)
        self.addCleanup(
            heketi_blockvolume_delete, self.heketi_client_node,
            self.heketi_server_url, block_vol["id"])

        # check volume name through heketi-cli
        block_vol_info = heketi_blockvolume_info(
            self.heketi_client_node, self.heketi_server_url,
            block_vol["id"], json=True)
        self.assertEqual(
            block_vol_info["name"], vol_name,
            ("Block volume Names are not same %s as %s",
             (block_vol_info["name"], vol_name)))

    @pytest.mark.tier2
    @podcmd.GlustoPod()
    def test_create_max_num_blockhostingvolumes(self):
        num_of_bv = 10
        new_bhv_list, bv_list, g_nodes = [], [], []
        free_space, nodenum = get_total_free_space(
            self.heketi_client_node, self.heketi_server_url)
        if nodenum < 3:
            self.skipTest("Skip the test case since number of"
                          "online nodes is less than 3.")
        free_space_available = int(free_space / nodenum)
        default_bhv_size = get_default_block_hosting_volume_size(
            self.heketi_client_node, self.heketi_dc_name)
        # Get existing list of BHV's
        existing_bhv_list = get_block_hosting_volume_list(
            self.heketi_client_node, self.heketi_server_url)

        # Skip the test if available space is less than default_bhv_size
        if free_space_available < default_bhv_size:
            self.skipTest("Skip the test case since free_space_available %s"
                          "is less than space_required_for_bhv %s ."
                          % (free_space_available, default_bhv_size))

        # Create BHV's
        while free_space_available > default_bhv_size:
            block_host_create_info = heketi_volume_create(
                self.heketi_client_node, self.heketi_server_url,
                default_bhv_size, json=True, block=True)
            if block_host_create_info["id"] not in existing_bhv_list.keys():
                new_bhv_list.append(block_host_create_info["id"])
            self.addCleanup(
                heketi_volume_delete, self.heketi_client_node,
                self.heketi_server_url, block_host_create_info["id"],
                raise_on_error=False)

            free_size = block_host_create_info["blockinfo"]["freesize"]
            if free_size > num_of_bv:
                block_vol_size = int(free_size / num_of_bv)
            else:
                block_vol_size, num_of_bv = 1, free_size

            # Create specified number of BV's in BHV's created
            for i in range(0, num_of_bv):
                block_vol = heketi_blockvolume_create(
                    self.heketi_client_node, self.heketi_server_url,
                    block_vol_size, json=True, ha=3, auth=True)
                self.addCleanup(
                    heketi_blockvolume_delete, self.heketi_client_node,
                    self.heketi_server_url, block_vol["id"],
                    raise_on_error=False)
                bv_list.append(block_vol["id"])
            free_space_available = int(free_space_available - default_bhv_size)

        # Get gluster node ips
        h_nodes_ids = heketi_node_list(
            self.heketi_client_node, self.heketi_server_url)
        for h_node in h_nodes_ids[:2]:
            g_node = heketi_node_info(
                self.heketi_client_node, self.heketi_server_url, h_node,
                json=True)
            g_nodes.append(g_node['hostnames']['manage'][0])

        # Check if there is no crash in gluster related services & heketi
        services = (
            ("glusterd", "running"), ("gluster-blockd", "running"),
            ("tcmu-runner", "running"), ("gluster-block-target", "exited"))
        for g_node in g_nodes:
            for service, state in services:
                wait_for_service_status_on_gluster_pod_or_node(
                    self.ocp_client[0], service, 'active', state,
                    g_node, raise_on_error=False)
            out = hello_heketi(self.heketi_client_node, self.heketi_server_url)
            self.assertTrue(
                out, "Heketi server %s is not alive" % self.heketi_server_url)

        # Delete all the BHV's and BV's created
        for bv_volume in bv_list:
            heketi_blockvolume_delete(
                self.heketi_client_node, self.heketi_server_url, bv_volume)

        # Check if any blockvolume exist in heketi & gluster
        for bhv_volume in new_bhv_list[:]:
            heketi_vol_info = heketi_volume_info(
                self.heketi_client_node, self.heketi_server_url,
                bhv_volume, json=True)
            self.assertNotIn(
                "blockvolume", heketi_vol_info["blockinfo"].keys())
            gluster_vol_info = get_block_list(
                'auto_get_gluster_endpoint', volname="vol_%s" % bhv_volume)
            self.assertIsNotNone(
                gluster_vol_info, "Failed to get volume info %s" % bhv_volume)
            new_bhv_list.remove(bhv_volume)
            for blockvol in gluster_vol_info:
                self.assertNotIn("blockvol_", blockvol)
                heketi_volume_delete(
                    self.heketi_client_node, self.heketi_server_url,
                    bhv_volume)

        # Check if all blockhosting volumes are deleted from heketi
        self.assertFalse(new_bhv_list)

    @pytest.mark.tier2
    @podcmd.GlustoPod()
    def test_targetcli_when_block_hosting_volume_down(self):
        """Validate no inconsistencies occur in targetcli when block volumes
           are created with one block hosting volume down."""
        h_node, h_server = self.heketi_client_node, self.heketi_server_url
        cmd = ("targetcli ls | egrep '%s' || echo unavailable")
        error_msg = (
            "targetcli has inconsistencies when block devices are "
            "created with one block hosting volume %s is down")

        # Delete BHV which has no BV or fill it completely
        bhv_list = get_block_hosting_volume_list(h_node, h_server).keys()
        for bhv in bhv_list:
            bhv_info = heketi_volume_info(h_node, h_server, bhv, json=True)
            if not bhv_info["blockinfo"].get("blockvolume", []):
                heketi_volume_delete(h_node, h_server, bhv)
                continue
            free_size = bhv_info["blockinfo"].get("freesize", 0)
            if free_size:
                bv = heketi_volume_create(
                    h_node, h_server, free_size, json=True)
                self.addCleanup(
                    heketi_volume_delete, h_node, h_server, bv["id"])

        # Create BV
        bv = heketi_blockvolume_create(h_node, h_server, 2, json=True)
        self.addCleanup(heketi_blockvolume_delete, h_node, h_server, bv["id"])

        # Bring down BHV
        bhv_name = get_block_hosting_volume_name(h_node, h_server, bv["id"])
        ret, out, err = volume_stop("auto_get_gluster_endpoint", bhv_name)
        if ret != 0:
            err_msg = "Failed to stop gluster volume %s. error: %s" % (
                bhv_name, err)
            g.log.error(err_msg)
            raise AssertionError(err_msg)
        self.addCleanup(
            podcmd.GlustoPod()(volume_start), "auto_get_gluster_endpoint",
            bhv_name)

        ocp_node = self.ocp_master_node[0]
        gluster_block_svc = "gluster-block-target"
        self.addCleanup(
            wait_for_service_status_on_gluster_pod_or_node,
            ocp_node, gluster_block_svc,
            "active", "exited", gluster_node=self.gluster_servers[0])
        self.addCleanup(
            restart_service_on_gluster_pod_or_node, ocp_node,
            gluster_block_svc, self.gluster_servers[0])
        for condition in ("continue", "break"):
            restart_service_on_gluster_pod_or_node(
                ocp_node, gluster_block_svc,
                gluster_node=self.gluster_servers[0])
            wait_for_service_status_on_gluster_pod_or_node(
                ocp_node, gluster_block_svc,
                "active", "exited", gluster_node=self.gluster_servers[0])

            targetcli = cmd_run_on_gluster_pod_or_node(
                ocp_node, cmd % bv["id"], self.gluster_servers[0])
            if condition == "continue":
                self.assertEqual(
                    targetcli, "unavailable", error_msg % bhv_name)
            else:
                self.assertNotEqual(
                    targetcli, "unavailable", error_msg % bhv_name)
                break

            # Bring up the same BHV
            ret, out, err = volume_start("auto_get_gluster_endpoint", bhv_name)
            if ret != 0:
                err = "Failed to start gluster volume %s on %s. error: %s" % (
                    bhv_name, h_node, err)
                raise exceptions.ExecutionError(err)

    @pytest.mark.tier1
    @podcmd.GlustoPod()
    def test_heket_block_volume_info_with_gluster_block_volume_info(self):
        """Verify heketi block volume info with the backend gluster
        block volume info
        """
        h_node, h_server = self.heketi_client_node, self.heketi_server_url
        vol_size = 1
        h_block_vol = heketi_blockvolume_create(
            h_node, h_server, vol_size, auth=True, json=True)
        self.addCleanup(
            heketi_blockvolume_delete, h_node, h_server, h_block_vol["id"])

        h_block_vol["blockvolume"]["hosts"].sort()
        h_block_vol_gbid = h_block_vol["blockvolume"]["username"]
        h_block_vol_name = h_block_vol["name"]
        h_block_vol_paswd = h_block_vol["blockvolume"]["password"]
        h_block_vol_hosts = h_block_vol["blockvolume"]["hosts"]
        h_block_host_vol_id = h_block_vol["blockhostingvolume"]
        h_block_vol_ha = h_block_vol["hacount"]

        # Fetch heketi blockhostingvolume info
        h_bhv_info = heketi_volume_info(
            h_node, h_server, h_block_host_vol_id, json=True)
        err_msg = "Failed to get heketi blockhostingvolume info for {}"
        self.assertTrue(h_bhv_info, err_msg.format(h_block_host_vol_id))
        h_bhv_name = h_bhv_info['name']
        err_msg = "Failed to get heketi BHV name for heketi blockvolume Id {}"
        self.assertTrue(h_bhv_name, err_msg.format(h_block_host_vol_id))

        # Get gluster blockvolume list
        g_block_vol_list = get_block_list(
            'auto_get_gluster_endpoint', h_bhv_name)
        err_msg = ("Failed to get gluter blockvolume list {}"
                   .format(g_block_vol_list))
        self.assertTrue(g_block_vol_list, err_msg)

        err_msg = (
            "Heketi block volume {} not present in gluster blockvolumes {}"
            .format(h_block_vol_name, g_block_vol_list))
        self.assertIn(h_block_vol_name, g_block_vol_list, err_msg)

        g_block_info = get_block_info(
            'auto_get_gluster_endpoint', h_bhv_name, h_block_vol_name)
        g_block_info["EXPORTED ON"].sort()

        g_block_vol_hosts = g_block_info["EXPORTED ON"]
        g_block_vol_gbid = g_block_info["GBID"]
        g_block_vol_name = g_block_info["NAME"]
        g_block_vol_paswd = g_block_info["PASSWORD"]
        g_block_vol_id = g_block_info["VOLUME"][4:]
        g_block_vol_ha = g_block_info["HA"]

        # verfiy block device info and glusterblock volume info
        err_msg = ("Did not match {} from heketi {} and gluster {} side")
        self.assertEqual(
            h_block_vol_gbid, g_block_vol_gbid,
            err_msg.format(
                "GBID", h_block_vol_gbid, g_block_vol_gbid, err_msg))
        self.assertEqual(
            h_block_vol_name, g_block_vol_name,
            err_msg.format(
                "blockvolume", h_block_vol_name, g_block_vol_name, err_msg))
        self.assertEqual(
            h_block_vol_paswd, g_block_vol_paswd,
            err_msg.format(
                "password", h_block_vol_paswd, g_block_vol_paswd, err_msg))
        self.assertEqual(
            h_block_vol_hosts, g_block_vol_hosts,
            err_msg.format(
                "hosts", h_block_vol_hosts, g_block_vol_hosts, err_msg))
        self.assertEqual(
            h_block_host_vol_id, g_block_vol_id,
            err_msg.format(
                "blockhost vol id", h_block_host_vol_id,
                g_block_vol_id, err_msg))
        self.assertEqual(
            h_block_vol_ha, g_block_vol_ha,
            err_msg.format(
                "ha", h_block_vol_ha, g_block_vol_ha, err_msg))

    @pytest.mark.tier1
    def test_dynamic_provisioning_block_vol_with_custom_prefix(self):
        """Verify creation of block volume with custom prefix
        """
        node = self.ocp_master_node[0]
        prefix = "autotest-{}".format(utils.get_random_str())

        # cmd to get available space
        cmd_get_free_space = "df -h | grep '/mnt'| awk '{{print $4}}'"

        # cmd to create a 100M file
        cmd_run_io = 'dd if=/dev/zero of=/mnt/testfile bs=1024 count=102400'

        # Create sc with prefix
        sc_name = self.create_storage_class(
            sc_name_prefix=prefix,
            create_vol_name_prefix=True, vol_name_prefix=prefix)

        # Create pvc and wait for it to be in bound state
        pvc_name = self.create_and_wait_for_pvc(sc_name=sc_name, pvc_size=1)

        # Verify blockvolume list with prefix
        h_block_vol = heketi_blockvolume_list_by_name_prefix(
            self.heketi_client_node, self.heketi_server_url, prefix)
        self.assertIsNotNone(
            h_block_vol,
            "Failed to find blockvolume with prefix {}".format(prefix))
        self.assertTrue(
            h_block_vol[0][2].startswith(prefix),
            "Failed to create blockvolume with the prefix {}".format(prefix))

        # Create app pod
        dc_name, pod_name = self.create_dc_with_pvc(pvc_name)

        err_msg = ("Failed to get the free space for the mount point of the "
                   "app pod {} with error {}")
        # Get free space of app pod before IO run
        _, free_space_before, err = oc_rsh(node, pod_name, cmd_get_free_space)
        self.assertTrue(free_space_before, err_msg.format(pod_name, err))

        # Running IO on the app pod
        ret, _, err = oc_rsh(node, pod_name, cmd_run_io)
        self.assertFalse(
            ret, "Failed to run the Io with the error msg {}".format(err))

        # Get free space of app pod after IO run
        _, free_space_after, err = oc_rsh(node, pod_name, cmd_get_free_space)
        self.assertTrue(free_space_after, err_msg.format(pod_name, err))
        self.assertGreaterEqual(
            free_space_before, free_space_after,
            "Expecting free space in app pod before {} should be greater than"
            " {} as 100M file is created".format(
                free_space_before, free_space_after))
