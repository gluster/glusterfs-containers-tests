#!/usr/bin/python

from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ConfigError
from cnslibs.common.heketi_libs import HeketiClientSetupBaseClass
from cnslibs.common.heketi_ops import (heketi_volume_create,
                                       heketi_volume_list,
                                       heketi_volume_delete)
from cnslibs.common import heketi_ops, podcmd
from cnslibs.common.openshift_ops import oc_rsh, get_ocp_gluster_pod_names


class TestHeketiVolume(HeketiClientSetupBaseClass):
    """
    Check /etc/fstab entry
    """
    @classmethod
    def setUpClass(cls):
        super(TestHeketiVolume, cls).setUpClass()
        cls.volume_size = cls.heketi_volume['size']

    @podcmd.GlustoPod()
    def test_to_check_entry_in_fstab_file(self):
        """
        Create a heketi volume and check entry
        in /etc/fstab and delete heketi volume
        and check corresponding brick entry must
        be removed
        """

        # Create heketi volume
        g.log.info("Creating a heketi volume")
        out = heketi_volume_create(self.heketi_client_node,
                                   self.heketi_server_url,
                                   self.volume_size, json=True)
        self.assertTrue(out, ("Failed to create heketi volume "
                        "of size %s" % str(self.volume_size)))
        g.log.info("Heketi volume successfully created" % out)
        self.volume_id = out["bricks"][0]["volume"]
        path = []
        for i in out["bricks"]:
            path.append(i["path"].rstrip("/brick"))

        # Listing heketi volumes
        g.log.info("List heketi volumes")
        out = heketi_volume_list(self.heketi_client_node,
                                 self.heketi_server_url)
        self.assertTrue(out, ("Failed to list heketi volumes"))
        g.log.info("Heketi volume successfully listed")

        gluster_pod = get_ocp_gluster_pod_names(
            self.heketi_client_node)[1]

        cmd = "oc rsync "+ gluster_pod +":/var/lib/heketi/fstab /tmp"
        out = g.run(self.heketi_client_node, cmd)
        self.assertTrue(out, ("Failed to copy the file"))
        g.log.info("Copied the file")
        out = g.run_local("scp -r root@" +self.heketi_client_node+":/tmp/fstab /tmp/file.txt")
        self.assertTrue(out, ("Failed to copy a file to /tmp/file.txt"))
        g.log.info("Successfully copied to /tmp/file.txt")
        out = g.run_local("ls /tmp")
        self.assertTrue(out, ("Failed to list"))
        g.log.info("Successfully listed")

        # open /tmp/fstab file
        datafile = open("/tmp/file.txt")
        # Check if the brick is mounted
        for i in path:
            string_to_search = i
            rcode, rout, rerr = g.run_local('grep %s %s' % (string_to_search, "/tmp/file.txt"))
            if rcode == 0:
                g.log.info("Brick %s is mounted" % i)
        datafile.close()

        out = g.run(self.heketi_client_node, "rm -rf /tmp/fstab")
        self.assertTrue(out, ("Failed to delete a file /tmp/fstab"))
        g.log.info("Successfully removed /tmp/fstab")
        out = g.run_local("rm -rf /tmp/file.txt")
        self.assertTrue(out, ("Failed to delete a file /tmp/file.txt"))
        g.log.info("Successfully removed /tmp/file.txt")

        # Delete heketi volume
        g.log.info("Deleting heketi volumes")
        out = heketi_volume_delete(self.heketi_client_node,
                                   self.heketi_server_url,
                                   self.volume_id)
        self.assertTrue(out, ("Failed to delete "
                        "heketi volume %s" % self.volume_id))
        g.log.info("Heketi volume successfully deleted %s" % self.volume_id)

        # Listing heketi volumes
        g.log.info("List heketi volumes")
        out = heketi_volume_list(self.heketi_client_node,
                                 self.heketi_server_url)
        self.assertTrue(out, ("Failed to list or No volumes to list"))
        g.log.info("Heketi volume successfully listed")

        # Check entry /etc/fstab
        gluster_pod = get_ocp_gluster_pod_names(
                self.heketi_client_node)[0]

        cmd = "oc rsync "+ gluster_pod +":/var/lib/heketi/fstab /"
        out = g.run(self.heketi_client_node, cmd)
        self.assertTrue(out, ("Failed to copy the file"))
        g.log.info("Copied the file")
        out = g.run_local("scp -r root@" +self.heketi_client_node+":/fstab /tmp/newfile.txt")
        self.assertTrue(out, ("Failed to copy to the file newfile.txt"))
        g.log.info("Successfully copied to the file newfile.txt")
        out = g.run_local("ls /tmp")
        self.assertTrue(out, ("Failed to list"))
        g.log.info("Successfully listed")

        # open /tmp/newfile.txt file
        datafile = open("/tmp/newfile.txt")
        # Check if the brick is mounted
        for i in path:
            string_to_search = i
            rcode, rout, rerr = g.run_local('grep %s %s' % (string_to_search, "/tmp/newfile.txt"))
            if rcode == 0:
                raise ConfigError("Particular %s brick entry is found" % i)
        datafile.close()

        out = g.run(self.heketi_client_node, "rm -rf /fstab")
        self.assertTrue(out, ("Failed to delete a file /fstab"))
        g.log.info("Successfully removed /fstab")
        out = g.run_local("rm -rf /tmp/newfile.txt")
        self.assertTrue(out, ("Failed to delete a file /tmp/newfile.txt"))
        g.log.info("Successfully removed /tmp/file.txt")
