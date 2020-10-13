from glusto.core import Glusto as g
import pytest

from openshiftstoragelibs import baseclass
from openshiftstoragelibs import heketi_ops


class TestHeketiAuthenticationFromOCPClient(baseclass.BaseClass):
    """Class to test heketi-client authentication"""

    @pytest.mark.tier1
    def test_heketi_authentication_with_user_credentials(self):
        """Heketi command authentication with invalid and valid credentials"""

        h_client, h_server = self.heketi_client_node, self.heketi_server_url
        err_msg = "Error: Invalid JWT token: Token missing iss claim"

        # Run heketi commands with invalid credentials
        for each_cmd in ("volume list", "topology info"):
            cmd = "timeout 120 heketi-cli -s  {} {}".format(
                self.heketi_server_url, each_cmd)
            ret, _, err = g.run(h_client, cmd)
            self.assertTrue(ret, "Command execution with invalid credentials"
                                 " should not succeed")
            self.assertEqual(
                err_msg, err.strip(), "Error is different from the command"
                                      " execution {}".format(err.strip()))

        # Run heketi commands with valid credentials
        kwar = {'json_arg': True, 'secret': self.heketi_cli_key,
                'user': self.heketi_cli_user}
        heketi_ops.heketi_volume_list(h_client, h_server, **kwar)
        heketi_ops.heketi_topology_info(h_client, h_server, **kwar)
