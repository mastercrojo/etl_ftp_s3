import sys
import os

import unittest
from unittest.mock import patch, MagicMock

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir)))
from ftp.ftp_client import connect_ftp

class TestFTP(unittest.TestCase):

    @patch("ftp.ftp_client.FTP")
    def test_connect_ftp(self, mock_ftp):
        """Verifica que la conexión al servidor FTP se establece correctamente"""
        mock_ftp.return_value = MagicMock()
        ftp = connect_ftp()
        self.assertIsNotNone(ftp)
        ftp.login.assert_called_once()
        ftp.cwd.assert_called_once()

if __name__ == "__main__":
    unittest.main()
