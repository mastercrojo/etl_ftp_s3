from ftplib import FTP
from config.settings import FTP_HOST, FTP_USER, FTP_PASSWORD, FTP_DIR

def connect_ftp():
    """Conectar al servidor FTP"""
    ftp = FTP(FTP_HOST)
    ftp.login(FTP_USER, FTP_PASSWORD)
    ftp.cwd(FTP_DIR)
    return ftp
