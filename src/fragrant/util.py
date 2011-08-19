from paramiko.transport import Transport
import paramiko
import socket
from fragrant.exceptions import SshError
import logging

def check_port(host, port):
    """
    Return True if port is open on host.
    """
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(2)
    try:
        s.connect((host, port))
    except socket.error as e:
        if e.errno in (61,):
            return False
        else:
            raise
    else:
        s.close()
        return True

def check_ssh_up(host, port):
    """
    Return True if SSH connectivity exists.

    First checks if port is open, then tries to see if it response to SSH connectivity.
    """
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(2)
        try:
            s.connect((host, port))
        except socket.error as e:
            if e.errno in (61,):
                return False
            else:
                raise
        else:
            # Basic connectivity works, try SSH
            t = Transport(s)
            # Shut up paramiko logging
            plog = logging.getLogger('paramiko.transport')
            plog.disabled = True
            try:
                t.start_client()
                return True
            except paramiko.SSHException as e:
                return False
            finally:
                t.close()
                s.close()
                plog.disabled = False
    except Exception as e:
        raise SshError('Error while checking %s:%d for SSH: %s' % (host, port, e))