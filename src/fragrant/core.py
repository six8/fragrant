import contextlib
import re
from fabric.api import hide, local
from fabric.context_managers import settings
from clom import clom, NOT_SET
import time
from fragrant.exceptions import Timeout, FragrantException
import logging
from fragrant.util import check_ssh_up

log = logging.getLogger(__name__)

class Vagrant(object):
    def __init__(self):
        self.vagrant = clom.vagrant

        self._ssh_config = None

    @property
    def ssh_config(self):
        """
        Read in ssh config settings for this vagrant instance.

        :returns: dict
        """
        if self._ssh_config is None:
            ssh_config = {}
            with hide('running', 'stdout', 'stderr'):
                for line in local(self.vagrant.ssh_config, capture=True).strip().split('\n'):
                    key, val = line.strip().split(' ', 1)
                    val = val.strip()
                    if key == 'Port':
                        val = int(val)
                        
                    ssh_config[key] = val
            self._ssh_config = ssh_config

        return self._ssh_config

    @property
    def ssh_host(self):
        return self._ssh_config.get('HostName', None)

    @property
    def ssh_port(self):
        return self._ssh_config.get('Port', None)

    def use_host(self):
        """
        Decorator to use the vagrant host for a task.
        """
        def decorate(func):
            @functools.wraps(func)
            def _call(*args, **kwargs):
                self._ensure_running()
                with self.ssh_context():
                    return func(*args, **kwargs)

            return _call

        return decorate

    @contextlib.contextmanager
    def ssh_context(self):
        """
        Context manager that sets up Fabric's host settings with settings
        for this box's SSH settings.
        """
        host = '{User}@{HostName}:{Port}'.format(**self.ssh_config)
        with settings(
            hosts = [host],
            disable_known_hosts = True,
            host_string = host,
            key_filename = self.ssh_config['IdentityFile']
        ):
            yield

    def cd(self):
        """
        Changed to the vagrant mounted directory on the remote host.
        """
        return cd('/mnt/vagrant')

    @property
    def is_running(self):
        return self.state == 'running'

    @property
    def state(self):
        with hide('running'):
            status = local(self.vagrant.status, capture=True)

        lines = filter(None, status.strip().split('\n'))
        if lines[0] == 'Current VM states:':
            _, state = re.split(r'\s+', lines[1])
            return state
        else:
            raise FragrantException('Could not determine VM state')


    @property
    def ssh_up(self):
        return check_ssh_up(self.ssh_host, self.ssh_port)

    def start(self):
        """
        Start the VM without provisioning
        """
        if not self.is_running:
            local(self.vagrant.up.with_opts('--no-provision'))

    def halt(self):
        """
        Halt the running VMs in the environment
        """
        with hide('running'):
            local(self.vagrant.halt)

    def provision(self):
        """
        Rerun the provisioning scripts on a running VM
        """
        if not self.is_up:
            return self.up()

        local(self.vagrant.provision)

    def up(self):
        """
        Creates the Vagrant environment
        """
        local(self.vagrant.up)

    def suspend(self):
        """
        Suspend a running Vagrant environment.
        """
        local(self.vagrant.suspend)

    def resume(self):
        """
        Resume a suspended Vagrant environment
        """
        local(self.vagrant.resume)

    def reload(self):
        """
        Reload the environment, halting it then restarting it.
        """
        local(self.vagrant.reload)

    def package(self, base, output):
        """
        Package a Vagrant environment for distribution
        """
        local(self.vagrant.package.with_opts(base=base, output=output))

    def destroy(self):
        """
        Destroy the environment, deleting the created virtual machines
        """
        local(self.vagrant.destroy)

    def init(self, box_name=NOT_SET, box_url=NOT_SET):
        """
        Initializes the current folder for Vagrant usage
        """
        local(self.vagrant.init(box_name, box_url))

    @property
    def boxes(self):
        """
        List of installed boxes.
        """
        with hide('running', 'stdout'):
            boxes = [
                b.strip()
                    for b in local(self.vagrant.box.list, capture=True).strip().split('\n')
            ]
        return boxes

    def has_box(self, name):
        """
        Check if vagrant box `name` exists.
        """
        return name in self.boxes

    def remove_box(self, name):
        """
        Remove a box from the system
        """
        with hide('running'):
            local(self.vagrant.box.remove(name))

    def add_box(self, name, uri):
        """
        Add a box to the system
        """
        with hide('running'):
            local(self.vagrant.box.add(name, uri))

    @contextlib.contextmanager
    def session(self, halt_if_started=False, timeout=None):
        """
        Context manager that starts the VM if needed and sets up a SSH context.

        :param halt_if_started: bool - If the context started the VM it will halt it when the context is exited
        :param timeout: int - Seconds to wait for VM to start
        """
        start = time.time()
        started_by_context = self._ensure_running(timeout)

        try:
            with self.ssh_context():
                log.info('Waiting for SSH on port %d...' % self.ssh_config['Port'])
                while not self.ssh_up:
                    if not self.is_running:
                        raise Exception('VM stopped while waiting for SSH')
                    elif timeout and time.time() - start > timeout:
                        raise Timeout('Waiting for SSH timed out')
                    else:
                        time.sleep(2)

                yield
        finally:
            if started_by_context and halt_if_started:
                self.halt()


    def _ensure_running(self, timeout=None):
        """
        If the VM is not running, start it.

        Returns True if the VM had to be started, False if it was already running.
        """

        start = time.time()
        started = False
        if not self.is_running:
            self.start()
            log.info('Waiting for VM to start...')
            while not self.is_running:
                if timeout and time.time() - start > timeout:
                    raise Timeout('Waiting for start timed out')
                else:
                    time.sleep(2)

            started = True

        return started