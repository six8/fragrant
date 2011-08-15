from fabric.api import hide, local
from clom import clom

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
                    ssh_config[key] = val.strip()
            self._ssh_config = ssh_config

        return self._ssh_config

    def use_host(self):
        """
        Decorator to use the vagrant host for a task.
        """
        def decorate(func):
            @functools.wraps(func)
            def _call(*args, **kwargs):
                self.start()
                env.hosts = ['{User}@{HostName}:{Port}'.format(**self.ssh_config)]
                env.key_filename = self.ssh_config['IdentityFile']
                env.disable_known_hosts = True
                env.host_string = env.hosts[0]
                return func(*args, **kwargs)

            return _call

        return decorate

    def cd(self):
        """
        Changed to the vagrant mounted directory on the remote host.
        """
        return cd('/mnt/vagrant')

    @property
    def is_up(self):
        with hide('running'):
            status = local(self.vagrant.status, capture=True)
            return 'powered on' in status
            
    def start(self):
        """
        Start the VM without provisioning
        """
        if not self._started:
            local(self.vagrant.up.with_opts('--no-provision'))
            self._started = True

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

    def package(self):
        """
        Package a Vagrant environment for distribution
        """
        local(self.vagrant.package)

    def destroy(self):
        """
        Destroy the environment, deleting the created virtual machines
        """
        local(self.vagrant.destroy)

    def init(self, box_name, box_url):
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
                b.strip()[4:]
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