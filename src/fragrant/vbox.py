from fabric.api import hide, local, env
from fabric.context_managers import settings, cd
from fabric.contrib.console import confirm
from fabric.operations import sudo
from fabric.utils import abort
from paramiko.transport import Transport
import paramiko
from clom import clom
import socket
from os import path
import re
import json
import string
import time
import logging

log = logging.getLogger(__name__)

_name_re = re.compile('^"(?P<name>[^"]+)"\s*(?P<uuid>{[^}]+})$')

class Timeout(Exception):
    pass

class FormatPattern(object):
    """
    Represents a Python string.format pattern that can also
    parse strings matching the pattern.

    Ex::

        >>> p = FormatPattern('Hello my name is {name}, I am {age:d}!')
        >>> p.match('Hello my name is Mike, I am 31!')
        { 'name' : 'Mike', 'age' : 31 }
        >>> p.format(name='Fred', age=22)
        'Hello my name is Fred, I am 22!'
    """
    _formatter = string.Formatter()

    def __init__(self, format):
        self._format = format
        self._fields, self._regexp = self._parse_format(self._format)

    @property
    def regexp(self):
        return self._regexp

    @property
    def format_spec(self):
        return self._format

    def _parse_format(self, format):
        """
        Builds a regular expression from format.

        Currently only handles simple format specs like `d`.
        """
        fields = {}
        exp = ['^']
        for (literal_text, field_name, format_spec, conversion) in self._formatter.parse(format):
            exp.append(re.escape(literal_text))

            if field_name:
                if format_spec == 'd':
                    type = int
                    pattern = '\d+'
                else:
                    type = str
                    pattern = '.+'

                fields[field_name] = dict(
                    pattern = pattern,
                    spec = format_spec,
                    type = type,
                )

                exp.append('(?P<{field_name}>{pattern})'.format(
                    field_name=re.escape(field_name),
                    pattern=pattern
                ))

        exp.append('$')

        return fields, re.compile(''.join(exp))

    def convert(self, field, value):
        """
        Convert a value to the type expected by a field.
        """
        return self._fields[field]['type'](value)

    def match(self, value):
        """
        Returns a dictionary of field values if the value string matches the pattern.

        Returns None if not a match.
        """
        m = self._regexp.match(value)
        if m:
            vals = m.groupdict()
            for k, v in vals.iteritems():
                vals[k] = self.convert(k, v)
            return vals
        return None

    def format(self, *args, **kwargs):
        """
        Fill the pattern with values from args and kwargs
        """
        return self._format.format(*args, **kwargs)

class VboxSettings(object):
    """
    Attribute access to a VirtualBox VM's settings.
    """

    # maps Vbox's settings to nested dictionaries for convenience
    _mappings = [
        ('nic{id:d}', 'nic.{id:d}.nic'),
        ('natnet{id:d}', 'nic.{id:d}.natnet'),
        ('hostonlyadapter{id:d}', 'nic.{id:d}.hostonlyadapter'),
        ('macaddress{id:d}', 'nic.{id:d}.macaddress'),
        ('uart{id:d}', 'nic.{id:d}.uart'),
        ('boot{id:d}', 'boot.{id:d}'),
        ('storagecontrollerinstance{id:d}', 'storagecontroller.{id:d}.instance'),
        ('storagecontrollerbootable{id:d}', 'storagecontroller.{id:d}.bootable'),
        ('storagecontrollerportcount{id:d}', 'storagecontroller.{id:d}.portcount'),
        ('storagecontrollertype{id:d}', 'storagecontroller.{id:d}.type'),
        ('storagecontrollermaxportcount{id:d}', 'storagecontroller.{id:d}.maxportcount'),
        ('storagecontrollername{id:d}', 'storagecontroller.{id:d}.name'),
        ('SnapshotName-{id:d}', 'Snapshot.{id:d}.Name'),
        ('SnapshotUUID-{id:d}', 'Snapshot.{id:d}.UUID'),
        ('SnapshotName', 'Snapshot.0.Name'),
        ('SnapshotUUID', 'Snapshot.0.UUID'),
        ('Forwarding({id:d})', 'Forwarding.{id:d}'),
    ]

    def __init__(self):
        self._values = {}
        self._forwards = None

        self._mapping_patterns = []
        for fpattern, tpattern in self._mappings:
            fpattern = FormatPattern(fpattern)
            tpattern = FormatPattern(tpattern)
            self._mapping_patterns.append((fpattern, tpattern))

    def _unflatten_dict(self, values):
        """
        Takes a dictionary with flat dot-name keys and returns a nested dictionary.

        Integer looking keys are converted to ints.

        >>> _unflatten_dict({ 'a.a' : 1, 'a.b' : 2, 'c' : 'foo'})
        { 'a' : { 'a' : 1, 'b' : 2 }, 'c' : 'foo' }
        """
        data = {}
        for key, val in values.iteritems():
            parts = key.split('.')
            d = data
            for p in parts[:-1]:
                if p.isdigit():
                    p = int(p)
                d = d.setdefault(p, {})
            key = parts[-1]
            if key.isdigit():
                key = int(key)
            d[key] = val
        return data

    def _to_python(self, value):
        """
        Convert vbox value to Python type.
        """
        if value == 'on':
            value = True
        elif value == 'off':
            value = False
        elif value == 'none':
            value = None
        elif value.isdigit():
            value = int(value)

        return value

    def _set_value(self, settings, key, value):
        """
        Set a value in settings while mapping keys to new keys.
        """
        for fpattern, tpattern in self._mapping_patterns:
            m = fpattern.match(key)
            if m is not None:
                key = tpattern.format(**m)

        settings[key] = value

    @classmethod
    def from_vminfo(cls, vminfo):
        """
        Load settings from a Vbox vminfo string.
        """
        self = cls()

        settings = {}
        for line in vminfo.strip().split('\n'):
            _k, _v = line.split('=', 1)
            key = _k.strip('"')
            value = self._to_python(_v.strip('"'))
            self._set_value(settings, key, value)

        self.__dict__['_values'] = self._unflatten_dict(settings)

        return self

    def __getattr__(self, item):
        """
        Get a setting. Non-existing settings return None.
        """
        return self.get(item, None)

    def __str__(self):
        return json.dumps(self._values, indent=2)

    def _get(self, key, *default):
        """
        Get settings allowing dot-notation access for nested settings.

        @see `get()`
        """
        d = self._values
        while '.' in key:
            lkey, key = key.split('.', 1)
            if lkey.isdigit():
                lkey = int(lkey)
            try:
                d = d[lkey]
            except KeyError:
                if default:
                    return default[0]
                else:
                    raise AttributeError(lkey)

        try:
            if key.isdigit():
                key = int(key)

            return d[key]
        except KeyError:
            if default:
                return default[0]
            else:
                raise AttributeError(key)

    def get(self, key, *default):
        """
        Get an Attribute value. If `default` is provided, it will be returned if
        the attribute is empty or not a valid Attribute. If default is not provided
        and key is not a valid attribute, AttributeError is raised.

        Dot-notation can be used for nested attributes.
        """
        try:
            return self._get(key)
        except AttributeError:
            # See if it's a key pattern
            for fpattern, tpattern in self._mapping_patterns:
                m = fpattern.match(key)
                if m is not None:
                    key = tpattern.format(**m)
                    print key
                    return self._get(key, *default)

        if default:
            return default[0]
        else:
            raise AttributeError(key)

    @property
    def forwards(self):
        """
        Returns a dictionary of nat port forwards
        """
        if self._forwards is None:
            self._forwards = {}
            if 'Forwarding' in self._values:
                for id, cfg in self._values['Forwarding'].iteritems():
                    name, type, hostip, hostport, guestip, guestport = cfg.split(',')
                    self._forwards[name] = dict(
                        name=name,
                        type=type,
                        hostip=hostip,
                        hostport=int(hostport) if hostport else None,
                        guestip=guestip,
                        guestport=int(guestport) if hostport else None,
                    )

        return self._forwards

class VboxManage(object):
    """
    Mid-level interface to VBoxManage command.

    User interaction is avoided.
    """
    def __init__(self):
        self.manage = clom.VBoxManage

    def _cmd(self, cmd, capture=False):
        """
        Run a VBoxManage command.
        """
        with hide('running'):
            return local(cmd, capture=capture).strip()

    @property
    def runningvms(self):
        """
        Returns a list of running VMs

        @see http://www.virtualbox.org/manual/ch08.html#vboxmanage-list
        """
        running = self._cmd(self.manage.list.runningvms, True)
        if running:
            return [
                _name_re.match(vm.strip()).groupdict()
                    for vm in running.split('\n')
            ]
        else:
            return []

    @property
    def vms(self):
        """
        Returns a list of registered VMs

        @see http://www.virtualbox.org/manual/ch08.html#vboxmanage-list
        """
        vms = self._cmd(self.manage.list.vms, True)
        if vms:
            return [
                _name_re.match(vm.strip()).groupdict()
                    for vm in vms.split('\n')
            ]
        else:
            return []

    @property
    def hostonlyifs(self):
        """
        Returns a list of host only interfaces

        @see http://www.virtualbox.org/manual/ch08.html#vboxmanage-list
        """
        hostonlyifs = {}
        ifdata = None
        for line in self._cmd(self.manage.list.hostonlyifs, True).split('\n'):
            k, v = line.strip().split(':', 1)
            v = v.strip()

            if k == 'Name':
                if ifdata:
                    # Fond a new name, start parser over
                    hostonlyifs[ifdata['Name']] = ifdata

                ifdata = dict(Name=v)
            elif ifdata:
                # Add the key to the data we already have
                ifdata[k] = v

        if ifdata:
            # Fond a new name, start parser over
            hostonlyifs[ifdata['Name']] = ifdata

        return hostonlyifs

    def remove_vm(self, vm):
        """
        Delete a VM.

        @see http://www.virtualbox.org/manual/ch08.html#vboxmanage-registervm
        """
        local(self.manage.unregistervm.with_opts(vm, '-delete'))

    def modify_vm(self, vm, *args, **kwargs):
        """
        Change a VM's settings

        @see http://www.virtualbox.org/manual/ch08.html#vboxmanage-modifyvm
        """
        modifyvm = self.manage.modifyvm.with_opts(vm).with_opts(*args, **kwargs)
        self._cmd(modifyvm)

    def control_vm(self, vm, *args, **kwargs):
        """
        Control a VM.

        @see http://www.virtualbox.org/manual/ch08.html#vboxmanage-controlvm
        """
        controlvm = self.manage.controlvm.with_opts(vm).with_opts(*args, **kwargs)
        self._cmd(controlvm)

    def start_vm(self, vm, headless=True):
        """
        Start a VM

        @see http://www.virtualbox.org/manual/ch08.html#vboxmanage-startvm
        """
        type = 'headless' if headless else 'gui'

        startvm = self.manage.startvm.with_opts(vm, type=type)
        self._cmd(startvm, capture=False)

    def port_forward(self, vm, name, hostport, guestport, hostip='', guestip='', type='tcp'):
        """
        Setup a port forward on a VM.

        @see http://www.virtualbox.org/manual/ch06.html#network_nat
        """
        if name in self.vminfo(vm).forwards:
            self.modify_vm(vm, '--natpf1', 'delete', name)

        self.modify_vm(vm, natpf1='{name},{type},{hostip},{hostport},{guestip},{guestport}'.format(**locals()))

    def vminfo(self, vm):
        """
        Get all the settings for the VM.

        @see http://www.virtualbox.org/manual/ch08.html#vboxmanage-showvminfo
        """
        showvminfo = self.manage.showvminfo.with_opts(vm, machinereadable=True, details=True)

        return VboxSettings.from_vminfo(self._cmd(showvminfo, True))

    def create_vm(self, name, ostype, register=True, settings=None):
        """
        Create a VM

        @see http://www.virtualbox.org/manual/ch08.html#idp12394480
        """
        cmd = self.manage.createvm(name=name, ostype=ostype)
        if register:
            cmd = cmd.with_opts(register=True)

        self._cmd(cmd)

        if settings:
            self.modify_vm(name, **settings)

    def create_hd(self, vm, size, controller_type='sata', controller='SATA Controller'):
        """
        Create a virtual hard disk.

        @see http://www.virtualbox.org/manual/ch08.html#vboxmanage-createvdi
        """
        cfg_file = self.vminfo(vm).CfgFile
        dir = path.dirname(cfg_file)

        filename = path.join(dir, '%s.vdi' % vm)
        self._cmd(self.manage.createhd(filename=filename, size=size))

        self._cmd(self.manage.storagectl.with_opts(vm).with_opts(name=controller, add=controller_type))

        self._cmd(self.manage.storageattach.with_opts(vm).with_opts(storagectl=controller, port=0, device=0, type='hdd', medium=filename))

        return filename

    def create_dvd(self, vm, controller='SATA Controller'):
        """
        Add a DVD drive to the VM.

        @see http://www.virtualbox.org/manual/ch08.html#idp14153472
        """
        self._cmd(self.manage.storageattach.with_opts(vm).with_opts(storagectl=controller, port=1, device=0, type='dvddrive', medium='emptydrive', forceunmount=True))

    def load_dvd(self, vm, iso_path, controller='SATA Controller'):
        """
        Load a DVD/CD iso in the DVD drive of the VM.

        @see http://www.virtualbox.org/manual/ch08.html#idp14153472
        """
        self.create_dvd(vm)
        self._cmd(self.manage.storageattach.with_opts(vm).with_opts(
            storagectl=controller,
            port=1,
            device=0,
            type='dvddrive',
            medium=iso_path,
            forceunmount=True
        ))

    def eject_dvd(self, vm):
        """
        Unload an iso connected to the DVD drive.
        """
        self.load_dvd(vm, 'emptydrive')

    @property
    def guest_additions_iso(self):
        paths = [
            '/Applications/VirtualBox.app/Contents/MacOS/VBoxGuestAdditions.iso',
            '/usr/share/virtualbox/VBoxGuestAdditions.iso'
        ]

        for p in paths:
            if path.exists(p):
                return p

manage = VboxManage()

def free_port():
    """
    Get an open port for the local host.
    """
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('', 0))
    port = s.getsockname()[1]
    s.close()
    return port

class Vbox(object):
    """
    High level interface to a Virtual Box VM.

    Provides Fabric user interaction for common VM tasks.
    """
    def __init__(self, name, username=None, password=None, ssh_port=None):
        self._running_stack_count = 0
        self._session = None

        # True if the VM was started by a context
        self._started_by_context = None

        self.name = name
        self._ssh_port = ssh_port
        self.host = '127.0.0.1'

        self.username = username
        self.password = password

    @property
    def is_running(self):
        """
        Return True if the VM is running
        """
        return self.name in [v['name'] for v in manage.runningvms]

    @property
    def exists(self):
        """
        Return True if the VM exists
        """
        return self.name in [v['name'] for v in manage.vms]

    def start(self, headless=True):
        """
        Start the VM
        """
        return manage.start_vm(self.name, headless=headless)

    def _ensure_running(self):
        """
        If the VM is not running, start it. Verify connectivity.

        Returns True if the VM had to be started, False if it was already running.
        """

        started = False
        if not self.is_running:

            # with hide('running'):
            #     local('VBoxManage setextradata "{vm_name}" "VBoxInternal/Devices/e1000/0/LUN#0/Config/SSH/HostPort" {port}'.format(port=port, vm_name=env.vm_name))
            #     local('VBoxManage setextradata "{vm_name}" "VBoxInternal/Devices/e1000/0/LUN#0/Config/SSH/GuestPort" 22'.format(vm_name=env.vm_name))
            #     local('VBoxManage setextradata "{vm_name}" "VBoxInternal/Devices/e1000/0/LUN#0/Config/SSH/Protocol" TCP'.format(vm_name=env.vm_name))

            self.enable_ssh_forward()

            log.info('Starting VM %s' % self.name)
            self.start(headless=False)

            while not self.is_running:
                time.sleep(2)

            started = True

        return started

    def port_forward(self, *args, **kwargs):
        """
        Setup a port forward for the VM.
        """
        return manage.port_forward(self.name, *args, **kwargs)

    def enable_ssh_forward(self):
        """
        Enable SSH port forwarding.
        """
        if not self._ssh_port:
            self._ssh_port = free_port()
            
        self.port_forward('ssh', hostport=self._ssh_port, guestport=22)

    @property
    def ssh_port(self):
        if self._ssh_port:
            return self._ssh_port
        elif 'ssh' in self.vminfo.forwards:
            forward = self.vminfo.forwards['ssh']
            return forward['hostport']

    @property
    def hostonly_nic(self):
        """
        Return's NIC # if host-only networking is enabled.
        """
        nics = self.vminfo.nic
        for id, nic in nics.iteritems():
            if nic.get('hostonlyadapter', False):
                return id
        return None

    def set_hostonly_nic(self, nic, hostonlyif=None):
        """
        Enable host-only networking for the specified NIC.
        """
        if not hostonlyif:
            hostonlyifs = manage.hostonlyifs
            if not hostonlyifs:
                raise Exception('No host only interfaces found.')
            else:
                hostonlyif = hostonlyifs.values()[0]['Name']

        kwargs = {
            'nic%d' % nic : 'hostonly',
            'hostonlyadapter%d' % nic : hostonlyif,
        }
        self.modify(**kwargs)

    def enable_pxe_boot(self, nic=1):
        """
        Enables PXE boot on specified NIC.

        The must be changed to `Am79C973` for PXE booting, however
        it's recommended to use `82540EM` for general usage.
        """
        #NIC Type Am79C973 for PXE boot
        kwargs = {
            'nic%d' % nic : 'nat',
            'nictype%d' % nic : 'Am79C973',
            'boot1' : 'net',
        }
        self.modify(**kwargs)

    def enable_normal_boot(self):
        self.modify(
            boot1='dvd',
            nic1='nat',
            nictype1='82540EM',
        )

    @property
    def hostonlyif(self):
        """
        Get information about the host-only network being used by this VM.
        """
        adapters = [v['hostonlyadapter'] for k, v in self.vminfo.nic.iteritems() if v.get('hostonlyadapter', False)]
        if adapters:
            ifname = adapters[0]
            return manage.hostonlyifs[ifname]

    def modify(self, *args, **opts):
        return manage.modify_vm(self.name, *args, **opts)

    def control(self, *args, **opts):
        return manage.control_vm(self.name, *args, **opts)

    def halt(self, poweroff=False, timeout=60):
        """
        Shutdown the VM.

        If `poweroff` is True, then shutdown with `poweroff`. Otherwise tries ACPI shutdown. If
        ACPI shutdown fails after `timeout` seconds, prompt for poweroff.
        """
        log.info('Shutting down...')

        method = 'poweroff' if poweroff else 'acpipowerbutton'
        log.debug('Using %s to shutdown' % method)
        self.control(method)
        try:
            try:
                self.join(timeout)
            except Timeout:
                if method == 'acpipowerbutton' and confirm('ACPI power off is failing, would you like to force shutdown?'):
                    return self.halt(poweroff=True)
                else:
                    abort('Shutdown failed, VM is still running.')
        except (KeyboardInterrupt, SystemExit):
            if confirm('Would you like to force shutdown now?'):
                self.halt(poweroff=True)
            raise

    def __enter__(self):
        """
        Vbox can be used as a context. When the context starts the VM will
        start. If the VM was started by the context, then when the context ends the VM will be halted.
        If the VM is already running, it will not be halted.

        :returns: VboxSession

        Ex:

            with Vbox('test') as session:
                print session.name

        """
        if self._running_stack_count == 0:
            self._started_by_context = not self.is_running
            self._ensure_running()
            self._session = VboxSession(self)

        self._running_stack_count += 1

        return self._session

    def __exit__(self, type, value, traceback):
        self._running_stack_count -= 1
        if self._running_stack_count == 0:
            if type is KeyboardInterrupt:
                if confirm('Do you want to shut down?'):
                    self.halt()
            elif type and value:
                log.error('Error: %s' % value)
                if confirm('Do you want to shut down?'):
                    self.halt()
            elif self._started_by_context:
                self.halt()

        return False
    
    @property
    def ssh_up(self):
        """
        Return True if SSH connectivity exists.
        """
        return self.check_ssh_up(quick=False)

    def check_ssh_up(self, quick=False):
        """
        Return True if SSH connectivity exists.

        First checks if port is open, then tries to see if it response to SSH connectivity.
        """
        if not self.ssh_port:
            raise Exception('SSH port for %r not assigned or detected.' % self.name)

        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(2)
        try:
            s.connect((self.host, self.ssh_port))
        except socket.error as e:
            if e.errno in (61,):
                return False
            else:
                raise
        else:
            if quick:
                return True
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

    def remove(self):
        """
        Remove the VM, deleting all disks.
        """
        if self.is_running:
            self.halt(poweroff=True)
        manage.remove_vm(self.name)

    def use_host(self):
        """
        Decorator to setup SSH hosts.
        """
        def decorate(func):
            @functools.wraps(func)
            def _call(*args, **kwargs):
                with self as session:
                    host = '{host}:{port}'.format(host=self.host, port=self.ssh_port)
                    with settings(
                        hosts = [host],
                        disable_known_hosts = True,
                        host_string = host,
                    ):
                        return func(*args, **kwargs)
            return _call

        return decorate

    def create(self, ostype, register=True, hostonly_nic=None, hd_size=4096, dvd=True, settings=None):
        """
        Create the VM
        """
        manage.create_vm(name=self.name, ostype=ostype, register=register, settings=settings)
        if hostonly_nic:
            self.set_hostonly_nic(nic=hostonly_nic)

        if hd_size:
            manage.create_hd(self.name, size=4096)
        if dvd:
            manage.create_dvd(self.name)

    @property
    def vminfo(self):
        """
        Get all the settings for the VM.

        TODO Consider caching the results.
        """
        return manage.vminfo(self.name)

    @property
    def path(self):
        """
        Directory the VM files are in.
        """
        cfg_file = self.vminfo.CfgFile
        return path.dirname(cfg_file)

    def join(self, timeout=None):
        """
        Block until the VM shuts down.
        """
        start = time.time()
        while self.is_running:
            if timeout and time.time() - start > timeout:
                raise Timeout('join timed out')
            else:
                time.sleep(2)

class VboxSession(object):
    """
    Contains actions that can be performed on a running VM.

    Usually acquired by a Vbox context:

        with Vbox('test') as session:
            print session.name

    """
    def __init__(self, vbox):
        self._vbox = vbox
        self.name = self._vbox.name

    def install_guest_additions(self):
        """
        Installs guest additions into the VM.
        """
        host = '{host}:{port}'.format(host=self._vbox.host, port=self._vbox.ssh_port)
        password = env.passwords.get(host, self._vbox.password)

        with settings(
            hosts = [host],
            disable_known_hosts = True,
            host_string = host,
            user = env.user or self._vbox.username,
            passwords = { host : password },
        ):
            log.info('Installing guest additions...')

            manage.load_dvd(self._vbox.name, manage.guest_additions_iso)

            mount_point = '/media/cdrom'

            sudo(clom.mkdir(mount_point, p=True))

            with settings(warn_only=True):
                r = sudo(clom.mountpoint(mount_point))
            if r.return_code == 0:
                sudo(clom.umount(mount_point))

            sudo(clom.mount('/dev/cdrom', mount_point))

            with cd(mount_point):
                sudo(clom.sh('VBoxLinuxAdditions.run'))

            sudo(clom.umount(mount_point))
            log.warn('It is OK if "Installing the Window System" failed.')

            manage.eject_dvd(self._vbox.name)

    def wait_for_ssh(self, timeout=None):
        """
        Block until SSH is accessible
        """
        log.info('Waiting for ssh connection at %s:%d...' % (self._vbox.host, self._vbox.ssh_port))
        start = time.time()
        while not self._vbox.ssh_up:
            if not self._vbox.is_running:
                raise Exception('VM stopped while waiting for SSH')
            elif timeout and time.time() - start > timeout:
                raise Timeout('wait_for_ssh timed out')
            else:
                time.sleep(2)

        log.debug('SSH is ready at %s:%d' % (self._vbox.host, self._vbox.ssh_port))

    def halt(self):
        return self._vbox.halt()
