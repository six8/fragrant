from fabric.api import *
from fragrant.vbox import Vbox

@task
def install_guest_additions():
    """
    Installs guest additions into the VM
    """
    vbox = Vbox(env.vm_name)
    print('Starting up to install guest additions...')
    with vbox as session:
        session.wait_for_ssh()
        session.install_guest_additions()

@task
def remove():
    """
    Remove an existing box from vagrant
    """
    vbox = Vbox(env.vm_name)
    vbox.remove()


@task
def sshtest():
    """
    Check to see if ssh is up
    """
    vbox = Vbox(env.vm_name)
    print vbox.ssh_up

@task
def ssh():
    """
    Check to see if ssh is up
    """
    vbox = Vbox(env.vm_name)
    with vbox as session:
        session.wait_for_ssh()
        open_shell()

@task
def start(headless=True):
    """
    Start the VM
    """
    vbox = Vbox(env.vm_name)
    vbox.start(headless=headless)

@task
def halt():
    """
    Stop the VM
    """
    vbox = Vbox(env.vm_name)
    vbox.halt()