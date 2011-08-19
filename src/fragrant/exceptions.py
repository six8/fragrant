class FragrantException(Exception):
    pass

class Timeout(FragrantException):
    pass

class SshError(FragrantException):
    pass