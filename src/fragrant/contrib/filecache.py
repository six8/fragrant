import hashlib
from tempfile import NamedTemporaryFile
import urllib2
import os
import shutil

import logging

log = logging.getLogger(__name__)

class FileCache(object):
    """
    Cache remote files locally.
    """
    readsize = 4096

    def __init__(self, cache_dir):
        self.cache_dir = cache_dir

    def get(self, url):
        filename = os.path.basename(url)

        path = os.path.join(self.cache_dir, '%s-%s' % (hashlib.sha1(url).hexdigest(), filename))

        if not os.path.exists(path):
            self._download(url, path)

        return path

    def _download(self, url, path):
        fetch_request = urllib2.Request(url)


        remote_file = urllib2.urlopen(fetch_request)

        try:
            log.info('Caching "%s" as "%s"', url, path)

            dir = os.path.dirname(path)
            if not os.path.exists(dir):
                os.makedirs(dir)

            filesize = int(remote_file.info().getheader('Content-Length', 0))

            outfile = NamedTemporaryFile('wb', self.readsize, delete=False)
            read = 0
            while 1:
                bytes = remote_file.read(self.readsize)
                if not bytes:
                    break

                outfile.write(bytes)
                read += len(bytes)
            outfile.close()
            
            log.debug('Read %d bytes', read)

            shutil.move(outfile.name, path)
        finally:
            remote_file.close()
