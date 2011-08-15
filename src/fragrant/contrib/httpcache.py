from eventlet import wsgi
import eventlet
from urllib import quote
import os
import logging
import posixpath
import urllib
import re
from BaseHTTPServer import BaseHTTPRequestHandler
from eventlet.green import urllib2
import thread

logging.basicConfig(level=logging.INFO)
log = logging.getLogger('httpcache')

__doc__ = """
An Eventlet based HTTP pass-through mirror.

Will transparently mirror files locally. Files that do not exist will be fetched
from the remote server and persisted locally. Range requests work by caching the range
as a separate file.
"""

class Response(object):
    FULL_RANGE = (0, -1)
    
    def __init__(self, content_type, size, range=None):
        self._range = range or self.FULL_RANGE
        self.content_type = content_type
        self.size = size
        
    @property
    def status(self):
        if self._range != self.FULL_RANGE:
            return 206
        else:
            return 200
        
    @property
    def content_range(self):
        return (self.content_start, self.content_end)
        
    @property
    def content_start(self):
        return self._range[0]

    @property
    def content_end(self):
        if self._range[1] == -1:
            return self.size - 1
        else:
            return self._range[1]

    @property
    def content_length(self):
        return (self.content_start - self.content_end) + 1
        
    @property
    def headers(self):
        headers = [
            ('Content-Length', str(self.content_length)),
        ]
        
        if self.status == 206:
            headers.append(('Content-Range', 'bytes %d-%d/%d' % (self.content_start, self.content_end, self.size)))
        
        return headers
    
class FileContent(object):
    readsize = 4096
    
    def __init__(self, f, range):
        self.range = range
        self.f = f
                    
    def __iter__(self):
        self.f.seek(self.range[0])

        total = (self.range[1] - self.range[0]) + 1
        sent = 0
        remaining = total
        while remaining:
            output = self.f.read(min(self.readsize, remaining))
            if not output:
                break

            yield output
            size = len(output)
            sent += size
            remaining -= size
            
        log.debug('Sent %d bytes of %d (range: %d-%d)', sent, total, self.range[0], self.range[1])
        self.f.close()
                
# FIXME Handle range downloads (need to download the full file, not just the range)        
class FileCacheContent(object):
    readsize = 4096

    def __init__(self, f, outfilename):
        self.f = f
        
        self.outfilename = outfilename
        self.outfile = open(self.outfilename + '.tmp', 'wb', self.readsize)
        
    def __iter__(self):
        sent = 0
        while 1:
            bytes = self.f.read(self.readsize)
            if not bytes:
                break
                
            self.outfile.write(bytes)
            
            yield bytes
            sent += len(bytes)
            
        log.debug('Sent %d bytes', sent)
                       
        self.f.close()
        self.outfile.close()
        os.rename(self.outfilename + '.tmp', self.outfilename)
                
class HttpCache(object):
    """
    HTTP pass-through cache server.

    Requests first checked for local copy, if not existing
    requests are forwarded to remote_base_url.

    Files are cached in cache_dir.
    """
    extensions_map = {
        '.rpm' : 'application/x-redhat-package-manager',
        None : 'application/octet-stream',
    }
        
    def __init__(self, remote_base_urls, cache_dir):
        self.mirrors = remote_base_urls
        self.cache_dir = cache_dir
        
    def translate_path(self, mirror_name, path):
        """Translate a /-separated PATH to the local filename syntax.

        Components that mean special things to the local file system
        (e.g. drive or directory names) are ignored.  (XXX They should
        probably be diagnosed.)

        """
        # abandon query parameters
        path = path.split('?',1)[0]
        path = path.split('#',1)[0]
        path = posixpath.normpath(urllib.unquote(path))
        words = path.split('/')
        words = filter(None, words)
        current_path = os.path.join(self.cache_dir, mirror_name)
        for word in words:
            drive, word = os.path.splitdrive(word)
            head, word = os.path.split(word)
            if word in (os.curdir, os.pardir): continue
            current_path = os.path.join(current_path, word)
        return current_path
        
    def reconstruct_url(self, environ):
        # From WSGI spec, PEP 333        
        url = quote(environ.get('SCRIPT_NAME',''))
        url += quote(environ.get('PATH_INFO','')).replace(url.replace(':', '%3A'), '')
        return url
                
    def _start_response(self, start_response, response):
        status = '%d %s' % (response.status, BaseHTTPRequestHandler.responses[response.status][0])
        start_response(status, response.headers)
                
    def _find_file(self, paths):
        for path, range in paths:
            if os.path.isfile(path):
                return path, range
                
        return None, None
        
    def _get_range(self, environ):
        http_range = environ.get('HTTP_RANGE', None)
        if http_range:
            m = re.match(r'^bytes=(?P<start>\d+)-(?P<end>\d+)?$', http_range)
            end = m.group('end')
            if end:
                end = int(end)
            else:
                end = -1
                
            return (int(m.group('start')), end)
        
    def do_GET(self, environ, start_response):
        """Common code for GET and HEAD commands.

        This sends the response code and MIME headers.

        Return value is either a file object (which has to be copied
        to the outputfile by the caller unless the command was HEAD,
        and must be closed by the caller under all circumstances), or
        None, in which case the caller has nothing further to do.

        """
        log.debug(environ)
        url = self.reconstruct_url(environ)
        _, mirror_name, url = url.split('/', 2)
        url = '/' + url
        path = self.translate_path(mirror_name, url)
        if os.path.isdir(path):
            return

        log.debug('Serving %s: %s - %s', mirror_name, path, url)
        
        name, ext = os.path.splitext(path)
        ctype = self.extensions_map.get(ext, self.extensions_map[None])
        
        range = self._get_range(environ)   
        range_path = path 
        if range:
            name, ext = os.path.splitext(path)                        
            range_path = '%s(%d-%d)%s' % (name, range[0], range[1], ext)
                        
        local_path, file_range = self._find_file([(path, True), (range_path, False)])
        if local_path:
            try:
                # Always read in binary mode. Opening files in text mode may cause
                # newline translations, making the actual size of the content
                # transmitted *less* than the content-length!
                f = open(local_path, 'rb')
            except IOError:
                start_response('500 Could not read file', [])
                return ''
                
            fs = os.stat(local_path)
            filesize = fs[6]
                
            response = Response(ctype, filesize, range)
            content = FileContent(f, range=(response.content_start, response.content_end) if file_range else None)            

            self._start_response(start_response, response)
            return content
        else:
            fetch_url = self.mirrors[mirror_name] + url
            headers = {}
            if range:
                headers = {
                    'Range' : 'bytes=%d-%s' % (range[0], range[1] if range[1] != -1 else '')
                }
                
            fetch_request = urllib2.Request(fetch_url, headers=headers)
            
            try:
                remote_file = urllib2.urlopen(fetch_request)
            except urllib2.HTTPError as e:
                start_response('%s %s' % (e.code, e), [])
                return ''
                
            log.info('Caching "%s" as "%s"', fetch_url, range_path)

            dir = os.path.dirname(range_path)
            if not os.path.exists(dir):
                os.makedirs(dir)
                
            response_range = remote_file.info().getheader('Content-Range', None)         
            if response_range:
                m = re.match(r'^bytes (?P<start>\d+)-(?P<end>\d+)/(?P<size>\d+)$', response_range)
                filesize = int(m.group('size'))
                range = int(m.group('start')), int(m.group('end'))
            else:
                filesize = int(remote_file.info().getheader('Content-Length', 0))
                
            response = Response(ctype, filesize, range)
            content = FileCacheContent(remote_file, range_path)            
            
            self._start_response(start_response, response)
            return content
                            
    def __call__(self, environ, start_response):
        if environ['REQUEST_METHOD'] == 'GET':
            return self.do_GET(environ, start_response)
        else:
            raise Exception('Unkown method %s' % environ['REQUEST_METHOD'])

def serve(mirror_url, cache_dir, port=8996):
    listener = HttpCache(mirror_url, cache_dir)
    wsgi.server(eventlet.listen(('', port)), listener)

def start(mirror_urls, cache_dir, port=8996):
    """
    Serve in the background
    """
    worker_pool = eventlet.GreenPool(20)
    sock = eventlet.listen(('', port))
    app = HttpCache(mirror_urls, cache_dir)
    
    def proper_shutdown():
        worker_pool.resize(0)
        sock.close()
        log.info("Shutting down. Requests left: %s", worker_pool.running())
        worker_pool.waitall()
        log.info("Exiting.")
        raise SystemExit()

    def queue_shutdown():
        eventlet.spawn_n(proper_shutdown)

    thread.start_new_thread(wsgi.server, (sock, app), dict(custom_pool=worker_pool))

    return queue_shutdown