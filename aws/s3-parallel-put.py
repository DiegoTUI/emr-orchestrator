#!/usr/bin/env python
# Parallel uploads to Amazon AWS S3
#
# The MIT License (MIT)
#
# Copyright (c) 2011-2014 Tom Payne
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

try:
    from cStringIO import StringIO
except ImportError:
    from StringIO import StringIO
from gzip import GzipFile
from itertools import chain, imap, islice
import logging
from multiprocessing import JoinableQueue, Process, current_process
from optparse import OptionGroup, OptionParser
import os.path
import re
from ssl import SSLError
import sys
import tarfile
import time
import mimetypes

from boto.s3.connection import S3Connection
from boto.s3.acl import CannedACLStrings
from boto.utils import compute_md5


DONE_RE = re.compile(r'\AINFO:s3-parallel-put\[putter-\d+\]:\S+\s+->\s+(\S+)\s*\Z')


def repeatedly(func, *args, **kwargs):
    while True:
        yield func(*args, **kwargs)


class FileObjectCache(object):

    def __init__(self):
        self.name = None
        self.file_object = None

    def open(self, name, *args):
        if name != self.name:
            self.name = name
            self.file_object = open(self.name, *args)
        return self

    def __enter__(self):
        return self.file_object

    def __exit__(self, exc_type, exc_value, traceback):
        pass


class Value(object):

    def __init__(self, file_object_cache, content=None, filename=None, md5=None, offset=None, path=None, size=None):
        self.file_object_cache = file_object_cache
        self.content = content
        self.filename = filename
        self.md5 = md5
        self.offset = offset
        self.path = path
        self.size = size

    def get_content(self):
        if self.content is None:
            if self.filename:
                with self.file_object_cache.open(self.filename) as file_object:
                    file_object.seek(self.offset)
                    self.content = file_object.read(self.size)
            elif self.path:
                with open(self.path) as file_object:
                    self.content = file_object.read()
            else:
                assert False
        return self.content

    def calculate_md5(self):
        if self.md5 is None:
            self.md5 = compute_md5(StringIO(self.get_content()))
        return self.md5

    def get_size(self):
        if self.size is None:
            if self.content:
                self.size = len(self.content)
            elif self.path:
                self.size = os.stat(self.path).st_size
            else:
                assert False
        return self.size


def walk_filesystem(source, options):
    if os.path.isdir(source):
        for dirpath, dirnames, filenames in os.walk(source):
            for filename in filenames:
                path = os.path.join(dirpath, filename)
                if not os.path.isfile(path):
                    continue
                key_name = os.path.normpath(os.path.join(options.prefix, path))
                yield (key_name, dict(path=path))
    elif os.path.isfile(source):
        key_name = os.path.normpath(os.path.join(options.prefix, source))
        yield (key_name, dict(path=source))


def walk_tar(source, options):
    try:
        tar_file = tarfile.open(source, 'r:')
        for tarinfo in tar_file:
            if tarinfo.isfile():
                path = tarinfo.name
                key_name = os.path.normpath(os.path.join(options.prefix, path))
                filename = source
                offset = tarinfo.offset_data
                size = tarinfo.size
                yield (key_name, dict(filename=filename, offset=offset, path=path, size=size))
            # http://blogs.oucs.ox.ac.uk/inapickle/2011/06/20/high-memory-usage-when-using-pythons-tarfile-module/
            tar_file.members = []
    except tarfile.ReadError:
        tar_file = tarfile.open(source)
        for tarinfo in tar_file:
            if tarinfo.isfile():
                path = tarinfo.name
                key_name = os.path.normpath(os.path.join(options.prefix, path))
                content = tar_file.extractfile(tarinfo).read()
                yield (key_name, dict(content=content, path=path))


def walker(walk, put_queue, sources, options):
    logger = logging.getLogger('%s[walker-%d]' % (os.path.basename(sys.argv[0]), current_process().pid))
    pairs = chain(*imap(lambda source: walk(source, options), sources))
    if options.resume:
        done = set()
        for filename in options.resume:
            with open(filename) as file_object:
                for line in file_object:
                    match = DONE_RE.match(line)
                    if match:
                        done.add(match.group(1))
        pairs = ((key_name, args) for key_name, args in pairs if key_name not in done)
    if options.limit:
        pairs = islice(pairs, options.limit)
    for pair in pairs:
        put_queue.put(pair)


def put_add(bucket, key_name, value):
    key = bucket.get_key(key_name)
    if key is None:
        return bucket.new_key(key_name)
    else:
        return None


def put_stupid(bucket, key_name, value):
    return bucket.new_key(key_name)


def put_update(bucket, key_name, value):
    key = bucket.get_key(key_name)
    if key is None:
        return bucket.new_key(key_name)
    else:
        # Boto's md5 function actually returns 3-tuple: (hexdigest, base64, size)
        value.calculate_md5()
        if key.etag == '"%s"' % value.md5[0]:
            return None
        else:
            return key


def putter(put, put_queue, stat_queue, options):
    logger = logging.getLogger('%s[putter-%d]' % (os.path.basename(sys.argv[0]), current_process().pid))
    connection, bucket = None, None
    file_object_cache = FileObjectCache()
    while True:
        args = put_queue.get()
        if args is None:
            put_queue.task_done()
            break
        key_name, value_kwargs = args
        value = Value(file_object_cache, **value_kwargs)
        try:
            if connection is None:
                connection = S3Connection(is_secure=options.secure, host=options.host)
            if bucket is None:
                bucket = connection.get_bucket(options.bucket)
            key = put(bucket, key_name, value)
            if key:
                if options.headers:
                    headers = dict(tuple(header.split(':', 1)) for header in options.headers)
                else:
                    headers = {}
                if options.content_type:
                    if options.content_type == "guess":
                        headers['Content-Type'] = mimetypes.guess_type(value.path)[0]
                    else:
                        headers['Content-Type'] = options.content_type

                content = value.get_content()
                md5 = value.md5
                if options.gzip:
                    headers['Content-Encoding'] = 'gzip'
                    string_io = StringIO()
                    gzip_file = GzipFile(compresslevel=9, fileobj=string_io, mode='w')
                    gzip_file.write(content)
                    gzip_file.close()
                    content = string_io.getvalue()
                    md5 = compute_md5(StringIO(content))
                if not options.dry_run:
                    key.set_contents_from_string(content, headers, md5=md5, policy=options.grant)
                logger.info('%s -> %s' % (value.path, key.name))
                stat_queue.put(dict(size=value.get_size()))
            else:
                logger.info('skipping %s -> %s' % (value.path, key_name))
        except SSLError as exc:
            logger.error('%s -> %s (%s)' % (value.path, key_name, exc))
            put_queue.put(args)
            connection, bucket = None, None
        put_queue.task_done()


def statter(stat_queue, start, options):
    logger = logging.getLogger('%s[statter-%d]' % (os.path.basename(sys.argv[0]), current_process().pid))
    count, total_size = 0, 0
    while True:
        kwargs = stat_queue.get()
        if kwargs is None:
            stat_queue.task_done()
            break
        count += 1
        total_size += kwargs.get('size', 0)
        stat_queue.task_done()
    duration = time.time() - start
    logger.info('put %d bytes in %d files in %.1f seconds (%d bytes/s, %.1f files/s)' % (total_size, count, duration, total_size / duration, count / duration))


def main(argv):
    parser = OptionParser()
    group = OptionGroup(parser, 'S3 options')
    group.add_option('--bucket', metavar='BUCKET',
            help='set bucket')
    group.add_option('--host', default='s3.amazonaws.com',
            help='set AWS host name')
    group.add_option('--insecure', action='store_false', dest='secure',
            help='use insecure connection')
    group.add_option('--secure', action='store_true', default=True, dest='secure',
            help='use secure connection')
    parser.add_option_group(group)
    group = OptionGroup(parser, 'Source options')
    group.add_option('--walk', choices=('filesystem', 'tar'), default='filesystem', metavar='MODE',
            help='set walk mode (filesystem or tar)')
    parser.add_option_group(group)
    group = OptionGroup(parser, 'Put options')
    group.add_option('--content-type', metavar='CONTENT-TYPE',
            help='set content type, set to "guess" to guess based on file name')
    group.add_option('--gzip', action='store_true',
            help='gzip values and set content encoding')
    group.add_option('--put', choices=('add', 'stupid', 'update'), default='update', metavar='MODE',
            help='set put mode (add, stupid, or update)')
    group.add_option('--prefix', default='', metavar='PREFIX',
            help='set key prefix')
    group.add_option('--resume', action='append', default=[], metavar='FILENAME',
            help='resume from log file')
    group.add_option('--grant', metavar='GRANT', default=None, choices=CannedACLStrings,
            help='A canned ACL policy to be applied to each file uploaded.\nChoices: %s' %
            ', '.join(CannedACLStrings))
    group.add_option('--header', metavar='HEADER:VALUE', dest='headers', action='append',
                     help='extra headers to add to the file, can be specified multiple times')
    parser.add_option_group(group)
    group = OptionGroup(parser, 'Logging options')
    group.add_option('--log-filename', metavar='FILENAME',
            help='set log filename')
    group.add_option('--quiet', '-q', action='count', default=0,
            help='less output')
    group.add_option('--verbose', '-v', action='count', default=0,
            help='more output')
    parser.add_option_group(group)
    group = OptionGroup(parser, 'Debug and performance tuning options')
    group.add_option('--dry-run', action='store_true',
            help='don\'t write to S3')
    group.add_option('--limit', metavar='N', type=int,
            help='set maximum number of keys to put')
    group.add_option('--processes', default=8, metavar='PROCESSES', type=int,
            help='set number of putter processes')
    parser.add_option_group(group)
    options, args = parser.parse_args(argv[1:])
    logging.basicConfig(filename=options.log_filename, level=logging.INFO + 10 * (options.quiet - options.verbose))
    logger = logging.getLogger(os.path.basename(sys.argv[0]))
    if len(args) < 1:
        logger.error('missing source operand')
        return 1
    if not options.bucket:
        logger.error('missing bucket')
        return 1
    connection = S3Connection(is_secure=options.secure)
    bucket = connection.get_bucket(options.bucket)
    del bucket
    del connection
    start = time.time()
    put_queue = JoinableQueue(1024 * options.processes)
    stat_queue = JoinableQueue()
    walk = {'filesystem': walk_filesystem, 'tar': walk_tar}[options.walk]
    walker_process = Process(target=walker, args=(walk, put_queue, args, options))
    walker_process.start()
    put = {'add': put_add, 'stupid': put_stupid, 'update': put_update}[options.put]
    putter_processes = list(islice(repeatedly(Process, target=putter, args=(put, put_queue, stat_queue, options)), options.processes))
    for putter_process in putter_processes:
        putter_process.start()
    statter_process = Process(target=statter, args=(stat_queue, start, options))
    statter_process.start()
    walker_process.join()
    for putter_process in putter_processes:
        put_queue.put(None)
    put_queue.close()
    for putter_process in putter_processes:
        putter_process.join()
    stat_queue.put(None)
    stat_queue.close()
    statter_process.join()
    put_queue.join_thread()
    stat_queue.join_thread()


if __name__ == '__main__':
    sys.exit(main(sys.argv))
