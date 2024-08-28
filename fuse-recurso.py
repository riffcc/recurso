import pyfuse3
import pyfuse3_asyncio
import os
import sys
import asyncio

from argparse import ArgumentParser
import stat
import logging
import errno
# Import the Recurso node
import recurso

try:
    import faulthandler
except ImportError:
    pass
else:
    faulthandler.enable()

log = logging.getLogger(__name__)

class RecursoFs(pyfuse3.Operations):
    def __init__(self):
        global author
        # Inititialise the Recurso file system
        super(RecursoFs, self).__init__()
        self.hello_name = b"message"
        self.hello_inode = pyfuse3.ROOT_INODE+1
        self.hello_data = b"hello recurso\n"
        self.recurso = None

    async def initialize(self):
        await self.load_recurso()

    async def load_recurso(self):
        # TODO: Allow for a ticket to be passed in

        # Start the Recurso node
        self.recurso = await recurso.setup_iroh_node()
        author = self.recurso.author
        
        # Create a root document
        root_doc_id = await recurso.create_root_document()
        root_document = await recurso.get_document(root_doc_id)

    async def getattr(self, inode, ctx=None):
        # Get attributes of given inode (file or directory)
        entry = pyfuse3.EntryAttributes()
        if inode == pyfuse3.ROOT_INODE:
            entry.st_mode = (stat.S_IFDIR | 0o755)
            entry.st_size = 0
        elif inode == self.hello_inode:
            entry.st_mode = (stat.S_IFREG | 0o644)
            entry.st_size = len(self.hello_data)
        else:
            raise pyfuse3.FUSEError(errno.ENOENT)

        stamp = int(1438467123.985654 * 1e9)
        entry.st_atime_ns = stamp
        entry.st_ctime_ns = stamp
        entry.st_mtime_ns = stamp
        entry.st_gid = os.getgid()
        entry.st_uid = os.getuid()
        entry.st_ino = inode

        return entry

    async def lookup(self, parent_inode, name, ctx=None):
        if parent_inode != pyfuse3.ROOT_INODE or name != self.hello_name:
            raise pyfuse3.FUSEError(errno.ENOENT)
        return await self.getattr(self.hello_inode)

    async def opendir(self, inode, ctx):
        if inode != pyfuse3.ROOT_INODE:
            raise pyfuse3.FUSEError(errno.ENOENT)
        print(inode)
        return inode

    async def readdir(self, fh, start_id, token):
        assert fh == pyfuse3.ROOT_INODE

        # only one entry
        if start_id == 0:
            pyfuse3.readdir_reply(
                token, self.hello_name, await self.getattr(self.hello_inode), 1)
        return

    async def open(self, inode, flags, ctx):
        if inode != self.hello_inode:
            raise pyfuse3.FUSEError(errno.ENOENT)
        if flags & os.O_RDWR or flags & os.O_WRONLY:
            raise pyfuse3.FUSEError(errno.EACCES)
        return pyfuse3.FileInfo(fh=inode)

    async def read(self, fh, off, size):
        assert fh == self.hello_inode
        return self.hello_data[off:off+size]

def init_logging(debug=False):
    formatter = logging.Formatter('%(asctime)s.%(msecs)03d %(threadName)s: '
                                  '[%(name)s] %(message)s', datefmt="%Y-%m-%d %H:%M:%S")
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    if debug:
        handler.setLevel(logging.DEBUG)
        root_logger.setLevel(logging.DEBUG)
    else:
        handler.setLevel(logging.INFO)
        root_logger.setLevel(logging.INFO)
    root_logger.addHandler(handler)

def parse_args():
    '''Parse command line'''

    parser = ArgumentParser()

    parser.add_argument('mountpoint', type=str,
                        help='Where to mount the file system')
    parser.add_argument('--debug', action='store_true', default=False,
                        help='Enable debugging output')
    parser.add_argument('--debug-fuse', action='store_true', default=False,
                        help='Enable FUSE debugging output')
    return parser.parse_args()

async def main():
    options = parse_args()

    init_logging(options.debug)

    recursofs = RecursoFs()
    await recursofs.initialize()

    fuse_options = set(pyfuse3.default_options)
    fuse_options.add('fsname=recurso')
    if options.debug_fuse:
        fuse_options.add('debug')
    pyfuse3.init(recursofs, options.mountpoint, fuse_options)
    try:
        await pyfuse3.main()
    except:
        pyfuse3.close(unmount=True)
        raise

    pyfuse3.close()


if __name__ == '__main__':
    pyfuse3_asyncio.enable()
    asyncio.run(main())