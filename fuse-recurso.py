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
        # Inititialise the Recurso file system
        super(RecursoFs, self).__init__()
        self.hello_name = b"message"
        self.hello_inode = pyfuse3.ROOT_INODE+1
        self.hello_data = b"hello recurso\n"
        self.recurso = None
        self.ticket = None

    async def load_recurso(self, ticket=None):
        global recurso

        # Start the Recurso node
        self.recurso = await recurso.setup_iroh_node(debug=debug_mode)
        
        # Create a root document
        self.root_doc_id, self.root_directory_doc_id, self.inode_map_doc_id = await recurso.create_root_document(ticket)
        # Load our root document
        root_doc = await recurso.node.docs().open(self.root_doc_id)
        # Create a ticket to join the root document
        ticket = await root_doc.share(recurso.iroh.ShareMode.WRITE, recurso.iroh.AddrInfoOptions.ID)
        print("To join another node, use this ticket: {}".format(ticket))
        print("You can use the command: `python3 fuse-recurso.py /mnt/test --ticket {}".format(ticket) + "`")

        return self.root_doc_id, self.inode_map_doc_id

    async def getattr(self, inode, ctx=None):
        # Get attributes of given inode (file or directory)
        entry = pyfuse3.EntryAttributes()
        # Clear the inode doc ID just in case
        inode_doc_id = None
        if inode == pyfuse3.ROOT_INODE or inode == "01101100011011110111011001100101":
            inode_doc_id = await recurso.get_by_key(self.inode_map_doc_id, str("01101100011011110111011001100101"))
            if debug_mode:
                print("Loaded root inode")
                print("Root inode doc ID: {}".format(inode_doc_id))
        else:
            # Lookup the inode in the central inode map
            inode_doc_id = await recurso.get_by_key(self.inode_map_doc_id, str(inode))
        print("Getting attributes for inode: {}".format(inode))

        inode_type = await recurso.get_by_key(inode_doc_id, "type")

        if debug_mode:
            print("Inode type: {}".format(inode_type))

        # Lookup the metadata for the inode
        metadata = await recurso.find_and_fetch_metadata_for_doc_id(inode_doc_id)

        # If the inode is a directory, update the size based on the number of children
        if inode_type == "directory":
            children_doc_id = await recurso.get_by_key(inode_doc_id, "children")
            children_document = await recurso.get_document(children_doc_id)
            children = await recurso.get_all_keys_by_prefix(children_document, "fs")
            entry.st_size = len(children)
        else:
            entry.st_size = metadata["st_size"]

        if inode == pyfuse3.ROOT_INODE:
            # Force root to always be 0o755 permissions
            entry.st_mode = (stat.S_IFDIR | 0o755)
        else:
            entry.st_mode = metadata["st_mode"]
        entry.st_atime_ns = recurso.convert_seconds_to_ns(metadata["st_atime"])
        entry.st_ctime_ns = recurso.convert_seconds_to_ns(metadata["st_ctime"])
        entry.st_mtime_ns = recurso.convert_seconds_to_ns(metadata["st_mtime"])
        entry.st_gid = metadata["st_gid"]
        entry.st_uid = metadata["st_uid"]
        entry.st_ino = int(inode)
        return entry

    async def lookup(self, parent_inode, name, ctx=None):
        print("Lookup called for: {}".format(name))
        # if parent_inode != pyfuse3.ROOT_INODE or name != self.hello_name:
        if parent_inode == pyfuse3.ROOT_INODE:
            print("Parent inode is root")
            # Find the base document for the root
            root_document_inode = "01101100011011110111011001100101"
            # Find the document ID for the root document
            root_document_doc_id = await recurso.get_by_key(self.inode_map_doc_id, root_document_inode)
            # Grab the metadata document for the root document
            metadata_doc_id = await recurso.get_by_key(root_document_doc_id, "metadata")
            # Grab the inode number from the metadata document
            parent_inode = await recurso.get_by_key(metadata_doc_id, "st_ino")
        print("Parent inode: {}".format(parent_inode))

        # Look up the parent inode document in the inode map
        parent_inode_doc_id = await recurso.get_by_key(self.inode_map_doc_id, str(parent_inode))
        print("Parent inode doc ID: {}".format(parent_inode_doc_id))

        # Load the children document from the parent inode
        children_doc_id = await recurso.get_by_key(parent_inode_doc_id, "children")
        print("Children doc ID: {}".format(children_doc_id))

        # Convert name from bytes to a string
        name = name.decode("utf8")
    
        print("Looking for lost child: {}".format(name))

        # Lookup the key name in the children document
        # Try it as a directory first
        try:
            child_doc_id = await recurso.get_by_key(children_doc_id, "fsdir-" + name + "...RECURSO.UNiQ.v0")
        except Exception as e:
            # This is a soft error, we might be looking for a file
            pass
        # If we didn't find a directory, try a file
        if not child_doc_id or child_doc_id == None:
            try:
                child_doc_id = await recurso.get_by_key(children_doc_id, "fsfile-" + name + "...RECURSO.UNiQ.v0")
            except Exception as e:
                # If we got here, we couldn't find a file or directory with that name
                print("Could not find child metadata for {}".format(name))
                raise pyfuse3.FUSEError(errno.ENOENT)
        if debug_mode:
            print("Found child doc ID: {}".format(child_doc_id))
            print("Pulling metadata for child doc ID: {}".format(child_doc_id))
        # We've got a place to pull metadata, let's get the inode
        metadata_doc_id = await recurso.get_by_key(child_doc_id, "metadata")
        if debug_mode:
            print("Child metadata doc ID: {}".format(metadata_doc_id))
        inode = await recurso.get_by_key(metadata_doc_id, "st_ino")
        if debug_mode:
            print("Child inode: {}".format(inode))
        return await self.getattr(inode)

    async def opendir(self, inode, ctx):
        # We're opening a directory, so we should figure out
        # * That it exists
        # * That we can access it
        # * Return a handle that will be used in readdir
        # * That it has a valid 64-bit inode
        # For now we'll only have compatibility with the root directory
        # if inode != pyfuse3.ROOT_INODE:
        #     raise pyfuse3.FUSEError(errno.ENOENT)
        if inode == pyfuse3.ROOT_INODE:
            # Override the root inode ("1") to the actual root inode
            inode = "01101100011011110111011001100101"

        print("Attempting to open directory: {}".format(inode))
        directory_doc_id = await recurso.get_by_key(root_doc_id, "directory")
        metadata_doc_id = await recurso.get_by_key(directory_doc_id, "metadata")
        metadata = await recurso.get_metadata(metadata_doc_id)
        inode = metadata["st_ino"]
        return inode

    async def readdir(self, fh, start_id, token):
        # Make sure we have a pointer to the inode map document
        if not self.inode_map_doc_id:
            print("Panic! No inode map ID found!")
            sys.exit(1)

        # Lookup the directory by inode from the central inode map
        directory_doc_id = await recurso.get_by_key(self.inode_map_doc_id, str(fh))

        # Lookup the metadata for the directory
        metadata = await recurso.find_and_fetch_metadata_for_doc_id(directory_doc_id)

        # Lookup the children for the directory which will contain the list of child files and directories
        children_doc_id = await recurso.get_by_key(directory_doc_id, "children")
        # Grab the children document
        children_document = await recurso.get_document(children_doc_id)

        # List directory children
        children = {}
        children["dirs"] = await recurso.get_all_keys_by_prefix(children_document, "fsdir")
        children["files"] = await recurso.get_all_keys_by_prefix(children_document, "fsfile")

        # If no children are found, return an empty list
        if not children:
            return []
        
        # Create a list of children from merging the two lists
        children_list = children["dirs"] + children["files"]
        # Sort children to ensure consistent order
        children_list.sort(key=lambda x: x.key())

        # Iterate over children, respecting the start_id
        for i, entry in enumerate(children_list):
            if i < start_id:
                continue
            real_name = entry.key().decode("utf8")
            real_name = real_name[real_name.find("-") + 1:]
            real_name = real_name.split('...RECURSO.UNiQ.v0')[0]
            hash = entry.content_hash()
            content = await entry.content_bytes(children_document)

            # To render properly...
            # We'll need to grab the document for that inode
            inode_doc_id = content.decode("utf8")
            # Get the metadata for the inode
            metadata = await recurso.find_and_fetch_metadata_for_doc_id(inode_doc_id)

            # Fetch the inode number
            real_inode = metadata["st_ino"]

            # Set attributes for the entry
            try:
                print("Getting attributes for: {}".format(real_inode))
                entry_attributes = await self.getattr(real_inode)
            except Exception as e:
                print("Error getting attributes for inode: {}".format(real_inode))
                print(e)
                continue

            print("Getting entry attributes for: {}".format(real_name))
            
            pyfuse3.readdir_reply(
                token, bytes(real_name, "utf8"), entry_attributes, i + 1)
        return

    async def open(self, inode, flags, ctx):
        print("Opening inode: {}".format(inode))
        if flags & os.O_RDWR or flags & os.O_WRONLY:
            raise pyfuse3.FUSEError(errno.EACCES)
        return pyfuse3.FileInfo(fh=inode)

    async def read(self, fh, off, size):
        print("Reading from inode: {}".format(fh))
        # Check that the inode exists by looking it up in the inode map
        try:
            # Find the document ID for the inode, based on the file handle we're reading from
            inode_doc_id = await recurso.get_by_key(self.inode_map_doc_id, fh)
        except Exception as e:
            print("Could not get inode document for inode/file handle: {}".format(fh))
            raise pyfuse3.FUSEError(errno.ENOENT)
        # Fetch the file using the blobhash
        blobhash = await recurso.get_by_key(inode_doc_id, "blob")
        file = await recurso.get_blob(blobhash)
        # Return the data
        return file[off:off+size]

    async def unlink(self, parent_inode, name, ctx):
        print(f"Deleting file: {name} from parent inode: {parent_inode}")

        # Convert name from bytes to a string
        name = name.decode("utf8")

        # Look up the parent inode document in the inode map
        parent_inode_doc_id = await recurso.get_by_key(self.inode_map_doc_id, str(parent_inode))

        # Load the children document from the parent inode
        children_doc_id = await recurso.get_by_key(parent_inode_doc_id, "children")

        try:
            # Try to find the file in the children document
            child_doc_id = await recurso.get_by_key(children_doc_id, recurso.encode_filename(name, "file"))
        except Exception as e:
            # If we can't find the file, raise an error
            raise pyfuse3.FUSEError(errno.ENOENT)

        # Get the inode of the file to be deleted
        metadata_doc_id = await recurso.get_by_key(child_doc_id, "metadata")
        inode = await recurso.get_by_key(metadata_doc_id, "st_ino")

        # Remove the file entry from the parent's children document
        await recurso.delete_key(children_doc_id, recurso.encode_filename(name, "file"))

        # Remove the file's inode entry from the inode map
        await recurso.delete_key(self.inode_map_doc_id, str(inode))

        # Delete the file's document and associated metadata
        await recurso.delete_document(child_doc_id)
        await recurso.delete_document(metadata_doc_id)

        # If the file has an associated blob, delete it
        try:
            blobhash = await recurso.get_by_key(child_doc_id, "blob")
            await recurso.delete_blob(blobhash)
        except Exception as e:
            # If there's no blob, we can ignore this error
            pass

        print(f"File {name} successfully deleted")


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
    parser.add_argument('--ticket', type=str, default=False, 
                        help='ticket to join a root document. If provided, will attempt to join a cluster')
    return parser.parse_args()

async def main():
    global root_doc_id
    global author
    global debug_mode

    debug_mode = False
    ticket = None
    options = parse_args()

    if options.debug:
        debug_mode = True

    init_logging(options.debug)

    recursofs = RecursoFs()
    if options.ticket:
        ticket = recurso.iroh.DocTicket(options.ticket)
    root_doc_id, inode_map_doc_id = await recursofs.load_recurso(ticket)

    fuse_options = set(pyfuse3.default_options)
    fuse_options.add('fsname=recurso')
    fuse_options.discard('default_permissions')
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