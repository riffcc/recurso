import iroh
import argparse
import asyncio
import time
import uuid
import stat
import random
import string
import decode_ticket
import json
import queue
import base64
from blake3 import blake3

# Utility functions
# These take docs, not doc IDs
async def get_all_keys(doc):
    query = iroh.Query.all(None)
    entries = await doc.get_many(query)
    return entries

async def get_all_keys_by_prefix(doc, prefix):
    query = iroh.Query.key_prefix(bytes(str(prefix), "utf-8"), None)
    entries = await doc.get_many(query)
    return entries

async def print_all_keys(doc):
    entries = await get_all_keys(doc)
    for entry in entries:
        key = entry.key()
        hash = entry.content_hash()
        content = await entry.content_bytes(doc)
        print("{} : {} (hash: {})".format(key, content.decode("utf8"), hash))

# These take doc IDs
async def get_by_key(doc_id, keyname):
    # Fetch the directory document from a key within a doc
    # Get the document we were passed
    try:
        doc = await node.docs().open(doc_id)
        # Lookup key
        key_entry = await doc.get_exact(author, bytes(str(keyname), "utf-8"), False)
        if key_entry is None:
            if debug_mode:
                print(f"Key '{keyname}' not found in document {doc_id}")
            return None
        key_doc_id = await key_entry.content_bytes(doc)
        # Decode the key_doc_id from bytes to string
        key_doc_id = key_doc_id.decode("utf-8")
        # Return the directory document ID
        return key_doc_id
    except Exception as e:
        print(f"Error in get_by_key for key '{keyname}': {str(e)}")
        return None

# Encode a filename into a valid keyname for a children document.
async def encode_filename(name, type):
    if type == "file":
        return "fsfile-" + name + '...RECURSO.UNiQ.v0'
    elif type == "directory":
        return "fsdir-" + name + '...RECURSO.UNiQ.v0'

async def decode_filename(keyname):
    if keyname.startswith("fsfile-"):
        return "file", keyname[7:].split('...RECURSO.UNiQ.v0')[0]
    elif keyname.startswith("fsdir-"):
        return "directory", keyname[6:].split('...RECURSO.UNiQ.v0')[0]
    else:
        return None, None

# Accepts bytes for the value. Make sure to convert to bytes before using this function.
async def set_by_key(doc_id, keyname, value):
    # Get the document we were passed
    try:
        doc = await node.docs().open(doc_id)
        # Set the value
        await doc.set_bytes(author, bytes(str(keyname), "utf-8"), value)
    except Exception as e:
        print(f"Error in set_by_key for key '{keyname}': {str(e)}")
        return None
    
async def delete_key(doc_id, keyname):
    # Get the document we were passed
    try:
        doc = await node.docs().open(doc_id)
        # Delete the value
        await doc.delete(author, bytes(str(keyname), "utf-8"))
    except Exception as e:
        print(f"Error in delete_key for key '{keyname}': {str(e)}")
        return None
    
async def delete_document(doc_id):
    # Get the document we were passed
    try:
        doc = await node.docs().drop(doc_id)
    except Exception as e:
        print(f"Error in delete_document for doc '{doc_id}': {str(e)}")
        return None
    
async def delete_blob(blob_hash):
    # Get the blob we were passed
    try:
        blob = await node.blobs().delete_blob(blob_hash)
    except Exception as e:
        print(f"Error in delete_blob for blob '{blob_hash}': {str(e)}")
        return None

# These take seconds
def convert_seconds_to_ns(seconds):
    seconds_to_ns = int(seconds * 1e9)
    return seconds_to_ns

# Main functions
async def scan_root_document(doc_id):
    print("Scanning root document {}".format(doc_id))
    doc = await node.docs().open(doc_id)
    # Fetch all keys from the root document
    query = iroh.Query.all(None)
    entries = await doc.get_many(query)
    type_is_correct = False
    for entry in entries:
        key = entry.key()
        hash = entry.content_hash()
        content = await entry.content_bytes(doc)
        if key == b"type":
            print("Type: {}".format(content.decode("utf8")))
            # Check if the type is set to "root document"
            if content.decode("utf8") == "root":
                # Mark type as correct so that we can check version
                type_is_correct = True
                print("Root document found")
            # Check if the type is set to anything other than "root document", but exists:
            else:
                print("Found a document of type: {}".format(content.decode("utf8")))
                print("Was expecting a root document. Bailing!")
                return "err_not_root"
        if key == b"version":
            # Check version is v0
            if content.decode("utf8") == "v0":
                print("Root document is v0")
                if type_is_correct:
                    return "ok"
                else:
                    return "err_not_root"
            else:
                print("Root document is not v0, bailing!")
                return "err_not_v0"
    else:
        print("No type set and no odd markers found. Creating as empty rootdoc...")
        print("Dumping all entries")
        for entry in entries:
            key = entry.key()
            hash = entry.content_hash()
            content = await entry.content_bytes(doc)
            print("{} : {} (hash: {})".format(key, content.decode("utf8"), hash))
        return "empty"

async def create_children_document(inode_map_doc_id):
    print("Creating children document")
    # Create the children document and fetch its ID
    doc = await node.docs().create()
    children_doc_id = doc.id()
    # Create the children document itself
    await doc.set_bytes(author, b"type", b"children")
    await doc.set_bytes(author, b"version", b"v0")
    await doc.set_bytes(author, b"created", bytes(str(time.time()), "utf-8"))
    await doc.set_bytes(author, b"updated", bytes(str(time.time()), "utf-8"))
    print("Created children document: {}".format(children_doc_id))
    # Debug mode: print out the doc we just created
    if debug_mode:
        # Fetch all keys from the document
        await print_all_keys(doc)
    return children_doc_id

# Create a metadata document with the name of a file or directory as well as its DirectoryDoc or FileDoc ID
async def create_metadata_document(name, type, doc_id, inode_map_doc_id, size):
    print("Creating metadata document")
    # Create the metadata document and fetch its ID
    doc = await node.docs().create()
    metadata_doc_id = doc.id()
    # Create the metadata document itself
    await doc.set_bytes(author, b"type", b"metadata")
    # Set the barename of the file.
    await doc.set_bytes(author, b"name", bytes(str(name), "utf-8"))
    await doc.set_bytes(author, b"version", b"v0")
    await doc.set_bytes(author, b"created", bytes(str(time.time()), "utf-8"))
    await doc.set_bytes(author, b"updated", bytes(str(time.time()), "utf-8"))

    # Generate an initial inode
    # 1. Generate a UUID
    uuid_value = uuid.uuid4()    
    # 2. Convert UUID to a 64-bit integer
    st_ino = uuid_value.int & 0xFFFFFFFFFFFFFFFF

    # If the type is a directory, set the st_mode to S_IFDIR
    if type == "directory":
        st_mode = stat.S_IFDIR | 0o755
    else:
        st_mode = stat.S_IFREG | 0o644

    # Initial metadata to populate the metadata document 
    metadata = {
        "st_mode": st_mode,  # Directory with rwxr-xr-x permissions
        "st_ino": st_ino,   # Generated inode number (UUID-based)
        "st_uid": 0,   # Root user ID
        "st_gid": 0,   # Root group ID
        "st_size": size,  # Initial size (empty if directory, size if file)
        "st_atime": int(time.time()), # Time of last access
        "st_mtime": int(time.time()), # Time of last modification
        "st_ctime": int(time.time()), # Time of last status change
    }

    # Set the metadata as individual keys in the document
    for key, value in metadata.items():
        await doc.set_bytes(author, bytes(key, "utf-8"), bytes(str(value), "utf-8"))

    # Load the inode map document
    inode_map_doc = await node.docs().open(inode_map_doc_id)
    print("Loaded inode map document: {}".format(inode_map_doc_id))

    # Push the origin document ID into the central inode map
    print("Pushing inode map item name {} for inode {}".format(name, metadata["st_ino"]))
    await inode_map_doc.set_bytes(author, bytes(str(metadata["st_ino"]), "utf-8"), bytes(str(doc_id), "utf-8"))

    print("Created metadata document: {}".format(metadata_doc_id))
    # Debug mode: print out the doc we just created
    if debug_mode:
        # Fetch all keys from the document
        await print_all_keys(doc)
    return metadata_doc_id, st_ino

async def create_directory_document(name, inode_map_doc_id, ticket_doc_id):
    print("Creating directory document")
    doc = await node.docs().create()
    directory_doc_id = doc.id()
    # Create the children document and fetch its ID
    children_doc_id = await create_children_document(inode_map_doc_id)
    # Create the metadata document and fetch its ID
    metadata_doc_id, st_ino = await create_metadata_document(name, "directory", directory_doc_id, inode_map_doc_id, 0)
    # Create the directory document
    await doc.set_bytes(author, b"type", b"directory")
    await doc.set_bytes(author, b"version", b"v0")
    await doc.set_bytes(author, b"created", bytes(str(time.time()), "utf-8"))
    await doc.set_bytes(author, b"updated", bytes(str(time.time()), "utf-8"))
    await doc.set_bytes(author, b"metadata", bytes(metadata_doc_id, "utf-8"))
    await doc.set_bytes(author, b"children", bytes(children_doc_id, "utf-8"))
    print("Created directory document: {}".format(directory_doc_id))
    # Create a ticket to join the directory document
    writable_ticket = await doc.share(iroh.ShareMode.WRITE, iroh.AddrInfoOptions.RELAY_AND_ADDRESSES)
    print("Created writable ticket: {}".format(writable_ticket))
    # Add that ticket to the tickets document
    tickets_doc = await node.docs().open(ticket_doc_id)
    await tickets_doc.set_bytes(author, bytes('inode_' + str(st_ino) + '-', "utf-8"), bytes(str(writable_ticket), "utf-8"))
    # Create a ticket to join the metadata document
    metadata_doc = await node.docs().open(metadata_doc_id)
    writable_ticket_metadata = await metadata_doc.share(iroh.ShareMode.WRITE, iroh.AddrInfoOptions.RELAY_AND_ADDRESSES)
    await tickets_doc.set_bytes(author, bytes('inode_' + str(st_ino) + '_metadata', "utf-8"), bytes(str(writable_ticket_metadata), "utf-8"))
    # Create a ticket to join the children document
    children_doc = await node.docs().open(children_doc_id)
    writable_ticket_children = await children_doc.share(iroh.ShareMode.WRITE, iroh.AddrInfoOptions.RELAY_AND_ADDRESSES)
    await tickets_doc.set_bytes(author, bytes('inode_' + str(st_ino) + '_children', "utf-8"), bytes(str(writable_ticket_children), "utf-8"))
    # Debug mode: print out the doc we just created
    if debug_mode:
        # Fetch all keys from the document
        await print_all_keys(doc)

    return directory_doc_id

async def create_file_document(name, size, blob_hash, inode_map_doc_id, ticket_doc_id):
    print("Creating file document")
    doc = await node.docs().create()
    file_doc_id = doc.id()
    # Create the metadata document and fetch its ID
    metadata_doc_id, st_ino = await create_metadata_document(name, "file", file_doc_id, inode_map_doc_id, size)
    # Create the file document
    await doc.set_bytes(author, b"type", b"file")
    await doc.set_bytes(author, b"version", b"v0")
    await doc.set_bytes(author, b"created", bytes(str(time.time()), "utf-8"))
    await doc.set_bytes(author, b"updated", bytes(str(time.time()), "utf-8"))
    await doc.set_bytes(author, b"metadata", bytes(str(metadata_doc_id), "utf-8"))
    await doc.set_bytes(author, b"blob", bytes(str(blob_hash), "utf-8"))
    await doc.set_bytes(author, b"size", bytes(str(size), "utf-8"))
    print("Created file document: {}".format(file_doc_id))

    # Create a ticket to join the file document
    writable_ticket = await doc.share(iroh.ShareMode.WRITE, iroh.AddrInfoOptions.RELAY_AND_ADDRESSES)
    print("Created writable ticket: {}".format(writable_ticket))
    # Add that ticket to the tickets document
    tickets_doc = await node.docs().open(ticket_doc_id)
    await tickets_doc.set_bytes(author, bytes('inode_' + str(st_ino) + '-', "utf-8"), bytes(str(writable_ticket), "utf-8"))
    # Create a ticket to join the metadata document
    metadata_doc = await node.docs().open(metadata_doc_id)
    writable_ticket_metadata = await metadata_doc.share(iroh.ShareMode.WRITE, iroh.AddrInfoOptions.RELAY_AND_ADDRESSES)
    await tickets_doc.set_bytes(author, bytes('inode_' + str(st_ino) + '_metadata', "utf-8"), bytes(str(writable_ticket_metadata), "utf-8"))
    # Create a ticket to sync the blob
    hash = iroh.Hash.from_string(str(blob_hash))    
    blob_format = iroh.BlobFormat.RAW
    ticket = await node.blobs().share(hash, blob_format, iroh.AddrInfoOptions.RELAY_AND_ADDRESSES)
    print("Created blob ticket: {}".format(ticket))
    # Add that ticket to the tickets document
    tickets_doc = await node.docs().open(ticket_doc_id)
    await tickets_doc.set_bytes(author, bytes('inode_' + str(st_ino) + '_blob', "utf-8"), bytes(str(ticket), "utf-8"))

    # Insert the file document ID into the inode map
    await set_by_key(inode_map_doc_id, bytes(str(st_ino), "utf-8"), bytes(str(file_doc_id), "utf-8"))

    # Debug mode: print out the doc we just created
    if debug_mode:
        # Fetch all keys from the document
        await print_all_keys(doc)

    return file_doc_id

async def create_dummy_file_document(name, size, inode_map_doc_id, ticket_doc_id):
    print("Creating dummy file and document")

    # Generate a random file of the size we want.
    random_file_contents = ''.join(random.choice(string.ascii_letters + string.digits) for _ in range(size))

    # Upload the file to Iroh
    add_outcome = await node.blobs().add_bytes(bytes(random_file_contents, "utf-8"))
    assert add_outcome.format == iroh.BlobFormat.RAW
    assert add_outcome.size == size

    print("add_outcome.hash: {}".format(add_outcome.hash))

    file_doc_id = await create_file_document(name, size, add_outcome.hash, inode_map_doc_id, ticket_doc_id)
    # Insert the uploaded blob to the file document
    await set_by_key(file_doc_id, "blob", bytes(str(add_outcome.hash), "utf-8"))
    return file_doc_id

async def create_root_document(ticket=False):
    global node
    # Find or create a root document for Recurso to use.
    # If we've been given a ticket
    if ticket:
        # We convert the ticket from a string to a DocTicket
        ticket = iroh.DocTicket(ticket)
        doc = await node.docs().join(ticket)
        doc_id = doc.id()
        print("Joined root doc: {}".format(doc_id))
    else:
        # Get a ticket for the 
        doc = await node.docs().create()
        doc_id = doc.id()
        print("Created new (blank) initial root doc: {}".format(doc_id))
    # Without this sleep, sync issues occur
    time.sleep(1)
    ticket_doc_id = await create_ticket_document()
    status = await scan_root_document(doc_id)
    print("Scan status: {}".format(status))
    if status == "ok":
        # Found a root document, return it
        # Scan the document for the directoy and inode map doc IDs
        print("Loading existing directory and inode map document IDs")
        directory_doc_id = await get_by_key(doc_id, "directory")
        inode_map_doc_id = await get_by_key(doc_id, "inode_map")
        return doc_id, directory_doc_id, inode_map_doc_id, ticket_doc_id
    elif status == "empty":
        # No root document found, create a new one and fetch the result
        directory_doc_id, inode_map_doc_id = await create_new_root_document(doc_id, ticket_doc_id)
        return doc_id, directory_doc_id, inode_map_doc_id, ticket_doc_id
    elif status == "err_not_root":
        # Found a document of type other than "root document"
        print("Found a document of type other than 'root document'. Bailing!")
        # Error out
        return None, None, None, None

async def create_ticket_document():
    print("Creating node inode document")
    doc = await node.docs().create()
    ticket_doc_id = doc.id()
    node_id = await node.net().node_id()
    print("Created node inode document: {}".format(ticket_doc_id))

    # Set the type, version, created, updated, and inode_map keys
    await doc.set_bytes(author, b"type", b"active_node_inodes")
    await doc.set_bytes(author, b"version", b"v0")
    await doc.set_bytes(author, b"node_id", bytes(str(node_id), "utf-8"))
    await doc.set_bytes(author, b"created", bytes(str(time.time()), "utf-8"))
    await doc.set_bytes(author, b"updated", bytes(str(time.time()), "utf-8"))

    return ticket_doc_id

async def create_inode_map_document():
    # Create a new inode map document.
    # Takes the root directory's document ID as an argument
    # DO NOT pass this any other document ID including the root document itself
    print("Creating inode map document")
    doc = await node.docs().create()
    inode_map_doc_id = doc.id()

    # Set the type, version, created, updated, and inode_map keys
    await doc.set_bytes(author, b"type", b"inode_map")
    await doc.set_bytes(author, b"version", b"v0")
    await doc.set_bytes(author, b"created", bytes(str(time.time()), "utf-8"))
    await doc.set_bytes(author, b"updated", bytes(str(time.time()), "utf-8"))

    print("Created inode map document: {}".format(inode_map_doc_id))
    # Debug mode: print out the doc we just created
    if debug_mode:
        # Fetch all keys from the document
        await print_all_keys(doc)
    return inode_map_doc_id

async def create_new_root_document(doc_id, ticket_doc_id):
    print("Creating new root document in: {}".format(doc_id))
    doc = await node.docs().open(doc_id)

    # Load the ticket map
    # Create the inode map document and fetch its ID
    inode_map_doc_id = await create_inode_map_document()
    # Create a ticket to join the inode map document
    writable_ticket = await doc.share(iroh.ShareMode.WRITE, iroh.AddrInfoOptions.RELAY_AND_ADDRESSES)
    print("Created writable ticket for the inode map: {}".format(writable_ticket))
    # Add the inode map ticket to the ticket map
    await set_by_key(ticket_doc_id, bytes(str("01101100011011110111011001100101"), "utf-8"), bytes(str(writable_ticket), "utf-8"))

    # Create the directory document and fetch its ID
    directory_doc_id = await create_directory_document("RECURSO_ROOT_DIRECTORY", inode_map_doc_id, ticket_doc_id)

    # Create the root document
    await doc.set_bytes(author, b"type", b"root")
    await doc.set_bytes(author, b"version", b"v0")
    await doc.set_bytes(author, b"created", bytes(str(time.time()), "utf-8"))
    await doc.set_bytes(author, b"updated", bytes(str(time.time()), "utf-8"))
    await doc.set_bytes(author, b"directory", bytes(directory_doc_id, "utf-8"))
    await doc.set_bytes(author, b"inode_map", bytes(inode_map_doc_id, "utf-8"))

    # Now we push the root document ID into the inode map
    # Load in the inode_map_doc
    inode_map_doc = await node.docs().open(inode_map_doc_id)

    # Fetch the inode number for the root directory's directory document
    metadata = await find_and_fetch_metadata_for_doc_id(directory_doc_id)
    # Set the inode number for the root directory to be equal to the document ID for the root directory's document
    await inode_map_doc.set_bytes(author, bytes(str("01101100011011110111011001100101"), "utf-8"), bytes(str(directory_doc_id), "utf-8"))
    # Set the real inode number to be equal to the document ID for the root directory's document
    await inode_map_doc.set_bytes(author, bytes(str(metadata["st_ino"]), "utf-8"), bytes(str(directory_doc_id), "utf-8"))

    # Create dummy files, push them into the children list
    children_doc_id = await get_by_key(directory_doc_id, "children")
    created_file_id = await create_dummy_file_document("example.txt", 5, inode_map_doc_id, ticket_doc_id)
    await set_by_key(children_doc_id, await encode_filename("example.txt", "file"), bytes(str(created_file_id), "utf-8"))
    created_file_id = await create_dummy_file_document("example2.txt", 512, inode_map_doc_id, ticket_doc_id)
    await set_by_key(children_doc_id, await encode_filename("example2.txt", "file"), bytes(str(created_file_id), "utf-8"))
    created_file_id = await create_dummy_file_document("hello.txt", 1024, inode_map_doc_id, ticket_doc_id)
    await set_by_key(children_doc_id, await encode_filename("hello.txt", "file"), bytes(str(created_file_id), "utf-8"))
    created_file_id = await create_dummy_file_document("world.txt", 10240, inode_map_doc_id, ticket_doc_id)
    await set_by_key(children_doc_id, await encode_filename("world.txt", "file"), bytes(str(created_file_id), "utf-8"))

    # Check that we have a valid inode map document
    assert inode_map_doc_id

    return directory_doc_id, inode_map_doc_id

# Find the metadata document ID within a DirectoryDoc or FileDoc
# Then use get_metadata to fetch the metadata from within it
async def find_and_fetch_metadata_for_doc_id(doc_id):
    # Get inode and other directory info from a DirectoryDoc or FileDoc
    doc = await node.docs().open(doc_id)
    # Lookup metadata key
    if debug_mode:
        print("Attempting to get metadata for " + doc_id)
    metadata_entry = await doc.get_exact(author, b"metadata", False)
    metadata_doc_id = await metadata_entry.content_bytes(doc)
    # Fetch metadata
    document_metadata = await get_metadata(metadata_doc_id.decode("utf-8"))
    # Debug mode: print out the metadata we just fetched
    if debug_mode:
        print(document_metadata)
    return document_metadata

async def get_metadata(doc_id):
    # Fetch the metadata document
    metadata_doc = await node.docs().open(doc_id)

    # Metadata to fetch
    metadata = {
        "st_mode": "UNLOADED",
        "st_ino": "UNLOADED",   # Generated inode number (UUID-based)
        "st_uid": "UNLOADED",   # Root user ID
        "st_gid": "UNLOADED",   # Root group ID
        "st_size": "UNLOADED",  # Initial size (empty directory)
        "st_atime": "UNLOADED", # Time of last access
        "st_mtime": "UNLOADED", # Time of last modification
        "st_ctime": "UNLOADED"  # Time of creation
    }

    # Populate the metadata dictionary with actual values
    for key, _ in metadata.items():
        entry = await metadata_doc.get_exact(author, key.encode(), False)
        if entry:
            value = await entry.content_bytes(metadata_doc)
            metadata[key] = int(value.decode())

    return metadata

async def get_document(doc_id):
    doc = await node.docs().open(doc_id)
    return doc

async def get_blob(blob_hash):
    print("Trying to grab blob: {}".format(blob_hash))
    hash = iroh.Hash.from_string(blob_hash)
    print("hash: {}".format(str(hash)))
    blob = await node.blobs().read_to_bytes(hash)
    print("read_to_bytes {}", blob)
    return blob

async def setup_iroh_node(ticket=False, debug=False):
    global node
    global author
    global debug_mode
    # setup event loop, to ensure async callbacks work
    iroh.iroh_ffi.uniffi_set_event_loop(asyncio.get_running_loop())

    print("Starting Recurso Distributed File System")

    # set debug mode based on debug flag
    debug_mode = debug

    # create iroh node
    node = await iroh.Iroh.memory()
    node_id = await node.net().node_id()
    print("Started Iroh node: {}".format(node_id))

    # Get and set default author globally
    first_author = await node.authors().default()
    # Set a new author
    author = await node.authors().import_author(iroh.Author.from_string("3huxdx54bapti2vmbtpfnrkiw2fpxy2ryod6bogns5nwqdy6zjba"))
    authors = await node.authors().list()
    assert len(authors) == 2

    if debug_mode:
        print(f"Default author: {author}")

async def gossip_loop(ticket, gossip_topic):
    global node
    # We're going to keep track of which nodes are connected to our gossip loop
    gossip_nodes = []

    # If we've been given a ticket, grab the node ID and add it to the connected nodes list
    if ticket:
        # Decode the ticket's node ID
        ticket_decoded = decode_ticket.decode_iroh_ticket(ticket)
        # List the nodes
        initial_gossip_nodes = ticket_decoded.nodes
        # Add the NodeAddr objects to the connected nodes list
        gossip_nodes.extend(initial_gossip_nodes)
        # Print out the initial nodes we'll connect to
        print("Nodes to connect to: {}".format(
            [gossip_node.node_id for gossip_node in gossip_nodes]
        ))

        for gossip_node in gossip_nodes:
            public_key = iroh.PublicKey.from_string(gossip_node.node_id)
            derp_url = gossip_node.info.derp_url
            addresses = gossip_node.info.direct_addresses
            node_addr = iroh.NodeAddr(
                node_id=public_key,
                derp_url=derp_url,  # This is optional, you can pass None if not needed
                addresses=addresses
            )
            await node.net().add_node_addr(node_addr)
            print("We connected to node {}".format(gossip_node.node_id))
        # Subscribe to the gossip topic
        my_node_id = await node.net().node_id()
        cb0 = GossipCallback(my_node_id)
        print("Subscribing to gossip topic: {} as node {}".format(gossip_topic, my_node_id))
        sink0 = await node.gossip().subscribe(gossip_topic, [gossip_node.node_id], cb0)

    # If we haven't been given a ticket, we're just going to listen for control messages
    else:
        print("Listening for control messages")
        cb0 = GossipCallback(node.net().node_id())
        # Convert gossip topic to base64 so that we can display it
        translated_gossip_topic = base64.b64encode(gossip_topic)
        print("Subscribing to gossip topic: {} as node {}".format(translated_gossip_topic, await node.net().node_id()))
        sink0 = await node.gossip().subscribe(gossip_topic, [await node.net().node_id()], cb0)

    while True:
        event = await cb0.chan.get()
        if debug_mode:
            print("<<", event.type())
        if (event.type() == iroh.MessageType.JOINED):
            if debug_mode:
                print(">>", event.type())
             # Broadcast message from whichever nodes did not join
            msg_content = '{{"msg": "Hello, join me!", "node_id": "{}", "join_ticket": "{}"}}'.format(
                await node.net().node_id(), read_only_ticket
            )
            msg_content = bytearray(msg_content.encode("utf-8"))

            await sink0.broadcast(msg_content)
        elif (event.type() == iroh.MessageType.RECEIVED):
            # We received a message, let's read it
            message = event.as_received().content.decode("utf-8")
            # Load the message as a JSON object
            message = json.loads(message)
            if message["msg"] == "Hello, join me!": 
                # Notify the user that a node gave us an offer to join
                print("Node {} joined and asked us to sync from them.".format(message["node_id"]))
                asyncio.create_task(sync_from_node(node, message["join_ticket"]))
        await asyncio.sleep(1)

async def watch_document(node, doc, event_queue, tickets_doc):
    while True:
        try:
            print("Watching document")
            update = await event_queue.get()
            print("Update: {}".format(update))
            await process_document_update(node, doc, update, tickets_doc)
        except asyncio.CancelledError:
            # Handle cancellation if needed
            break
        except Exception as e:
            print(f"Error processing update: {e}")
            # Optionally, add a small delay before continuing
            await asyncio.sleep(0.1)

async def process_document_update(node, doc, update, tickets_doc):
    if update.type() == iroh.WatchEventType.INSERT:
        content = await update.content_bytes(doc)
        print(update.key())
        print("TRIGGERED")
        if update.key() == b"join_ticket":
            new_ticket = content.decode()
            new_doc, event_queue = await join_and_watch_document(node, iroh.DocTicket(new_ticket))
            if event_queue:
                asyncio.create_task(watch_document(node, new_doc, event_queue, tickets_doc))

async def sync_from_node(node, read_only_ticket):
    remote_tickets_doc, event_queue = await join_and_watch_document(node, read_only_ticket)
    remote_node_id = decode_ticket.decode_iroh_ticket(read_only_ticket).nodes[0].node_id
    print("Syncing {}".format(read_only_ticket) + " from node: {}".format(remote_node_id))
    await asyncio.sleep(1)
    if remote_tickets_doc:
        # Load everything once
        first_tickets = await get_all_keys_by_prefix(remote_tickets_doc, "inode_")
        # Iterate over children, respecting the start_id
        for i, entry in enumerate(first_tickets):
            # Print the key
            # Load the ticket
            ticket_data = await entry.content_bytes(remote_tickets_doc)
            ticket_data = ticket_data.decode()
            print("iteration: {}".format(i))
            # If the ticket is for a blob, join it and watch it
            if b"_blob" in entry.key():
                print("Syncing blob ticket")
                decoded_ticket = decode_ticket.decode_iroh_ticket(ticket_data)
                # We should consider downloading the blob here
                always_download_blobs = True
                if always_download_blobs:
                    cb = AddCallback()
                    nodeaddr = iroh.NodeAddr(iroh.PublicKey.from_string(decoded_ticket.node.node_id), decoded_ticket.node.info.derp_url, decoded_ticket.node.info.direct_addresses)
                    hash = iroh.Hash.from_string(decoded_ticket.hash)
                    opts = iroh.BlobDownloadOptions(iroh.BlobFormat.RAW, [nodeaddr], iroh.SetTagOption.auto())
                    blob = await node.blobs().download(hash, opts, cb)
            elif b"inode_" in entry.key():
                # It's a document.
                # Sync the ticket
                print("Syncing document ticket")
                new_doc, event_queue = await join_and_watch_document(node, ticket_data)
                asyncio.create_task(watch_document(node, new_doc, event_queue, new_doc.id()))
            else:
                # This should never fire.
                print("Unknown ticket type")

        while True:
            update = await event_queue.get()
            print(update)
            if update.type() == iroh.WatchEventType.INSERT:
                print("TRIGGERED")
                content = await update.content_bytes(remote_tickets_doc)
                new_ticket = content.decode()
                new_doc, event_queue = await join_and_watch_document(node, new_ticket)
                if new_doc:
                    asyncio.create_task(watch_document(node, new_doc, event_queue, new_doc.id()))

async def join_and_watch_document(node, ticket):
    try:
        event_queue = queue.Queue()
        # Convert the ticket to a DocTicket
        ticket = iroh.DocTicket(ticket)
        doc = await node.docs().join(ticket)
        callback = DocWatch(event_queue)
        await doc.subscribe(callback)
        return doc, event_queue
    except Exception as e:
        print(f"Failed to join document: {e}")
        return None, None
# Classes

# GossipMessage
class GossipCallback(iroh.GossipMessageCallback):
    def __init__(self, name):
        # Initialisation
        if debug_mode:
            print("init", name)
        self.name = name
        self.chan = asyncio.Queue()

    async def on_message(self, msg):
        if debug_mode:
            print(self.name, msg.type())
        await self.chan.put(msg)

# Add callback for when we get a hash back from iroh
class AddCallback:
    hash = None
    format = None

    async def progress(x, progress_event):
        if debug_mode:
            print(progress_event.type())
        if progress_event.type() == iroh.AddProgressType.ALL_DONE:
            all_done_event = progress_event.as_all_done()
            x.hash = all_done_event.hash
            if debug_mode:
                print(all_done_event.hash)
                print(all_done_event.format)
            x.format = all_done_event.format
        if progress_event.type() == iroh.AddProgressType.ABORT:
            abort_event = progress_event.as_abort()
            raise Exception(abort_event.error)

class DocWatch:
    def __init__(self, queue):
        self.queue = queue

    async def event(self, e):
        t = e.type()
        if t == iroh.LiveEventType.INSERT_LOCAL:
            entry = e.as_insert_local()
            print(f"LiveEvent - InsertLocal: entry hash {entry.content_hash()}")
            self.queue.put(True)
        elif t == iroh.LiveEventType.INSERT_REMOTE:
            insert_remove_event = e.as_insert_remote()
            if debug_mode:
                print(f"LiveEvent - InsertRemote:\n\tentry hash:\n\t{insert_remove_event.entry.content_hash()}\n\tcontent_status: {insert_remove_event.content_status}")
                print("Insert Remove events will be eventually followed by the ContentReady event")
        elif t == iroh.LiveEventType.CONTENT_READY:
            hash_val = e.as_content_ready()
            if debug_mode:
                print(f"LiveEvent - ContentReady: hash {hash_val}")
        elif t == iroh.LiveEventType.NEIGHBOR_UP:
            node_id = e.as_neighbor_up()
            if debug_mode:
                print(f"LiveEvent - NeighborUp: node id {node_id}")
        elif t == iroh.LiveEventType.NEIGHBOR_DOWN:
            node_id = e.as_neighbor_down()
            if debug_mode:
                print(f"LiveEvent - NeighborDown: node id {node_id}")
        elif t == iroh.LiveEventType.SYNC_FINISHED:
            sync_event = e.as_sync_finished()
            if debug_mode:
                print(f"Live Event - SyncFinished: synced peer: {sync_event.peer}")
        elif t == iroh.LiveEventType.PENDING_CONTENT_READY:
            if debug_mode:
                print(f"Live Event - PendingContentReady")
        else:
            if debug_mode:
                print("Event type was: {}".format(t))

async def main():
    global node
    global author
    global debug_mode
    global inode_map_doc_id
    global gossip_topic
    global read_only_ticket
    # set initial var states
    debug_mode = False
    ticket = False

    # parse arguments
    parser = argparse.ArgumentParser(description='Recurso Demo')
    parser.add_argument('--ticket', type=str, help='ticket to join a root document')
    parser.add_argument('--debug', action='store_true', help='enable debug mode')

    args = parser.parse_args()

    if args.debug:
        debug_mode = True
    if args.ticket:
        ticket = args.ticket
        print("Loaded ticket")

    # Setup iroh node
    await setup_iroh_node(ticket, debug_mode)

    # create or find root document
    root_doc_id, root_directory_doc_id, inode_map_doc_id, ticket_doc_id = await create_root_document(ticket=ticket)

    # Load our root document
    root_doc = await node.docs().open(root_doc_id)
    # Create a ticket to join the root document. Use Relay instead of ID if needed.
    new_ticket = await root_doc.share(iroh.ShareMode.WRITE, iroh.AddrInfoOptions.RELAY_AND_ADDRESSES)
    # Load our tickets list
    tickets_doc = await node.docs().open(ticket_doc_id)
    # Create a read only ticket to join our tickets list
    read_only_ticket = await tickets_doc.share(iroh.ShareMode.READ, iroh.AddrInfoOptions.RELAY_AND_ADDRESSES)
    print("To join another node, use this ticket: {}".format(new_ticket))
    print("You can use this command: \n")
    print("python3 recurso.py --ticket {}".format(new_ticket) + "\n")

    # We'll create a hash of the root doc ID and use it as our gossip topic
    gossip_topic = blake3(bytes(root_doc_id, "utf-8")).digest()

    # In a background thread, use a gossip loop that listens for control messages
    asyncio.create_task(gossip_loop(ticket, gossip_topic))

    # Stay alive until we get a SIGINT
    try:
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        print("SIGINT or CTRL-C detected. Exiting...")
    finally:
        print("Exiting...")
    exit()


if __name__ == "__main__":
    asyncio.run(main())