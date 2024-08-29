import iroh
import argparse
import asyncio
import time
import uuid

# Utility functions
# These take docs, not doc IDs
async def get_all_keys(doc):
    query = iroh.Query.all(None)
    entries = await doc.get_many(query)
    return entries

async def get_all_keys_by_prefix(doc, prefix):
    query = iroh.Query.key_prefix(bytes(prefix, "utf-8"), None)
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
    doc = await node.docs().open(doc_id)
    # Lookup key
    key_entry = await doc.get_exact(author, bytes(str(keyname), "utf-8"), False)
    key_doc_id = await key_entry.content_bytes(doc)
    # Decode the key_doc_id from bytes to string
    key_doc_id = key_doc_id.decode("utf-8")
    # Return the directory document ID
    return key_doc_id

# Main functions
async def scan_root_document(doc_id):
    print("Scanning root document")
    doc = await node.docs().open(doc_id)
    print("Opened root doc for scanning: {}".format(doc_id))
    # Fetch all keys from the root document
    query = iroh.Query.all(None)
    entries = await doc.get_many(query)
    #print("Keys: {}".format(entries))
    # Check if we have a type set
    if "type" in entries:
        print("Type: {}".format(entries["type"]))
        # Check if the type is set to "root document"
        if entries["type"] == "root":
            print("Root document found")
            # Check version is v0
            if entries["version"] == "v0":
                print("Root document is v0")
                return "state", "ok"
            else:
                print("Root document is not v0, bailing!")
                return "state", "err_not_v0"
        # Check if the type is set to anything other than "root document", but exists:
        elif entries["type"] and entries["type"] != "root document":
            print("Found a document of type: {}".format(entries["type"]))
            print("Was expecting a root document. Bailing!")
            return "state", "err_not_root"
    else:
        print("No type set and no odd markers found. Creating as empty rootdoc...")
        return "state", "empty"

async def create_children_document(inode_map_doc_id):
    print("Creating children document")
    # Create the children document and fetch its ID
    doc = await node.docs().create()
    children_doc_id = doc.id()
    # DEBUG: Force upload a file
    add_outcome = await node.blobs().add_bytes(b"hello from recurso, reading real files")
    assert add_outcome.format == iroh.BlobFormat.RAW
    assert add_outcome.size == 38
    # Create the children document itself
    await doc.set_bytes(author, b"type", b"children")
    await doc.set_bytes(author, b"version", b"v0")
    await doc.set_bytes(author, b"created", bytes(str(time.time()), "utf-8"))
    await doc.set_bytes(author, b"updated", bytes(str(time.time()), "utf-8"))
    # await doc.set_bytes(author, b"fsdir-never", b"rheibcmkl4jn63iolncyffoxyhoe327unn5wndwvmvkb5dmnxsjq")
    # await doc.set_bytes(author, b"fsdir-gonna", b"rheibcmkl4jn63iolncyffoxyhoe327unn5wndwvmvkb5dmnxsjq")
    # await doc.set_bytes(author, b"fsdir-give", b"rheibcmkl4jn63iolncyffoxyhoe327unn5wndwvmvkb5dmnxsjq")
    # await doc.set_bytes(author, b"fsdir-you", b"rheibcmkl4jn63iolncyffoxyhoe327unn5wndwvmvkb5dmnxsjq")
    # await doc.set_bytes(author, b"fsdir-up", b"rheibcmkl4jn63iolncyffoxyhoe327unn5wndwvmvkb5dmnxsjq")
    # Upload a file
    file_doc_id = await create_file_document("hello-world", add_outcome.hash, inode_map_doc_id)
    await doc.set_bytes(author, b"fsfile-hello-world", bytes(str(file_doc_id), "utf-8"))
    print("Created children document: {}".format(children_doc_id))
    # Debug mode: print out the doc we just created
    if debug_mode:
        # Fetch all keys from the document
        await print_all_keys(doc)
    return children_doc_id

# Create a metadata document with the name of a file or directory as well as its DirectoryDoc or FileDoc ID
async def create_metadata_document(name, doc_id, inode_map_doc_id):
    print("Creating metadata document")
    # Create the metadata document and fetch its ID
    doc = await node.docs().create()
    metadata_doc_id = doc.id()
    # Create the metadata document itself
    await doc.set_bytes(author, b"type", b"metadata")
    await doc.set_bytes(author, b"name", bytes(str(name), "utf-8"))
    await doc.set_bytes(author, b"version", b"v0")
    await doc.set_bytes(author, b"created", bytes(str(time.time()), "utf-8"))
    await doc.set_bytes(author, b"updated", bytes(str(time.time()), "utf-8"))

    # Generate an initial inode
    # 1. Generate a UUID
    uuid_value = uuid.uuid4()    
    # 2. Convert UUID to a 64-bit integer
    st_ino = uuid_value.int & 0xFFFFFFFFFFFFFFFF

    # Initial metadata to populate the metadata document 
    metadata = {
        "st_mode": 0o040755,  # Directory with rwxr-xr-x permissions
        "st_ino": st_ino,   # Generated inode number (UUID-based)
        "st_uid": 0,   # Root user ID
        "st_gid": 0,   # Root group ID
        "st_size": 0,  # Initial size (empty directory)
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
    print("Pushing inode map item name {} for inode {}".format(name, st_ino))
    await inode_map_doc.set_bytes(author, bytes(str(st_ino), "utf-8"), bytes(str(doc_id), "utf-8"))

    # Grab all keys from the inode map document
    print("Printing all keys from the inode map document")
    keys = await print_all_keys(inode_map_doc)

    print("Created metadata document: {}".format(metadata_doc_id))
    # Debug mode: print out the doc we just created
    if debug_mode:
        # Fetch all keys from the document
        await print_all_keys(doc)
    return metadata_doc_id

async def create_directory_document(name, inode_map_doc_id):
    print("Creating directory document")
    doc = await node.docs().create()
    directory_doc_id = doc.id()
    # Create the children document and fetch its ID
    children_doc_id = await create_children_document(inode_map_doc_id)
    # Create the metadata document and fetch its ID
    metadata_doc_id = await create_metadata_document(name, directory_doc_id, inode_map_doc_id)
    # Create the directory document
    await doc.set_bytes(author, b"type", b"directory")
    await doc.set_bytes(author, b"version", b"v0")
    await doc.set_bytes(author, b"created", bytes(str(time.time()), "utf-8"))
    await doc.set_bytes(author, b"updated", bytes(str(time.time()), "utf-8"))
    await doc.set_bytes(author, b"metadata", bytes(metadata_doc_id, "utf-8"))
    await doc.set_bytes(author, b"children", bytes(children_doc_id, "utf-8"))
    print("Created directory document: {}".format(directory_doc_id))
    # Debug mode: print out the doc we just created
    if debug_mode:
        # Fetch all keys from the document
        await print_all_keys(doc)

    return directory_doc_id

async def create_file_document(name, blob_hash, inode_map_doc_id):
    print("Creating file document")
    doc = await node.docs().create()
    file_doc_id = doc.id()
    # Create the metadata document and fetch its ID
    metadata_doc_id = await create_metadata_document(name, file_doc_id, inode_map_doc_id)
    # Create the directory document
    await doc.set_bytes(author, b"type", b"file")
    await doc.set_bytes(author, b"version", b"v0")
    await doc.set_bytes(author, b"created", bytes(str(time.time()), "utf-8"))
    await doc.set_bytes(author, b"updated", bytes(str(time.time()), "utf-8"))
    await doc.set_bytes(author, b"metadata", bytes(metadata_doc_id, "utf-8"))
    await doc.set_bytes(author, b"blob", bytes(str(blob_hash), "utf-8"))
    print("Created file document: {}".format(file_doc_id))
    # Debug mode: print out the doc we just created
    if debug_mode:
        # Fetch all keys from the document
        await print_all_keys(doc)

    return file_doc_id

async def create_root_document(ticket=False):
    # Find or create a root document for Recurso to use.
    # If we've been given a ticket
    if ticket:
        doc = await node.doc_join(args.ticket)
        doc_id = doc.id()
        print("Joined doc: {}".format(doc_id))
        # TODO: Load in inode_map_doc_id
    else:
        doc = await node.docs().create()
        doc_id = doc.id()
        print("Created initial root doc: {}".format(doc_id))
    state, status = await scan_root_document(doc_id)
    if status == "ok":
        # Found a root document, return it
        return doc_id, inode_map_doc_id
    elif status == "empty":
        # No root document found, create a new one and fetch the result
        directory_doc_id, inode_map_doc_id = await create_new_root_document(doc_id)
        return doc_id, directory_doc_id, inode_map_doc_id
    elif status == "err_not_root":
        # Found a document of type other than "root document"
        print("Found a document of type other than 'root document'. Bailing!")
        return "state", "err_not_root"
    return doc_id, directory_doc_id, inode_map_doc_id

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

async def create_new_root_document(doc_id):
    print("Creating new root document in: {}".format(doc_id))
    doc = await node.docs().open(doc_id)

    # Create the inode map document and fetch its ID
    inode_map_doc_id = await create_inode_map_document()

    # Create the directory document and fetch its ID
    directory_doc_id = await create_directory_document("RECURSO_ROOT_DIRECTORY", inode_map_doc_id)

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
    metadata = await get_metadata_for_doc_id(directory_doc_id)
    # Set the inode number for the root directory to be equal to the inode number of the metadata document
    await inode_map_doc.set_bytes(author, b"1", bytes(str(metadata["st_ino"]), "utf-8"))
    # Set the real inode number to be equal to the document ID for the root directory's document
    await inode_map_doc.set_bytes(author, bytes(str(metadata["st_ino"]), "utf-8"), bytes(str(directory_doc_id), "utf-8"))

    # Check that we have a valid inode map document
    assert inode_map_doc_id

    return directory_doc_id, inode_map_doc_id

async def get_metadata_for_doc_id(doc_id):
    # Get inode and other directory info from a DirectoryDoc or FileDoc
    doc = await node.docs().open(doc_id)
    # Lookup metadata key
    print("Attempting to get metadata for " + doc_id)
    metadata_entry = await doc.get_exact(author, b"metadata", False)
    metadata_doc_id = await metadata_entry.content_bytes(doc)
    # Fetch metadata
    document_metadata = await get_metadata(metadata_doc_id.decode("utf-8"))
    # Debug mode: print out the metadata we just fetched
    if debug_mode:
        print(document_metadata)
    return document_metadata

async def get_document(doc_id):
    doc = await node.docs().open(doc_id)
    return doc

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
        "st_ctime": "UNLOADED"
    }

    # Populate the metadata dictionary with actual values
    for key, _ in metadata.items():
        entry = await metadata_doc.get_exact(author, key.encode(), False)
        if entry:
            value = await entry.content_bytes(metadata_doc)
            metadata[key] = int(value.decode())

    return metadata

async def setup_iroh_node(ticket=False, debug=False):
    global node
    global author
    global debug_mode
    # setup event loop, to ensure async callbacks work
    iroh.iroh_ffi.uniffi_set_event_loop(asyncio.get_running_loop())

    print("Starting Recurso Demo")

    # set debug mode based on debug flag
    debug_mode = debug

    # create iroh node
    node = await iroh.Iroh.memory()
    node_id = await node.net().node_id()
    print("Started Iroh node: {}".format(node_id))

    # Get and set default author globally
    author = await node.authors().default()
    print(f"Default author: {author}")

async def main():
    global node
    global author
    global debug_mode
    global inode_map_doc_id

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

    # Setup iroh node
    await setup_iroh_node(ticket, debug_mode)

    # create or find root document
    root_doc_id, root_directory_doc_id, inode_map_doc_id = await create_root_document()

    # list docs
    docs = await node.docs().list()
    print("List all {} docs:".format(len(docs)))
    for doc in docs:
        print("\t{}".format(doc))

    return 0
    exit()


if __name__ == "__main__":
    asyncio.run(main())