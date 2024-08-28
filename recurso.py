import iroh
import argparse
import asyncio
import time

# Utility functions
async def get_all_keys(doc):
    query = iroh.Query.all(None)
    entries = await doc.get_many(query)
    return entries

async def print_all_keys(doc):
    entries = await get_all_keys(doc)
    for entry in entries:
        key = entry.key()
        hash = entry.content_hash()
        content = await entry.content_bytes(doc)
        print("{} : {} (hash: {})".format(key, content.decode("utf8"), hash))

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

async def create_children_document():
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

async def create_directory_document():
    print("Creating directory document")
    doc = await node.docs().create()
    directory_doc_id = doc.id()
    # Create the children document and fetch its ID
    children_doc_id = await create_children_document()
    # Create the directory document
    await doc.set_bytes(author, b"type", b"directory")
    await doc.set_bytes(author, b"version", b"v0")
    await doc.set_bytes(author, b"created", bytes(str(time.time()), "utf-8"))
    await doc.set_bytes(author, b"updated", bytes(str(time.time()), "utf-8"))
    await doc.set_bytes(author, b"children", bytes(children_doc_id, "utf-8"))
    print("Created directory document: {}".format(directory_doc_id))
    # Debug mode: print out the doc we just created
    if debug_mode:
        # Fetch all keys from the document
        await print_all_keys(doc)

    return directory_doc_id

async def create_root_document(ticket=False):
    # Find or create a root document for Recurso to use.
    # If we've been given a ticket
    if ticket:
        doc = await node.doc_join(args.ticket)
        doc_id = doc.id()
        print("Joined doc: {}".format(doc_id))
    else:
        doc = await node.docs().create()
        doc_id = doc.id()
        print("Created initial root doc: {}".format(doc_id))
    state, status = await scan_root_document(doc_id)
    if status == "ok":
        # Found a root document, return it
        return doc_id
    elif status == "empty":
        # No root document found, create a new one
        return await create_new_root_document(doc_id)
        return doc_id
    elif status == "err_not_root":
        # Found a document of type other than "root document"
        print("Found a document of type other than 'root document'. Bailing!")
        return "state", "err_not_root"
    return doc_id

async def create_new_root_document(doc_id):
    print("Creating new root document in: {}".format(doc_id))
    doc = await node.docs().open(doc_id)
    # Create the directory document and fetch its ID
    directory_doc_id = await create_directory_document()

    # Create the root document
    await doc.set_bytes(author, b"type", b"root")
    await doc.set_bytes(author, b"version", b"v0")
    await doc.set_bytes(author, b"created", bytes(str(time.time()), "utf-8"))
    await doc.set_bytes(author, b"updated", bytes(str(time.time()), "utf-8"))
    await doc.set_bytes(author, b"directory", bytes(directory_doc_id, "utf-8"))
    return doc_id

async def get_document(doc_id):
    doc = await node.docs().open(doc_id)
    return doc

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
    await setup_iroh_node(ticket, )

    # create or find root document
    await create_root_document()

    # list docs
    docs = await node.docs().list()
    print("List all {} docs:".format(len(docs)))
    for doc in docs:
        print("\t{}".format(doc))

    return 0
    exit()


if __name__ == "__main__":
    asyncio.run(main())