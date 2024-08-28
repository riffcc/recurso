import iroh

import argparse
import asyncio
import time

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
        if entries["type"] == "root document":
            print("Root document found")
            # Check version is v0
            if entries["version"] == "v0":
                print("Root document is v0")
            else:
                print("Root document is not v0, bailing!")
                return
        # Check if the type is set to anything other than "root document", but exists:
        elif entries["type"] and entries["type"] != "root document":
            print("Found a document of type: {}".format(entries["type"]))
            print("Was expecting a root document. Bailing!")
            return
    else:
        print("No type set and no odd markers found. Continuing...")
        await create_new_root_document(doc_id)
    return doc

async def create_children_document():
    doc = await node.docs().create()
    children_doc_id = doc.id()
    print("Created children document: {}".format(children_doc_id))
    return children_doc_id

async def create_directory_document(doc_id):
    print("Creating directory document in: {}".format(doc_id))
    doc = await node.docs().open(doc_id)
    # Create the children document and fetch its ID
    children_doc_id = await create_children_document()
    # Create the directory document
    await doc.set_bytes(author, b"type", b"directory")
    await doc.set_bytes(author, b"version", b"v0")
    await doc.set_bytes(author, b"created", time.time())
    await doc.set_bytes(author, b"updated", time.time())
    await doc.set_bytes(author, b"children", children_doc_id)
    # Debug mode: print out the doc we just created
    if debug_mode:
        # Fetch all keys from the document
        query = iroh.Query.all(None)
        entries = await doc.get_many(query)
    return doc_id

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
    await scan_root_document(doc_id)
    return doc.id()

async def create_new_root_document(doc_id):
    print("Creating new root document in: {}".format(doc_id))
    doc = await node.docs().open(doc_id)
    await doc.set_bytes(author, b"type", b"root document")
    await doc.set_bytes(author, b"version", b"v0")
    await doc.set_bytes(author, b"created", bytes(str(time.time()), "utf-8"))
    await doc.set_bytes(author, b"updated", bytes(str(time.time()), "utf-8"))
    return doc_id

async def main():
    global node
    global author
    global debug_mode
    # setup event loop, to ensure async callbacks work
    iroh.iroh_ffi.uniffi_set_event_loop(asyncio.get_running_loop())

    # set initial var states
    debug_mode = False

    # parse arguments
    parser = argparse.ArgumentParser(description='Recurso Demo')
    parser.add_argument('--ticket', type=str, help='ticket to join a root document')
    parser.add_argument('--debug', action='store_true', help='enable debug mode')

    args = parser.parse_args()

    if args.debug:
        debug_mode = True

    print("Starting Recurso Demo")

    # create iroh node
    node = await iroh.Iroh.memory()
    node_id = await node.net().node_id()
    print("Started Iroh node: {}".format(node_id))

    # Get and set default author globally
    author = await node.authors().default()
    print(f"Default author {author}")

    # create or find root document
    await create_root_document()

    # list docs
    docs = await node.docs().list()
    print("List all {} docs:".format(len(docs)))
    for doc in docs:
        print("\t{}".format(doc))

    exit()


if __name__ == "__main__":
    asyncio.run(main())