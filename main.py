import iroh

import argparse
import asyncio

async def create_root_document(ticket=False):
    # Find or create a root document for Recurso to use.
    # If we've been given a ticket
    if ticket:
        doc = await node.doc_join(args.ticket)
        doc_id = doc.id()
        print("Joined doc: {}".format(doc_id))
        return doc.id()
    else:
        doc = await node.docs().create()
        doc_id = doc.id()
        print("Created initial root doc: {}".format(doc_id))
        return doc.id()

async def main():
    global node
    # setup event loop, to ensure async callbacks work
    iroh.iroh_ffi.uniffi_set_event_loop(asyncio.get_running_loop())

    # parse arguments
    parser = argparse.ArgumentParser(description='Recurso Demo')
    parser.add_argument('--ticket', type=str, help='ticket to join a root document')

    args = parser.parse_args()

    print("Starting Recurso Demo")

    # create iroh node
    node = await iroh.Iroh.memory()
    node_id = await node.net().node_id()
    print("Started Iroh node: {}".format(node_id))

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