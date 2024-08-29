# Test that the root document is created
import pytest
import asyncio
import recurso

@pytest.mark.asyncio
async def test_create_root_document():
    global author
    await recurso.setup_iroh_node()
    author = recurso.author

    root_doc_id, root_directory_doc_id, inode_map_doc_id = await recurso.create_root_document()
    root_document = await recurso.get_document(root_doc_id)

    # Basic smoke tests
    assert root_document is not None
    assert root_doc_id is not None

    # Grab all keys from the root document
    entries = await recurso.get_all_keys(root_document)
    # Assert that we got some keys
    assert entries is not None
    assert len(entries) > 0

    # Check type is == "root"
    type_entry = await root_document.get_exact(author, b"type", False)
    type_content = await type_entry.content_bytes(root_document)
    assert type_content == b"root"

    # Check version is == "v0"
    version_entry = await root_document.get_exact(author, b"version", False)
    version_content = await version_entry.content_bytes(root_document)
    assert version_content == b"v0"

    # Check directory ID is == some string
    directory_entry = await root_document.get_exact(author, b"directory", False)
    directory_content = await directory_entry.content_bytes(root_document)
    assert isinstance(directory_content.decode('utf-8'), str)
    assert len(directory_content.decode('utf-8')) > 0

async def test_get_root_document_directory_metadata():
    global author
    await recurso.setup_iroh_node()
    author = recurso.author

    root_doc_id, root_directory_doc_id, inode_map_doc_id = await recurso.create_root_document()
    root_document = await recurso.get_document(root_doc_id)

    root_directory_document = await recurso.get_document(root_doc_id)

    await recurso.get_metadata_for_doc_id(root_directory_doc_id)