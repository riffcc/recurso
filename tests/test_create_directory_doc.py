# Test that the root document is created
import pytest
import asyncio
import recurso

@pytest.mark.asyncio
async def test_create_directory_document():
    global author
    await recurso.setup_iroh_node()
    author = recurso.author

    directory_doc_id = await recurso.create_directory_document()
    directory_document = await recurso.get_document(directory_doc_id)

    # Basic smoke tests
    assert directory_document is not None
    assert directory_doc_id is not None

    # Grab all keys from the root document
    entries = await recurso.get_all_keys(directory_document)
    # Assert that we got some keys
    assert entries is not None
    assert len(entries) > 0

    # Check type is == "document"
    type_entry = await directory_document.get_exact(author, b"type", False)
    type_content = await type_entry.content_bytes(directory_document)
    assert type_content == b"directory"

    # Check version is == "v0"
    version_entry = await directory_document.get_exact(author, b"version", False)
    version_content = await version_entry.content_bytes(directory_document)
    assert version_content == b"v0"

    # Check metadata ID is == some string
    metadata_entry = await directory_document.get_exact(author, b"metadata", False)
    metadata_content = await metadata_entry.content_bytes(directory_document)
    assert isinstance(metadata_content.decode('utf-8'), str)
    assert len(metadata_content.decode('utf-8')) > 0
    
    # Check children ID is == some string
    children_entry = await directory_document.get_exact(author, b"children", False)
    children_content = await children_entry.content_bytes(directory_document)
    assert isinstance(children_content.decode('utf-8'), str)
    assert len(children_content.decode('utf-8')) > 0

async def test_create_directory_document_metadata():
    global author
    await recurso.setup_iroh_node()
    author = recurso.author

    directory_doc_id = await recurso.create_directory_document()
    directory_document = await recurso.get_document(directory_doc_id)

    await recurso.get_directory_info(directory_doc_id)