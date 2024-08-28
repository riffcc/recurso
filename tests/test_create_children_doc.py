# Test that the root document is created
import pytest
import asyncio
import recurso

@pytest.mark.asyncio
async def test_create_children_document():
    global author
    await recurso.setup_iroh_node()
    author = recurso.author

    children_doc_id = await recurso.create_children_document()
    children_document = await recurso.get_document(children_doc_id)

    # Basic smoke tests
    assert children_document is not None
    assert children_doc_id is not None
    
    # Grab all keys from the root document
    entries = await recurso.get_all_keys(children_document)
    # Assert that we got some keys
    assert entries is not None
    assert len(entries) > 0

    # Check type is == "children"
    type_entry = await children_document.get_exact(author, b"type", False)
    type_content = await type_entry.content_bytes(children_document)
    assert type_content == b"children"

    # Check version is == "v0"
    version_entry = await children_document.get_exact(author, b"version", False)
    version_content = await version_entry.content_bytes(children_document)
    assert version_content == b"v0"
