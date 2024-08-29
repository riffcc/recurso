# Setup
import os
import sys

# Add the project root directory to the Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, project_root)

# Test that the root document is created
import pytest
import asyncio
import recurso

@pytest.mark.asyncio
async def test_create_directory_document():
    global author
    await recurso.setup_iroh_node()
    author = recurso.author

    root_doc_id, inode_map_doc_id = await recurso.create_root_document()
    root_document = await recurso.get_document(root_doc_id)

    directory_doc_id = await recurso.create_directory_document("test", inode_map_doc_id)
    directory_document = await recurso.get_document(directory_doc_id)

    # Basic smoke tests
    assert directory_document is not None
    assert directory_doc_id is not None

    # Grab all keys from the root document
    entries = await recurso.get_all_keys(directory_document)
    # Assert that we got some keys
    assert entries is not None
    assert len(entries) > 0

    # Check type is == "directory"
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

    # Create some children entries
    children_doc_id = children_content.decode('utf-8')
    children_document = await recurso.get_document(children_doc_id)

    # Print children_doc_id
    print("Children doc ID: {}".format(children_doc_id))
    await children_document.set_bytes(author, b"fsdir-never", b"rheibcmkl4jn63iolncyffoxyhoe327unn5wndwvmvkb5dmnxsjq")
    await children_document.set_bytes(author, b"fsdir-gonna", b"rheibcmkl4jn63iolncyffoxyhoe327unn5wndwvmvkb5dmnxsjq")
    await children_document.set_bytes(author, b"fsdir-give", b"rheibcmkl4jn63iolncyffoxyhoe327unn5wndwvmvkb5dmnxsjq")
    await children_document.set_bytes(author, b"fsdir-you", b"rheibcmkl4jn63iolncyffoxyhoe327unn5wndwvmvkb5dmnxsjq")
    await children_document.set_bytes(author, b"fsdir-up", b"rheibcmkl4jn63iolncyffoxyhoe327unn5wndwvmvkb5dmnxsjq")

    # Lookup the entries and assert that 5 are found
    entries = await recurso.get_all_keys_by_prefix(children_document, "fsdir")

asyncio.run(test_create_directory_document())