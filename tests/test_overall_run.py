# Test node main functions
import pytest
import asyncio
import recurso

# Test that Recurso runs without errors
@pytest.mark.asyncio
async def test_main_recurso():
    return_code = await recurso.main()
    assert return_code == 0
