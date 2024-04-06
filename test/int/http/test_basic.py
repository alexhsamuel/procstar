from   contextlib import closing
import pytest
from   procstar.spec import make_proc
from   procstar.testing.inst import Instance

#-------------------------------------------------------------------------------

async def poll(async_client, proc_id):
    while (res := await async_client.get_proc(proc_id))["state"] == "running":
        assert res["state"] in {"running", "terminated", "error"}
    return res


def test_sync_instance():
    """
    Tests the test setup with a sync client.
    """
    with closing(Instance()) as inst:
        assert inst.client.get_procs() == {}


@pytest.mark.asyncio
async def test_instance_async():
    """
    Tests the test setup with an async client.
    """
    with closing(Instance()) as inst:
        async with inst.async_client() as client:
            assert await client.get_procs() == {}


@pytest.mark.asyncio
async def test_procs_async():
    with closing(Instance()) as inst:
        async with inst.async_client() as client:
            spec = make_proc(["/usr/bin/echo", "Hello, world!"])
            proc_id0 = await client.start_proc(spec)
            proc_id1 = await client.start_proc(spec, proc_id="proc1")
            assert proc_id1 == "proc1"

            res0 = await poll(client, proc_id0)
            assert res0["state"] == "terminated"
            assert res0["status"]["exit_code"] == 0
            assert res0["fds"]["stdout"]["text"] == "Hello, world!\n"
            assert res0["fds"]["stderr"]["text"] == ""
            await client.delete_proc(proc_id0)

            res1 = await poll(client, proc_id1)
            assert res1["state"] == "terminated"
            assert res1["status"]["exit_code"] == 0
            assert res1["fds"]["stdout"]["text"] == "Hello, world!\n"
            assert res1["fds"]["stderr"]["text"] == ""
            await client.delete_proc(proc_id1)


