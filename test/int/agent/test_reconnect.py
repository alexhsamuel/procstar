import asyncio
from   contextlib import aclosing
import pytest

from   procstar import spec
from   procstar.testing.agent import Assembly, ProcstarError

SLEEP_EXE = "/usr/bin/sleep"

#-------------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_ws_reconnect():
    """
    Starts a couple of processes, drops the websocket connection, waits for
    reconnection, and confirms that process results are still accessible.
    """
    async with Assembly.start() as asm:
        proc0, _ = await asm.server.start(
            "reconnect0",
            spec.make_proc([SLEEP_EXE, "0.2"])
        )
        proc1, _ = await asm.server.start(
            "reconnect1",
            spec.make_proc([SLEEP_EXE, "0.4"])
        )

        with asm.server.connections.subscription() as sub:
            for _ in range(3):
                # Close the connection.
                conn, = asm.server.connections.values()
                await conn.ws.close()
                # Wait for reconnect.
                conn_id, conn = await anext(sub)

        # Results should be available.
        res0, res1 = await asyncio.gather(asm.wait(proc0), asm.wait(proc1))
        assert res0.status.exit_code == 0
        assert res1.status.exit_code == 0


@pytest.mark.asyncio
async def test_ws_reconnect_nowait():
    """
    Starts a couple of processes, drops the websocket connection, and
    confirms that process results are still accessible, without waiting
    explicitly for reconnect.
    """
    async with Assembly.start() as asm:
        proc0, _ = await asm.server.start(
            "reconnect0",
            spec.make_proc([SLEEP_EXE, "0.2"])
        )
        proc1, _ = await asm.server.start(
            "reconnect1",
            spec.make_proc([SLEEP_EXE, "0.4"])
        )

        # Close the connection.
        conn, = asm.server.connections.values()
        await conn.ws.close()

        # Wait for results anyway.  The procstar instance should reconnect.
        res0, res1 = await asyncio.gather(asm.wait(proc0), asm.wait(proc1))
        assert res0.status.exit_code == 0
        assert res1.status.exit_code == 0


@pytest.mark.asyncio
async def test_proc_reconnect():
    """
    Reconnects both the ws and `Process` instances, simulating restart.
    """
    async with Assembly.start() as asm:
        proc0, _ = await asm.server.start(
            "reconnect0",
            spec.make_proc([SLEEP_EXE, "0.2"])
        )
        proc1, _ = await asm.server.start(
            "reconnect1",
            spec.make_proc([SLEEP_EXE, "0.4"])
        )

        await asyncio.sleep(0.1)

        # Restart server.
        await asm.stop_server()
        await asm.start_server()

        res0, res1 = await asyncio.gather(asm.wait(proc0), asm.wait(proc1))
        assert res0.status.exit_code == 0
        assert res1.status.exit_code == 0


@pytest.mark.asyncio
async def test_proc_connect_timeout():
    """
    Tests timeout on connection attempts.
    """
    async with aclosing(Assembly()) as asm:
        # Start and then stop the server, so we have an unused port.
        await asm.start_server()
        await asm.stop_server()

        # Don't start the server, but start an instance.
        with pytest.raises(ProcstarError):
            await asm.start_procstar()

        # Start the server.
        await asm.start_server()
        # Now it should work.
        await asm.start_procstar()


if __name__ == "__main__":
    import logging
    logging.getLogger().setLevel(logging.INFO)
    asyncio.run(test_proc_reconnect())

