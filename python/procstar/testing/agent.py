import asyncio
from   contextlib import asynccontextmanager, suppress
import logging
import os
from   pathlib import Path
import secrets
import signal
import socket
import tempfile
import uuid

from   . import get_procstar_path, TLS_CERT_PATH, TLS_KEY_PATH
from   procstar import proto
from   procstar.agent.proc import Result
import procstar.agent.server

logger = logging.getLogger(__name__)

DEFAULT = object()

#-------------------------------------------------------------------------------

def _get_local(ws_server):
    """
    Returns an iterable of local socket names bound by `ws_server`.
    """
    return (
        s.getsockname()[: 2]
        for s in ws_server.sockets
        if s.type == socket.SOCK_STREAM
        and s.family in (socket.AF_INET, socket.AF_INET6)
    )


class ProcstarError(RuntimeError):
    """
    A procstar process terminated unexpectedly.
    """


class Assembly:
    """
    Integration test assembly consisting of a websocket server and multiple
    procstar instances connecting to it.
    """

    def __init__(self, *, access_token=DEFAULT):
        """
        Does not start the websocket server or any procstar instances.
        """
        if access_token is DEFAULT:
            access_token = secrets.token_urlsafe(32)
        self.access_token = access_token

        # The procstar server.
        self.server = procstar.agent.server.Server()

        # The websocket server and the task running it.
        self.ws = None

        # The port is assigned automatically the first time the server starts.
        self.port = None

        # Async (OS) process objects for the procstar instance processes, keyed
        # by conn_id.
        self.conn_procs = {}

        # Temporary files.
        self.temp_dir = tempfile.TemporaryDirectory()


    async def start_server(self):
        """
        Starts the websocket server.

        :precondition:
          The server is not started.
        """
        assert self.ws is None
        # Create the websockets server, that runs our protocol server.  Choose a
        # new port the first time, then keep using the same port, so procstar
        # instances can reconnect.
        ws_server = await self.server.run(
            host        ="localhost",
            port        =self.port,
            tls_cert    =(TLS_CERT_PATH, TLS_KEY_PATH),
            access_token=self.access_token,
        )
        self.port = tuple(_get_local(ws_server))[0][1]
        logger.info(f"started on port {self.port}")
        # Start it up in a task.
        ws_task = asyncio.create_task(ws_server.serve_forever())

        self.ws = ws_server, ws_task


    async def stop_server(self):
        """
        Stops the websocket server.

        Idempotent.
        """
        if self.ws is None:
            # Not started.
            return

        ws_server, ws_task = self.ws
        self.ws = None

        ws_server.close()
        await ws_server.wait_closed()
        try:
            assert ws_task is not None
            await ws_task
        except asyncio.CancelledError:
            pass


    def _build(self, conn_id, group_id, access_token, args):
        """
        Returns argv and env to start a procstar process.
        """
        token = (
            self.access_token if access_token is DEFAULT
            else access_token
        )
        return (
            [
                get_procstar_path(),
                "--agent",
                "--agent-host", "localhost",
                "--agent-port", str(self.port),
                "--group-id", group_id,
                "--conn-id", conn_id,
                "--connect-count-max", "1",
                "--log-level", "trace",
                *args,
            ],
            {
                "RUST_BACKTRACE": "1",
                "PROCSTAR_AGENT_CERT": str(TLS_CERT_PATH),
                "PROCSTAR_AGENT_TOKEN": token,
            }
            | os.environ
        )


    async def start_procstars(self, counts, *, access_token=DEFAULT, args=[]):
        """
        Starts procstar instances and waits for them to connect.

        :param counts:
          Mapping from group ID to instance count.
        :param args:
          Additional command line args to pass to procstar.
        """
        conns = set(
            (g, str(uuid.uuid4()))
            for g, n in counts.items()
            for _ in range(n)
        )
        procs = set()

        with self.server.connections.subscription() as events:
            # Start the processes.
            for group_id, conn_id in conns:
                argv, env = self._build(conn_id, group_id, access_token, args)
                conn_dir = Path(self.temp_dir.name) / conn_id
                conn_dir.mkdir()
                proc = await asyncio.create_subprocess_exec(
                    *argv,
                    cwd=conn_dir,
                    env=env,
                )
                procs.add(proc)
                self.conn_procs[conn_id] = proc

            async def wait_for_connect(conns):
                """
                Waits for procstar processes to connect.
                """
                connected = set()
                async for _, conn in events:
                    if conn is not None:
                        logger.info(f"instance connected: {conn_id}")
                        connected.add(
                            (conn.info.conn.group_id, conn.info.conn.conn_id)
                        )
                        if len(connected) == len(conns):
                            assert connected == conns
                            return None

            # Create a task to await incoming connections from all procstar
            # processes, and one task awaiting each procstar processes.
            aws = [wait_for_connect(conns)] + [ p.wait() for p in procs ]
            tasks = [ asyncio.create_task(a) for a in aws ]
            try:
                # Wait for a task to complete.  We expect it to be the task
                # awaiting incoming connections.
                res = await next(iter(asyncio.as_completed(tasks)))
                if res is not None:
                    # A procstar process failed before connecting.
                    raise ProcstarError(
                        f"procstar process failed with {res} before connecting")
            finally:
                for task in tasks:
                    task.cancel()


    def start_procstar(self, *, group_id=proto.DEFAULT_GROUP, args=[]):
        """
        Starts a single procstar instance.
        """
        return self.start_procstars({group_id: 1}, args=args)


    async def stop_instance(self, conn_id):
        """
        Stops a procstar instance.
        """
        process = self.conn_procs.pop(conn_id)
        with suppress(ProcessLookupError):
            process.send_signal(signal.SIGKILL)
        await process.wait()


    def stop_instances(self):
        """
        Stops all procstar instances.
        """
        conn_ids = tuple(self.conn_procs.keys())
        return asyncio.gather(*(
            self.stop_instance(c)
            for c in conn_ids
        ))


    async def aclose(self):
        """
        Shuts everything down.
        """
        await self.stop_instances()
        await self.stop_server()
        self.temp_dir.cleanup()


    @classmethod
    @asynccontextmanager
    async def start(cls, *, counts={"default": 1}, access_token=DEFAULT):
        """
        Async context manager for a ready-to-go assembly.

        Yields an assembley with procstar instances and the websocket server
        already started.  Shuts them down on exit.
        """
        asm = cls(access_token=access_token)
        await asm.start_server()
        await asm.start_procstars(counts)
        try:
            yield asm
        finally:
            await asm.aclose()


    async def wait(self, proc) -> Result:
        """
        Waits until `proc` is no longer running, and returns its `Result`.

        Other updates are ignored.
        """
        async for update in proc.updates:
            if (
                    isinstance(update, Result)
                    and update.state != "running"
            ):
                return update
        else:
            assert False, "proc deleted while running"


    async def run(self, spec) -> Result:
        """
        Runs a proc from `spec`, waits until no longer running, and returns
        its `Result`.
        """
        proc_id = str(uuid.uuid4())
        proc, res = await self.server.start(proc_id, spec)
        return (
            (await self.wait(proc)) if res.state == "running"
            else res
        )



