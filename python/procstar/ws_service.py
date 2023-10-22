"""
WebSocket service for incoming connections from procstar instances.
"""

import asyncio
from   collections.abc import Mapping
from   contextlib import asynccontextmanager
from   dataclasses import dataclass
import ipaddress
import logging
import random
import websockets.server
from   websockets.exceptions import ConnectionClosedError

from   . import proto
from   .proto import ProtocolError, serialize_message, deserialize_message

# Timeout to receive an initial login message.
TIMEOUT_LOGIN = 60

# FIXME: What is the temporal scope of a connection?

logger = logging.getLogger(__name__)

#-------------------------------------------------------------------------------

class NoGroupError(LookupError):
    """
    No group with the given group name.
    """



class NoOpenConnectionInGroup(RuntimeError):
    """
    The group contains no open connections.
    """



class NoConnectionError(LookupError):
    """
    No connection with the given name.
    """


#-------------------------------------------------------------------------------

@dataclass
class ConnectionInfo:
    address: ipaddress._BaseAddress
    port: int

    def __str__(self):
        return f"{self.address}:{self.port}"



@dataclass
class Connection:
    """
    A connection to a single procstar instance.

    The connection object survives disconnection and reconnection from the
    procstar instance.  Thus, the websocket may be closed, and the remote
    procstar instance may no longer exist.  If the same procstar instance later
    reconnects, it uses the same `Connection` instance.
    """

    conn_id: str
    info: ConnectionInfo = None
    ws: asyncio.protocols.Protocol = None
    group: str = None

    def __hash__(self):
        return hash(self.conn_id)


    @property
    def open(self):
        return self.ws.open


    async def send(self, msg):
        data = serialize_message(msg)

        try:
            await self.ws.send(data)
        except ConnectionClosedError:
            # Connection closed; drop it.
            # FIXME: Don't forget the connection.
            logger.warning(f"{self.info}: connection closed")
            # FIXME: Mark it as closed?  Or is its internal closed flag enough?
            # FIXME: Think carefully the temporarily dropped connection logic.
            assert self.ws.closed



class Connections(Mapping):

    def __init__(self):
        self.__conns = {}
        self.__groups = {}
        self.__waiting = set()


    def _add(self, conn):
        """
        Adds a new connection.
        """
        conn_id = conn.conn_id
        assert conn_id not in self.__conns
        self.__conns[conn_id] = conn
        # Add it to the group.
        group = self.__groups.setdefault(conn.group, set())
        group.add(conn_id)
        self.__event(conn_id, conn)


    def _pop(self, conn_id) -> Connection:
        """
        Deletes and returns a connection.
        """
        conn = self.__conns.pop(conn_id)
        # Remove it from its group.
        group = self.__groups[conn.group]
        group.remove(conn_id)
        # If the group is now empty, clean it up.
        if len(group) == 0:
            del self.__groups[conn.group]
        self.__event(conn_id, None)
        return conn


    def __event(self, conn_id, conn):
        for queue in self.__waiting:
            queue.put_nowait((conn_id, conn))


    @asynccontextmanager
    async def watching(self):
        queue = asyncio.Queue()
        self.__waiting.add(queue)

        async def events():
            while True:
                yield await queue.get()

        try:
            yield events()
        finally:
            self.__waiting.remove(queue)


    # Mapping methods.

    def __contains__(self, conn_id):
        return self.__conns.__contains__(conn_id)


    def __getitem__(self, conn_id):
        return self.__conns.__getitem__(conn_id)


    def __len__(self):
        return self.__conns.__len__()


    def __iter__(self):
        return self.__conns.__iter__()


    def values(self):
        return self.__conns.values()


    def items(self):
        return self.__conns.items()


    # Group methods

    def choose_connection(self, group) -> Connection:
        """
        Chooses an open connection in 'group'.
        """
        try:
            conn_ids = self.__groups[group]
        except KeyError:
            raise NoGroupError(group) from None

        connections = [
            c
            for i in conn_ids
            if (c := self[i]).open
        ]
        if len(connections) == 0:
            raise NoOpenConnectionInGroup(group)

        # FIXME: Better choice mechanism.
        return random.choice(connections)



#-------------------------------------------------------------------------------

class Process:

    # FIXME: What happens when the connection is closed?

    def __init__(self, conn_id, proc_id):
        self.proc_id = proc_id
        self.conn_id = conn_id
        self.result = None
        # FIXME: Receive proc-specific errors.
        self.errors = []

        self.__waiting = set()


    def wait(self):
        """
        Returns a future that resolves to a new result, when available, or
        none if the processes is deleted.
        """
        fut = asyncio.get_event_loop().create_future()
        self.__waiting.add(fut)
        return fut


    def _awake(self, res):
        for fut in self.__waiting:
            fut.set_result(res)
        self.__waiting.clear()



class Server:

    class _Processes(Mapping):
        """
        Tracks processes.
        """

        def __init__(self):
            self.__procs = {}


        def create(self, conn_id, proc_id) -> Process:
            """
            Creates and returns a new process on `connection` with `proc_id`.

            `proc_id` must be unknown.
            """
            assert proc_id not in self.__procs
            self.__procs[proc_id] = proc = Process(conn_id, proc_id)
            return proc


        def on_message(self, conn_id, msg):
            """
            Enqueues `msg` received from `conn_id` to the corresponding
            process.
            """
            def get_proc(proc_id):
                try:
                    return self.__procs[proc_id]
                except KeyError:
                    logger.info(f"new proc on {conn_id}: {proc_id}")
                    return self.create(conn_id, proc_id)

            match msg:
                case proto.ProcidList(proc_ids):
                    logger.debug(f"msg proc_id list: {proc_ids}")
                    for proc_id in proc_ids:
                        _ = get_proc(proc_id)

                case proto.ProcResult(proc_id, res):
                    proc = get_proc(proc_id)
                    logger.debug(f"msg proc result: {proc_id}")
                    proc.result = res
                    proc._awake(res)

                case proto.ProcDelete(proc_id):
                    proc = get_proc(proc_id)
                    logger.debug(f"msg proc delete: {proc_id}")
                    del self.__procs[proc_id]
                    proc._awake(None)

                case proto.Register:
                    # We should receive this only immediately after connection.
                    logger.error(f"msg unexpected: {msg}")

                case proto.IncomingMessageError():
                    # FIXME: Implement.
                    # FIXME: Proc-specific errors.
                    raise NotImplementedError()


        # Mapping methods

        def __contains__(self, proc_id):
            return self.__procs.__contains__(proc_id)


        def __getitem__(self, proc_id):
            return self.__procs.__getitem__(proc_id)


        def __len__(self):
            return self.__procs.__len__()


        def __iter__(self):
            return self.__procs.__iter__()


        def values(self):
            return self.__procs.values()


        def items(self):
            return self.__procs.items()



    def __init__(self):
        # Track connections.
        # FIXME: Make Connection a nested class.
        self.connections = Connections()
        # Track processes.
        self.__processes = self._Processes()


    def run(self, loc=(None, None)):
        """
        Returns an async context manager that runs the websocket server.

        :param loc:
          `host, port` pair.  If `host` is none, runs on all interfaces.
          If `port` is none, chooses an unused port on each interface.
        """
        host, port = loc
        return websockets.server.serve(self._serve_connection, host, port)


    async def _serve_connection(self, ws):
        """
        Serves an incoming connection.

        Use this bound method with `websockets.server.serve()`.
        """
        assert ws.open

        # Collect remote loc.
        address, port, *_ = ws.remote_address
        address = ipaddress.ip_address(address)
        info = ConnectionInfo(address, port)

        try:
            # Wait for a Register message.
            try:
                msg = await asyncio.wait_for(ws.recv(), TIMEOUT_LOGIN)
            except TimeoutError:
                raise ProtocolError(f"no register in {TIMEOUT_LOGIN} s")
            except ConnectionClosedError:
                raise ProtocolError("closed before register")

            # Only Register is acceptable.
            type, msg = deserialize_message(msg)
            if type != "Register":
                raise ProtocolError(f"expected register; got {type}")

        except Exception as exc:
            logger.warning(f"{info}: {exc}")
            await ws.close()
            return

        conn_id = msg.conn_id

        # Do we recognize this connection?
        try:
            connection = self.connections[conn_id]

        except KeyError:
            # A new connection ID.
            logger.info(f"[{conn_id}] connecting from {info} group {msg.group}")
            connection = Connection(
                conn_id     =conn_id,
                info        =info,
                ws          =ws,
                group       =msg.group,
            )
            self.connections._add(connection)

        else:
            logger.info(f"[{conn_id}] reconnecting")

            # Previous connection with the same ID.  First, some sanity checks.
            if info.address != connection.info.address:
                # Allow the address to change, in case the remote reconnects
                # through a different interface.  The port may always be
                # different, of course.
                logger.warning(
                    f"[{conn_id}] new address: {connection.info.address}")
            if msg.group != connection.group:
                logger.error(f"[{conn_id}] new group: {msg.group}")
                ws.close()
                return

            # Is the old connection socket still (purportedly) open?
            if not connection.ws.closed:
                logger.warning(f"[{conn_id}] closing old connection")
                connection.ws.close()
                assert not connection.ws.open

            # Use the new socket with the old connection.
            connection.info = info
            connection.ws = ws
            connection.group = msg.group

        # Receive messages.
        while True:
            try:
                msg = await ws.recv()
            except ConnectionClosedError:
                logger.info(f"[{conn_id}] connection closed")
                break
            type, msg = deserialize_message(msg)
            # Process the message.
            self.__processes.on_message(conn_id, msg)

        await ws.close()
        assert ws.closed
        # Don't forget the connection; the other end may reconnect.


    async def start(
            self,
            proc_id,
            spec,
            *,
            group=proto.DEFAULT_GROUP,
    ) -> Process:
        """
        Starts a new process on a connection in `group`.

        :return:
          The connection on which the process starts.
        """
        conn = self.connections.choose_connection(group)
        # FIXME: If the connection is closed, choose another.
        await conn.send(proto.ProcStart(specs={proc_id: spec}))
        return self.__processes.create(conn.conn_id, proc_id)


    async def reconnect(self, conn_id, proc_id) -> Process:
        # FIXME
        raise NotImplementedError()


    async def delete(self, proc_id):
        """
        Deletes a process.
        """
        # FIXME: No proc?
        proc = self.__processes[proc_id]
        # FIXME: No connection?
        conn = self.connections[proc.conn_id]
        await conn.send(proto.ProcDeleteRequest(proc_id))



#-------------------------------------------------------------------------------

def main():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--host", metavar="ADDR", default=None,
        help="serve from interface bound to ADDR [def: all]")
    parser.add_argument(
        "--port", metavar="PORT", type=int, default=proto.DEFAULT_PORT,
        help=f"serve from PORT [def: {proto.DEFAULT_PORT}]")
    args = parser.parse_args()

    async def run(server, loc):
        async with server.run(loc):
            while True:
                async for conn_id, msg in server:
                    logger.info(f"[{conn_id}] received {msg}")


    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s [%(levelname)-7s] %(message)s",
    )
    logging.getLogger("websockets.server").setLevel(logging.INFO)
    try:
        asyncio.run(run(Server(), loc=(args.host, args.port)))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()

