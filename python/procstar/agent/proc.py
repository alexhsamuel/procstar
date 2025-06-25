"""
Processes on connected procstar instances.
"""

import asyncio
from   collections.abc import Mapping
from   dataclasses import dataclass, field
from   functools import cached_property
import logging

from   .exc import ProcessUnknownError
from   procstar import proto
from   procstar.lib.asyn import iter_queue
from   procstar.lib.py import Interval
import procstar.lib.json

logger = logging.getLogger(__name__)

#-------------------------------------------------------------------------------

class ProcessDeletedError(RuntimeError):
    """
    The process was deleted.
    """

    def __init__(self, proc_id):
        super().__init__(f"process deleted: {proc_id}")
        self.proc_id = proc_id



class ConnectionTimeoutError(RuntimeError):
    """
    The connection on which the process was running timed out.
    """

    def __init__(self, conn_id):
        super().__init__(f"agent reconnect timeout: conn_id {conn_id}")
        self.conn_id = conn_id



class AgentMessageError(RuntimeError):
    """
    The agent responded to a request message with an error.
    """

    def __init__(self, msg, err):
        super().__init__(f"agent error response to message: {err}")
        self.msg = msg
        self.err = err



# Derive from Jso to convert dict into object semantics.
class Result(procstar.lib.json.Jso):
    """
    The proc res dictionary produced by the agent.
    """



@dataclass
class FdData:
    """
    The fd name.
    """
    fd: str

    """
    The interval of output bytes contained in this update.
    """
    interval: Interval = field(default_factory=lambda: Interval(0, 0))

    """
    The output encoding.
    """
    encoding: str | None = None

    """
    The output data.
    """
    data: bytes = b""



#-------------------------------------------------------------------------------

class Process:
    """
    A process running under a connected procstar instance.
    """

    proc_id: str
    conn_id: str

    # FIXME
    errors: list[str]

    # FIXME: What happens when the connection is closed?

    def __init__(self, conn, proc_id):
        self.__conn = conn
        self.proc_id = proc_id
        # FIXME: Receive proc-specific errors.
        self.errors = []
        self.__msgs = asyncio.Queue()


    @property
    def conn_id(self):
        return self.__conn.conn_id


    def _on_message(self, msg):
        self.__msgs.put_nowait(msg)


    @cached_property
    async def updates(self):
        """
        A singleton async iterator over updates for this process.

        The iterator may:
        - yield a `Result` instance
        - yield a `FdData` instance
        - terminate if the process is deleted
        - raise `ProcessUnknownError` if the proc ID is unknown
        - raise `ConnectionTimeoutError` if the connection timed out

        """
        async for msg in iter_queue(self.__msgs):
            match msg:
                case proto.ProcResult(_, result):
                    yield Result(result)

                case proto.ProcFdData(_, fd, start, stop, encoding, data):
                    # FIXME: Process data.
                    yield FdData(
                        fd      =fd,
                        interval=Interval(start, stop),
                        encoding=encoding,
                        data    =data,
                    )

                case proto.ProcDelete(_):
                    # Done.
                    break

                case proto.ProcUnknown(_):
                    raise ProcessUnknownError(self.proc_id)

                case proto.RequestError(msg, err):
                    logger.error(f"agent error: {err}")
                    raise AgentMessageError(msg, err)

                case proto.ConnectionTimeout():
                    logger.warning(f"proc {self.proc_id}: agent connection timeout: {self.conn_id}")
                    raise ConnectionTimeoutError(self.conn_id)

                case _:
                    assert False, f"unexpected msg: {msg!r}"


    def request_result(self):
        """
        Returns a coro that sends a request for updated result.
        """
        return self.__conn.try_send(proto.ProcResultRequest(self.proc_id))


    def request_fd_data(self, fd, *, interval=Interval(0, None)):
        """
        Returns a coro that requests updated output data,
        """
        return self.__conn.try_send(proto.ProcFdDataRequest(
            proc_id =self.proc_id,
            fd      =fd,
            start   =interval.start,
            stop    =interval.stop,
        ))


    def send_signal(self, signum):
        """
        Returns a coro that sends a signal to the proc.
        """
        return self.__conn.send(proto.ProcSignalRequest(self.proc_id, signum))


    def request_delete(self):
        """
        Returns a coro that requests deletion of the proc.
        """
        return self.__conn.send(proto.ProcDeleteRequest(self.proc_id))


    async def delete(self):
        """
        Requests deletion of the proc and awaits confirmation.
        """
        await self.request_delete()
        # The update iterator exhausts when the proc is deleted.
        async for update in self.updates:
            pass



#-------------------------------------------------------------------------------

class Processes(Mapping):
    """
    Processes running under connected procstar instances.

    Maps proc ID to `Process` instances.
    """

    def __init__(self):
        self.__procs = {}


    def create(self, conn, proc_id) -> Process:
        """
        Creates and returns a new process on `connection` with `proc_id`.

        `proc_id` must be unknown.
        """
        assert proc_id not in self.__procs
        self.__procs[proc_id] = proc = Process(conn, proc_id)
        return proc


    def on_message(self, conn, msg):
        """
        Processes `msg` to the corresponding process.

        :param conn:
          The connection from which the message was received.
        """
        conn_id = conn.conn_id

        def get_proc(proc_id):
            """
            Looks up or creates, if necessary, the `Process` object.
            """
            try:
                return self.__procs[proc_id]
            except KeyError:
                logger.info(f"new proc on {conn_id}: {proc_id}")
                return self.create(conn, proc_id)

        def send_by_conn():
            """
            Dispatches the current msg to all processes on this connection.
            """
            for proc in self.__procs.values():
                if proc.conn_id == conn_id:
                    proc._on_message(msg)

        match msg:
            case proto.ProcidList(proc_ids):
                # Make sure we track a proc for each proc ID the instance knows.
                for proc_id in proc_ids:
                    _ = get_proc(proc_id)

            case proto.ProcResult(proc_id):
                # Attach Procstar server and connection info to the result.
                msg.res["procstar"] = conn.info
                get_proc(proc_id)._on_message(msg)

            case proto.ProcFdData(proc_id):
                get_proc(proc_id)._on_message(msg)

            case proto.ProcDelete(proc_id) | proto.ProcUnknown(proc_id):
                self.__procs.pop(proc_id)._on_message(msg)

            case proto.Register:
                # We should receive this only immediately after connection.
                logger.error(f"msg unexpected: {msg}")

            case proto.RequestError():
                try:
                    proc_id = msg.msg["proc_id"]
                except KeyError:
                    # An error in response to ProcStartRequest may pertain to multiple proc IDs.
                    if msg.msg["type"] == "ProcStartRequest":
                        for proc_id in msg.msg["specs"].keys():
                            get_proc(proc_id)._on_message(msg)
                    else:
                        # No associated proc ID, or we can't figure it out.
                        logger.error(f"agent error: {msg.err}: {msg.msg}")
                else:
                    # Forward to the proc the original message was related to.
                    get_proc(proc_id)._on_message(msg)

            case proto.ConnectionTimeout():
                logger.warning(f"agent connection timeout: {conn_id}")
                send_by_conn()

            case proto.ShutDown(shutdown_state):
                logger.info(f"agent shut down: {conn_id}: {shutdown_state}")

            case _:
                logger.error(f"unknown msg: {msg}")



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



