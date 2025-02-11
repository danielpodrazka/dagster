import os
import signal
import subprocess
import sys
import threading
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from io import TextIOWrapper
from subprocess import Popen
from time import sleep
from typing import Any, NamedTuple, Optional

import dagster._check as check
from dagster._core.errors import DagsterError
from dagster._serdes.serdes import deserialize_value, serialize_value, whitelist_for_serdes
from dagster._utils.error import (
    ExceptionInfo,
    SerializableErrorInfo,
    serializable_error_info_from_exc_info,
)


def write_unary_input(input_file: str, obj: NamedTuple) -> None:
    check.str_param(input_file, "input_file")
    check.not_none_param(obj, "obj")
    with open(os.path.abspath(input_file), "w", encoding="utf8") as fp:
        fp.write(serialize_value(obj))


def read_unary_input(input_file: str) -> tuple[object, ...]:
    check.str_param(input_file, "input_file")
    with open(os.path.abspath(input_file), encoding="utf8") as fp:
        return deserialize_value(fp.read(), NamedTuple)


def ipc_write_unary_response(output_file: str, obj: NamedTuple) -> None:
    check.not_none_param(obj, "obj")
    with ipc_write_stream(output_file) as stream:
        stream.send(obj)


def read_unary_response(
    output_file: str, timeout: int = 30, ipc_process: "Optional[Popen[Any]]" = None
) -> Optional[NamedTuple]:
    messages = list(ipc_read_event_stream(output_file, timeout=timeout, ipc_process=ipc_process))
    check.invariant(len(messages) == 1)
    return messages[0]


@whitelist_for_serdes
class IPCStartMessage(NamedTuple("_IPCStartMessage", [])):
    def __new__(cls):
        return super().__new__(cls)


@whitelist_for_serdes
class IPCErrorMessage(
    NamedTuple(
        "_IPCErrorMessage",
        [("serializable_error_info", SerializableErrorInfo), ("message", Optional[str])],
    )
):
    """This represents a user error encountered during the IPC call. This indicates a business
    logic error, rather than a protocol. Consider this a "task failed successfully"
    use case.
    """

    def __new__(cls, serializable_error_info: SerializableErrorInfo, message: Optional[str]):
        return super().__new__(
            cls,
            serializable_error_info=check.inst_param(
                serializable_error_info, "serializable_error_info", SerializableErrorInfo
            ),
            message=check.opt_str_param(message, "message"),
        )


@whitelist_for_serdes
class IPCEndMessage(NamedTuple("_IPCEndMessage", [])):
    def __new__(cls):
        return super().__new__(cls)


class DagsterIPCProtocolError(DagsterError):
    """This indicates that something went wrong with the protocol. E.g. the
    process being called did not emit an IPCStartMessage first.
    """

    def __init__(self, message: str):
        self.message = message
        super().__init__(message)


class FileBasedWriteStream:
    def __init__(self, file_path: str):
        check.str_param("file_path", file_path)
        self._file_path = file_path

    def send(self, dagster_named_tuple: NamedTuple) -> None:
        _send(self._file_path, dagster_named_tuple)

    def send_error(self, exc_info: ExceptionInfo, message: Optional[str] = None) -> None:
        _send_error(self._file_path, exc_info, message=message)


def _send(file_path: str, obj: NamedTuple) -> None:
    with open(os.path.abspath(file_path), "a+", encoding="utf8") as fp:
        fp.write(serialize_value(obj) + "\n")


def _send_error(file_path: str, exc_info: ExceptionInfo, message: Optional[str]) -> None:
    return _send(
        file_path,
        IPCErrorMessage(
            serializable_error_info=serializable_error_info_from_exc_info(exc_info), message=message
        ),
    )


@contextmanager
def ipc_write_stream(file_path: str) -> Iterator[FileBasedWriteStream]:
    check.str_param("file_path", file_path)
    _send(file_path, IPCStartMessage())
    try:
        yield FileBasedWriteStream(file_path)
    except Exception:
        _send_error(file_path, sys.exc_info(), message=None)
    finally:
        _send(file_path, IPCEndMessage())


def _process_line(file_pointer: TextIOWrapper, sleep_interval: float = 0.1) -> Optional[NamedTuple]:
    while True:
        line = file_pointer.readline()
        if line:
            return deserialize_value(line.rstrip(), NamedTuple)
        sleep(sleep_interval)


def _poll_process(ipc_process: "Optional[Popen[Any]]") -> None:
    if not ipc_process:
        return
    if ipc_process.poll() is not None:
        raise DagsterIPCProtocolError(
            f"Process exited with return code {ipc_process.returncode} while waiting for events"
        )


def ipc_read_event_stream(
    file_path: str, timeout: int = 30, ipc_process: "Optional[Popen[Any]]" = None
) -> Iterator[Optional[NamedTuple]]:
    # Wait for file to be ready
    sleep_interval = 0.1
    elapsed_time = 0
    while elapsed_time < timeout and not os.path.exists(file_path):
        _poll_process(ipc_process)
        elapsed_time += sleep_interval
        sleep(sleep_interval)

    if not os.path.exists(file_path):
        raise DagsterIPCProtocolError(
            f"Timeout: read stream has not received any data in {timeout} seconds"
        )

    with open(os.path.abspath(file_path), encoding="utf8") as file_pointer:
        message = _process_line(file_pointer)
        while elapsed_time < timeout and message is None:
            _poll_process(ipc_process)
            elapsed_time += sleep_interval
            sleep(sleep_interval)
            message = _process_line(file_pointer)

        # Process start message
        if not isinstance(message, IPCStartMessage):
            raise DagsterIPCProtocolError(
                f"Attempted to read stream at file {file_path}, but first message was not an "
                "IPCStartMessage"
            )

        message = _process_line(file_pointer)
        while not isinstance(message, IPCEndMessage):
            if message is None:
                _poll_process(ipc_process)
            yield message
            message = _process_line(file_pointer)


# Windows subprocess termination utilities. See here for why we send CTRL_BREAK_EVENT on Windows:
# https://stefan.sofa-rockers.org/2013/08/15/handling-sub-process-hierarchies-python-linux-os-x/


def open_ipc_subprocess(parts: Sequence[str], **kwargs: Any) -> "Popen[Any]":
    """Sets the correct flags to support graceful termination."""
    check.list_param(parts, "parts", str)

    creationflags = 0
    if sys.platform == "win32":
        creationflags = subprocess.CREATE_NEW_PROCESS_GROUP

        # pass_fds is not supported on Windows
        if kwargs.get("pass_fds"):
            del kwargs["pass_fds"]
            kwargs["close_fds"] = False

    return subprocess.Popen(
        parts,
        creationflags=creationflags,
        **kwargs,
    )


def interrupt_ipc_subprocess(proc: "Popen[Any]") -> None:
    """Send CTRL_BREAK on Windows, SIGINT on other platforms."""
    if sys.platform == "win32":
        proc.send_signal(signal.CTRL_BREAK_EVENT)
    else:
        proc.send_signal(signal.SIGINT)


def interrupt_then_kill_ipc_subprocess(proc: "Popen[Any]", wait_time: int = 10) -> None:
    interrupt_ipc_subprocess(proc)
    try:
        proc.wait(timeout=wait_time)
    except subprocess.TimeoutExpired:
        proc.kill()


def interrupt_ipc_subprocess_pid(pid: int) -> None:
    """Send CTRL_BREAK_EVENT on Windows, SIGINT on other platforms."""
    check.int_param(pid, "pid")

    if sys.platform == "win32":
        os.kill(pid, signal.CTRL_BREAK_EVENT)
    else:
        os.kill(pid, signal.SIGINT)


# ########################
# ##### SHUTDOWN PIPE
# ########################

_PIPE_SHUTDOWN_INDICATOR = "SHUTDOWN"


def get_ipc_shutdown_pipe() -> tuple[int, int]:
    r_fd, w_fd = os.pipe()
    if sys.platform == "win32":
        import msvcrt

        os.set_inheritable(r_fd, True)
        r_fd = msvcrt.get_osfhandle(r_fd)  # Convert to Windows handle

        # import ctypes
        # import ctypes.wintypes
        # kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        # kernel32.SetHandleInformation(
        #     ctypes.wintypes.HANDLE(r_fd),
        #     1,  # HANDLE_FLAG_INHERIT
        #     1,  # HANDLE_FLAG_INHERIT
        # )
        # w_fd = msvcrt.get_osfhandle(w_fd)
    return r_fd, w_fd

    # If you rely on handle inheritance, make sure the handle is marked inheritable
    # (depending on your Python version & Windows version).
    # For example:
    # import ctypes


@contextmanager
def monitor_ipc_shutdown_pipe(pipe_fd: int, stop_event: threading.Event) -> Iterator[None]:
    """Context manager that opens a named pipe at `pipe_path` and waits for `shutdown_indicator`.
    If `shutdown_indicator` is received, it sends SIGINT to the process, causing a
    KeyboardInterrupt in the main thread.

    :param pipe_path: Path to a named pipe (FIFO).
    """
    if sys.platform == "win32":
        import msvcrt

        pipe_fd = msvcrt.open_osfhandle(pipe_fd, os.O_RDONLY)

    def _watch_pipe():
        # Open the named pipe in read mode
        with open(pipe_fd) as pipe:
            while not stop_event.is_set():
                print("ATTEMPTING TO READ...")
                line = pipe.readline()
                print("READ LINE", line)
                if not line:
                    # EOF or pipe closed
                    break
                if _PIPE_SHUTDOWN_INDICATOR in line.strip():
                    # Emulate a Ctrl+C in the main thread
                    os.kill(os.getpid(), signal.SIGINT)
                    break

    # Start a background thread that watches the pipe
    monitor_thread = threading.Thread(target=_watch_pipe, daemon=True)
    monitor_thread.start()

    try:
        yield  # Code inside the 'with' block executes here
    finally:
        # Signal the thread to exit and wait for it to stop
        stop_event.set()
        monitor_thread.join()


def send_ipc_shutdown_signal(w_fd: int) -> None:
    os.write(w_fd, f"{_PIPE_SHUTDOWN_INDICATOR}\n".encode())
    os.close(w_fd)
