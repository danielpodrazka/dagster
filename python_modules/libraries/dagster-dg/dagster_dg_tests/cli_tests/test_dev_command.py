import contextlib
import platform
import socket
import subprocess
import time
from collections.abc import Iterator

import psutil
import requests
from dagster_dg.utils import ensure_dagster_dg_tests_import, interrupt_subprocess, open_subprocess
from dagster_graphql.client import DagsterGraphQLClient

ensure_dagster_dg_tests_import()
from dagster_dg_tests.utils import (
    ProxyRunner,
    isolated_example_code_location_foo_bar,
    isolated_example_deployment_foo,
)


def test_dev_command_deployment_context_success():
    with ProxyRunner.test() as runner, isolated_example_deployment_foo(runner):
        runner.invoke("code-location", "scaffold", "code-location-1")
        runner.invoke("code-location", "scaffold", "code-location-2")

        port = _find_free_port()
        code_locations = {"code-location-1", "code-location-2"}
        with _launch_dev_command(["--port", str(port)]):
            _wait_for_webserver_running(port)
            assert _query_code_locations(port) == code_locations


def test_dev_command_code_location_context_success():
    with ProxyRunner.test() as runner, isolated_example_code_location_foo_bar(runner):
        port = _find_free_port()
        code_locations = {"foo-bar"}
        with _launch_dev_command(["--port", str(port)]):
            _wait_for_webserver_running(port)
            assert _query_code_locations(port) == code_locations


def test_dev_command_outside_project_context_fails():
    with ProxyRunner.test() as runner, runner.isolated_filesystem():
        port = _find_free_port()
        with _launch_dev_command(["--port", str(port)], capture_output=True) as dev_process:
            assert dev_process.wait() != 0
            assert dev_process.stdout
            assert (
                "This command must be run inside a code location or deployment directory."
                in dev_process.stdout.read().decode()
            )


def test_dev_command_has_options_of_dagster_dev():
    from dagster._cli.dev import dev_command as dagster_dev_command
    from dagster_dg.cli import dev_command as dev_command

    exclude_dagster_dev_params = {
        # Exclude options that are used to set the target. `dg dev` does not use.
        "empty_workspace",
        "workspace",
        "python_file",
        "module_name",
        "package_name",
        "attribute",
        "working_directory",
        "grpc_port",
        "grpc_socket",
        "grpc_host",
        "use_ssl",
        # Misc others to exclude
        "use_legacy_code_server_behavior",
    }

    dg_dev_param_names = {param.name for param in dev_command.params}
    dagster_dev_param_names = {param.name for param in dagster_dev_command.params}
    dagster_dev_params_to_check = dagster_dev_param_names - exclude_dagster_dev_params

    unmatched_params = dagster_dev_params_to_check - dg_dev_param_names
    assert not unmatched_params, f"dg dev missing params: {unmatched_params}"


# Modify this test with a new option whenever a new forwarded option is added to `dagster-dev`.
def test_dev_command_forwards_options_to_dagster_dev():
    with ProxyRunner.test() as runner, isolated_example_code_location_foo_bar(runner):
        port = _find_free_port()
        options = [
            "--code-server-log-level",
            "debug",
            "--log-level",
            "debug",
            "--log-format",
            "json",
            "--port",
            str(port),
            "--host",
            "localhost",
            "--live-data-poll-rate",
            "3000",
        ]
        with _launch_dev_command(options) as dev_process:
            time.sleep(1)  # wait for dg dev to start
            child_processes = _get_child_processes(dev_process.pid)
            # On Unix, this is reliably the first child process, but the situation is more
            # complicated on Windows. So explicitly find the `uv run dagster dev` process.
            for proc in child_processes:
                print(_get_proc_repr(proc))  # noqa: T201
            uv_run_proc = next(p for p in child_processes if p.cmdline()[0] == "uv")
            expected_cmdline = [
                "uv",
                "run",
                "dagster",
                "dev",
                *options,
            ]
            assert uv_run_proc.cmdline() == expected_cmdline


# ########################
# ##### HELPERS
# ########################


@contextlib.contextmanager
def _launch_dev_command(
    options: list[str], capture_output: bool = False
) -> Iterator[subprocess.Popen]:
    # We start a new process instead of using the runner to avoid blocking the test. We need to
    # poll the webserver to know when it is ready.
    proc = open_subprocess(
        [
            "dg",
            "dev",
            *options,
        ],
        stdout=subprocess.PIPE if capture_output else None,
    )
    try:
        yield proc
    finally:
        if psutil.pid_exists(proc.pid):
            child_processes = _get_child_processes(proc.pid)
            interrupt_subprocess(proc.pid)
            proc.wait(timeout=10)
            # The `dagster dev` command exits before the gRPC servers it spins up have shutdown. Wait
            # for the child processes to exit here to make sure we don't leave any hanging processes.
            #
            # We disable this check on Windows because interrupt signal propagation does not work in a
            # CI environment. Interrupt propagation is dependent on processes sharing a console (which
            # is the case in a user terminal session, but not in a CI environment). So on windows, we
            # force kill the processes after a timeout.
            _wait_for_child_processes_to_exit(
                child_processes, timeout=10, force_kill=platform.system() == "Windows"
            )


def _get_child_processes(pid) -> list[psutil.Process]:
    parent = psutil.Process(pid)
    # Windows will sometimes return the parent process as its own child. Filter this out.
    return [p for p in parent.children(recursive=True) if p.pid != pid]


def _find_free_port() -> int:
    with contextlib.closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(("", 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]


def _wait_for_webserver_running(port: int) -> None:
    start_time = time.time()
    while True:
        try:
            server_info = requests.get(f"http://localhost:{port}/server_info").json()
            if server_info:
                return
        except:
            print("Waiting for dagster-webserver to be ready..")  # noqa: T201

        if time.time() - start_time > 30:
            raise Exception("Timed out waiting for dagster-webserver to serve requests")

        time.sleep(1)


_GET_CODE_LOCATION_NAMES_QUERY = """
query GetCodeLocationNames {
  repositoriesOrError {
    __typename
    ... on RepositoryConnection {
      nodes {
        name
        location {
          name
        }
      }
    }
    ... on PythonError {
      message
    }
  }
}
"""


def _query_code_locations(port: int) -> set[str]:
    gql_client = DagsterGraphQLClient(hostname="localhost", port_number=port)
    result = gql_client._execute(_GET_CODE_LOCATION_NAMES_QUERY)  # noqa: SLF001
    assert result["repositoriesOrError"]["__typename"] == "RepositoryConnection"
    return {node["location"]["name"] for node in result["repositoriesOrError"]["nodes"]}


def _wait_for_child_processes_to_exit(
    child_procs: list[psutil.Process], timeout: int, force_kill: bool = False
) -> None:
    start_time = time.time()
    while True:
        running_child_procs = [proc for proc in child_procs if proc.is_running()]
        if not running_child_procs:
            break
        if time.time() - start_time > timeout:
            stopped_child_procs = [proc for proc in child_procs if not proc.is_running()]
            stopped_proc_lines = [_get_proc_repr(proc) for proc in stopped_child_procs]
            running_proc_lines = [_get_proc_repr(proc) for proc in running_child_procs]
            desc = "\n".join(
                [
                    "STOPPED:",
                    *stopped_proc_lines,
                    "RUNNING:",
                    *running_proc_lines,
                ]
            )
            if force_kill:
                for proc in running_child_procs:
                    try:
                        proc.kill()
                    # Can happen if the process shut down from another shutting down in the
                    # iteration.
                    except psutil.NoSuchProcess:
                        pass
            else:
                raise Exception(
                    f"Timed out waiting for all child processes to exit. Remaining:\n{desc}"
                )
        time.sleep(0.5)


def _get_proc_repr(proc: psutil.Process) -> str:
    return f"PID [{proc.pid}] PPID [{_get_ppid(proc)}]: {_get_cmdline(proc)}"


def _get_ppid(proc: psutil.Process) -> str:
    try:
        return str(proc.ppid())
    except psutil.NoSuchProcess:
        return "IRRETRIEVABLE"


def _get_cmdline(proc: psutil.Process) -> str:
    try:
        return str(proc.cmdline())
    except psutil.NoSuchProcess:
        return "CMDLINE IRRETRIEVABLE"
