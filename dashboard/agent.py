import argparse
import asyncio
import io
import json
import logging
import logging.handlers
import os
import pathlib
import sys
import signal

import ray
import ray._private.ray_constants as ray_constants
import ray._private.services
import ray._private.utils
import ray.dashboard.consts as dashboard_consts
import ray.dashboard.utils as dashboard_utils
from ray.dashboard.consts import _PARENT_DEATH_THREASHOLD
from ray._raylet import GcsClient
from ray._private.gcs_utils import GcsAioClient
from ray._private.ray_logging import (
    setup_component_logger,
    configure_log_file,
)
from ray.experimental.internal_kv import (
    _initialize_internal_kv,
    _internal_kv_initialized,
)

# Import psutil after ray so the packaged version is used.
import psutil

# Publishes at most this number of lines of Raylet logs, when the Raylet dies
# unexpectedly.
_RAYLET_LOG_MAX_PUBLISH_LINES = 20

# Reads at most this amount of Raylet logs from the tail, for publishing and
# checking if the Raylet was terminated gracefully.
_RAYLET_LOG_MAX_TAIL_SIZE = 1 * 1024**2

try:
    create_task = asyncio.create_task
except AttributeError:
    create_task = asyncio.ensure_future

logger = logging.getLogger(__name__)


class DashboardAgent:
    def __init__(
        self,
        node_ip_address,
        dashboard_agent_port,
        gcs_address,
        minimal,
        metrics_export_port=None,
        node_manager_port=None,
        listen_port=ray_constants.DEFAULT_DASHBOARD_AGENT_LISTEN_PORT,
        disable_metrics_collection: bool = False,
        *,  # the following are required kwargs
        object_store_name: str,
        raylet_name: str,
        log_dir: str,
        temp_dir: str,
        session_dir: str,
        logging_params: dict,
        agent_id: int,
        session_name: str,
    ):
        """Initialize the DashboardAgent object."""
        # Public attributes are accessible for all agent modules.
        self.ip = node_ip_address
        self.minimal = minimal

        assert gcs_address is not None
        self.gcs_address = gcs_address

        self.temp_dir = temp_dir
        self.session_dir = session_dir
        self.log_dir = log_dir
        self.dashboard_agent_port = dashboard_agent_port
        self.metrics_export_port = metrics_export_port
        self.node_manager_port = node_manager_port
        self.listen_port = listen_port
        self.object_store_name = object_store_name
        self.raylet_name = raylet_name
        self.logging_params = logging_params
        self.node_id = os.environ["RAY_NODE_ID"]
        self.metrics_collection_disabled = disable_metrics_collection
        self.agent_id = agent_id
        self.session_name = session_name
        # TODO(edoakes): RAY_RAYLET_PID isn't properly set on Windows. This is
        # only used for fate-sharing with the raylet and we need a different
        # fate-sharing mechanism for Windows anyways.
        if sys.platform not in ["win32", "cygwin"]:
            self.ppid = int(os.environ["RAY_RAYLET_PID"])
            assert self.ppid > 0
            logger.info("Parent pid is %s", self.ppid)

        # grpc server is None in mininal.
        self.server = None
        # http_server is None in minimal.
        self.http_server = None

        # Used by the agent and sub-modules.
        # TODO(architkulkarni): Remove gcs_client once the agent exclusively uses
        # gcs_aio_client and not gcs_client.
        self.gcs_client = GcsClient(address=self.gcs_address)
        _initialize_internal_kv(self.gcs_client)
        assert _internal_kv_initialized()
        self.gcs_aio_client = GcsAioClient(address=self.gcs_address)

        if not self.minimal:
            self._init_non_minimal()

    def _init_non_minimal(self):
        from ray._private.gcs_pubsub import GcsAioPublisher
        self.aio_publisher = GcsAioPublisher(address=self.gcs_address)

        try:
            from grpc import aio as aiogrpc
        except ImportError:
            from grpc.experimental import aio as aiogrpc

        # We would want to suppress deprecating warnings from aiogrpc library
        # with the usage of asyncio.get_event_loop() in python version >=3.10
        # This could be removed once https://github.com/grpc/grpc/issues/32526
        # is released, and we used higher versions of grpcio that that.
        if sys.version_info.major >= 3 and sys.version_info.minor >= 10:
            import warnings

            with warnings.catch_warnings():
                warnings.simplefilter("ignore", category=DeprecationWarning)
                aiogrpc.init_grpc_aio()
        else:
            aiogrpc.init_grpc_aio()

        self.server = aiogrpc.server(options=(("grpc.so_reuseport", 0),))
        grpc_ip = "127.0.0.1" if self.ip == "127.0.0.1" else "0.0.0.0"
        try:
            self.grpc_port = ray._private.tls_utils.add_port_to_grpc_server(
                self.server, f"{grpc_ip}:{self.dashboard_agent_port}"
            )
        except Exception:
            # TODO(SongGuyang): Catch the exception here because there is
            # port conflict issue which brought from static port. We should
            # remove this after we find better port resolution.
            logger.exception(
                "Failed to add port to grpc server. Agent will stay alive but "
                "disable the grpc service."
            )
            self.server = None
            self.grpc_port = None
        else:
            logger.info("Dashboard agent grpc address: %s:%s", grpc_ip, self.grpc_port)

    async def _configure_http_server(self, modules):
        from ray.dashboard.http_server_agent import HttpServerAgent

        http_server = HttpServerAgent(self.ip, self.listen_port)
        await http_server.start(modules)
        return http_server

    def _load_modules(self):
        """Load dashboard agent modules."""
        modules = []
        agent_cls_list = dashboard_utils.get_all_modules(
            dashboard_utils.DashboardAgentModule
        )
        for cls in agent_cls_list:
            logger.info(
                "Loading %s: %s", dashboard_utils.DashboardAgentModule.__name__, cls
            )
            c = cls(self)
            modules.append(c)
        logger.info("Loaded %d modules.", len(modules))
        return modules

    @property
    def http_session(self):
        assert (
            self.http_server
        ), "Accessing unsupported API (HttpServerAgent) in a minimal ray."
        return self.http_server.http_session

    @property
    def publisher(self):
        assert (
            self.aio_publisher
        ), "Accessing unsupported API (GcsAioPublisher) in a minimal ray."
        return self.aio_publisher

    async def run(self):
        async def _check_parent():
            """Check if raylet is dead and fate-share if it is."""
            try:
                curr_proc = psutil.Process()
                parent_death_cnt = 0
                while True:
                    parent = curr_proc.parent()
                    # If the parent is dead, it is None.
                    parent_gone = parent is None
                    init_assigned_for_parent = False
                    parent_changed = False

                    if parent:
                        # Sometimes, the parent is changed to the `init` process.
                        # In this case, the parent.pid is 1.
                        init_assigned_for_parent = parent.pid == 1
                        # Sometimes, the parent is dead, and the pid is reused
                        # by other processes. In this case, this condition is triggered.
                        parent_changed = self.ppid != parent.pid

                    if parent_gone or init_assigned_for_parent or parent_changed:
                        parent_death_cnt += 1
                        logger.warning(
                            f"Raylet is considered dead {parent_death_cnt} X. "
                            f"If it reaches to {_PARENT_DEATH_THREASHOLD}, the agent "
                            f"will kill itself. Parent: {parent}, "
                            f"parent_gone: {parent_gone}, "
                            f"init_assigned_for_parent: {init_assigned_for_parent}, "
                            f"parent_changed: {parent_changed}."
                        )
                        if parent_death_cnt < _PARENT_DEATH_THREASHOLD:
                            await asyncio.sleep(
                                dashboard_consts.DASHBOARD_AGENT_CHECK_PARENT_INTERVAL_S
                            )
                            continue

                        log_path = os.path.join(self.log_dir, "raylet.out")
                        error = False
                        msg = f"Raylet is terminated: ip={self.ip}, id={self.node_id}. "
                        try:
                            with open(log_path, "r", encoding="utf-8") as f:
                                # Seek to _RAYLET_LOG_MAX_TAIL_SIZE from the end if the
                                # file is larger than that.
                                f.seek(0, io.SEEK_END)
                                pos = max(0, f.tell() - _RAYLET_LOG_MAX_TAIL_SIZE)
                                f.seek(pos, io.SEEK_SET)
                                # Read remaining logs by lines.
                                raylet_logs = f.readlines()
                                # Assume the SIGTERM message must exist within the last
                                # _RAYLET_LOG_MAX_TAIL_SIZE of the log file.
                                if any(
                                    "Raylet received SIGTERM" in line
                                    for line in raylet_logs
                                ):
                                    msg += "Termination is graceful."
                                    logger.info(msg)
                                else:
                                    msg += (
                                        "Termination is unexpected. Possible reasons "
                                        "include: (1) SIGKILL by the user or system "
                                        "OOM killer, (2) Invalid memory access from "
                                        "Raylet causing SIGSEGV or SIGBUS, "
                                        "(3) Other termination signals. "
                                        f"Last {_RAYLET_LOG_MAX_PUBLISH_LINES} lines "
                                        "of the Raylet logs:\n"
                                    )
                                    msg += "    " + "    ".join(
                                        raylet_logs[-_RAYLET_LOG_MAX_PUBLISH_LINES:]
                                    )
                                    error = True
                        except Exception as e:
                            msg += f"Failed to read Raylet logs at {log_path}: {e}!"
                            logger.exception(msg)
                            error = True
                        if error:
                            logger.error(msg)
                            # TODO: switch to async if necessary.
                            ray._private.utils.publish_error_to_driver(
                                ray_constants.RAYLET_DIED_ERROR,
                                msg,
                                gcs_publisher=ray._raylet.GcsPublisher(
                                    address=self.gcs_address
                                ),
                            )
                        else:
                            logger.info(msg)
                        sys.exit(0)
                    else:
                        parent_death_cnt = 0
                    await asyncio.sleep(
                        dashboard_consts.DASHBOARD_AGENT_CHECK_PARENT_INTERVAL_S
                    )
            except Exception:
                logger.exception("Failed to check parent PID, exiting.")
                sys.exit(1)

        if sys.platform not in ["win32", "cygwin"]:
            check_parent_task = create_task(_check_parent())

        # Start a grpc asyncio server.
        if self.server:
            await self.server.start()

        modules = self._load_modules()

        # Setup http server if necessary.
        if not self.minimal:
            # If the agent is not minimal it should start the http server
            # to communicate with the dashboard in a head node.
            # Http server is not started in the minimal version because
            # it requires additional dependencies that are not
            # included in the minimal ray package.
            try:
                self.http_server = await self._configure_http_server(modules)
            except Exception:
                # TODO(SongGuyang): Catch the exception here because there is
                # port conflict issue which brought from static port. We should
                # remove this after we find better port resolution.
                logger.exception(
                    "Failed to start http server. Agent will stay alive but "
                    "disable the http service."
                )

        # Write the dashboard agent port to kv.
        # TODO: Use async version if performance is an issue
        # -1 should indicate that http server is not started.
        http_port = -1 if not self.http_server else self.http_server.http_port
        grpc_port = -1 if not self.server else self.grpc_port
        await self.gcs_aio_client.internal_kv_put(
            f"{dashboard_consts.DASHBOARD_AGENT_PORT_PREFIX}{self.node_id}".encode(),
            json.dumps([http_port, grpc_port]).encode(),
            True,
            namespace=ray_constants.KV_NAMESPACE_DASHBOARD,
        )

        tasks = [m.run(self.server) for m in modules]
        if sys.platform not in ["win32", "cygwin"]:
            tasks.append(check_parent_task)
        await asyncio.gather(*tasks)

        await self.server.wait_for_termination()

        if self.http_server:
            await self.http_server.cleanup()


def open_capture_files(log_dir):
    filename = f"agent-{args.agent_id}"
    return (
        ray._private.utils.open_log(pathlib.Path(log_dir) / f"{filename}.out"),
        ray._private.utils.open_log(pathlib.Path(log_dir) / f"{filename}.err"),
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Dashboard agent.")
    parser.add_argument(
        "--node-ip-address",
        required=True,
        type=str,
        help="the IP address of this node.",
    )
    parser.add_argument(
        "--gcs-address", required=True, type=str, help="The address (ip:port) of GCS."
    )
    parser.add_argument(
        "--metrics-export-port",
        required=True,
        type=int,
        help="The port to expose metrics through Prometheus.",
    )
    parser.add_argument(
        "--dashboard-agent-port",
        required=True,
        type=int,
        help="The port on which the dashboard agent will receive GRPCs.",
    )
    parser.add_argument(
        "--node-manager-port",
        required=True,
        type=int,
        help="The port to use for starting the node manager",
    )
    parser.add_argument(
        "--object-store-name",
        required=True,
        type=str,
        default=None,
        help="The socket name of the plasma store",
    )
    parser.add_argument(
        "--listen-port",
        required=False,
        type=int,
        default=ray_constants.DEFAULT_DASHBOARD_AGENT_LISTEN_PORT,
        help="Port for HTTP server to listen on",
    )
    parser.add_argument(
        "--raylet-name",
        required=True,
        type=str,
        default=None,
        help="The socket path of the raylet process",
    )
    parser.add_argument(
        "--logging-level",
        required=False,
        type=lambda s: logging.getLevelName(s.upper()),
        default=ray_constants.LOGGER_LEVEL,
        choices=ray_constants.LOGGER_LEVEL_CHOICES,
        help=ray_constants.LOGGER_LEVEL_HELP,
    )
    parser.add_argument(
        "--logging-format",
        required=False,
        type=str,
        default=ray_constants.LOGGER_FORMAT,
        help=ray_constants.LOGGER_FORMAT_HELP,
    )
    parser.add_argument(
        "--logging-filename",
        required=False,
        type=str,
        default=dashboard_consts.DASHBOARD_AGENT_LOG_FILENAME,
        help="Specify the name of log file, "
        'log to stdout if set empty, default is "{}".'.format(
            dashboard_consts.DASHBOARD_AGENT_LOG_FILENAME
        ),
    )
    parser.add_argument(
        "--logging-rotate-bytes",
        required=False,
        type=int,
        default=ray_constants.LOGGING_ROTATE_BYTES,
        help="Specify the max bytes for rotating "
        "log file, default is {} bytes.".format(ray_constants.LOGGING_ROTATE_BYTES),
    )
    parser.add_argument(
        "--logging-rotate-backup-count",
        required=False,
        type=int,
        default=ray_constants.LOGGING_ROTATE_BACKUP_COUNT,
        help="Specify the backup count of rotated log file, default is {}.".format(
            ray_constants.LOGGING_ROTATE_BACKUP_COUNT
        ),
    )
    parser.add_argument(
        "--log-dir",
        required=True,
        type=str,
        default=None,
        help="Specify the path of log directory.",
    )
    parser.add_argument(
        "--temp-dir",
        required=True,
        type=str,
        default=None,
        help="Specify the path of the temporary directory use by Ray process.",
    )
    parser.add_argument(
        "--session-dir",
        required=True,
        type=str,
        default=None,
        help="Specify the path of this session.",
    )

    parser.add_argument(
        "--minimal",
        action="store_true",
        help=(
            "Minimal agent only contains a subset of features that don't "
            "require additional dependencies installed when ray is installed "
            "by `pip install 'ray[default]'`."
        ),
    )
    parser.add_argument(
        "--disable-metrics-collection",
        action="store_true",
        help=("If this arg is set, metrics report won't be enabled from the agent."),
    )
    parser.add_argument(
        "--agent-id",
        required=True,
        type=int,
        help="ID to report when registering with raylet",
        default=os.getpid(),
    )
    parser.add_argument(
        "--session-name",
        required=False,
        type=str,
        default=None,
        help="The session name (cluster id) of this cluster.",
    )

    args = parser.parse_args()

    try:
        logging_params = dict(
            logging_level=args.logging_level,
            logging_format=args.logging_format,
            log_dir=args.log_dir,
            filename=args.logging_filename,
            max_bytes=args.logging_rotate_bytes,
            backup_count=args.logging_rotate_backup_count,
        )
        logger = setup_component_logger(**logging_params)

        # Initialize event loop, see Dashboard init code for caveat
        # w.r.t grpc server init in the DashboardAgent initializer.
        loop = ray._private.utils.get_or_create_event_loop()

        # Setup stdout/stderr redirect files
        out_file, err_file = open_capture_files(args.log_dir)
        configure_log_file(out_file, err_file)

        agent = DashboardAgent(
            args.node_ip_address,
            args.dashboard_agent_port,
            args.gcs_address,
            args.minimal,
            temp_dir=args.temp_dir,
            session_dir=args.session_dir,
            log_dir=args.log_dir,
            metrics_export_port=args.metrics_export_port,
            node_manager_port=args.node_manager_port,
            listen_port=args.listen_port,
            object_store_name=args.object_store_name,
            raylet_name=args.raylet_name,
            logging_params=logging_params,
            disable_metrics_collection=args.disable_metrics_collection,
            agent_id=args.agent_id,
            session_name=args.session_name,
        )

        def sigterm_handler():
            logger.warning("Exiting with SIGTERM immediately...")
            # Exit code 0 will be considered as an expected shutdown
            os._exit(signal.SIGTERM)

        if sys.platform != "win32":
            # TODO(rickyyx): we currently do not have any logic for actual
            # graceful termination in the agent. Most of the underlying
            # async tasks run by the agent head doesn't handle CancelledError.
            # So a truly graceful shutdown is not trivial w/o much refactoring.
            # Re-open the issue: https://github.com/ray-project/ray/issues/25518
            # if a truly graceful shutdown is required.
            loop.add_signal_handler(signal.SIGTERM, sigterm_handler)

        loop.run_until_complete(agent.run())
    except Exception:
        logger.exception("Agent is working abnormally. It will exit immediately.")
        exit(1)
