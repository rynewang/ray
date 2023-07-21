import ray.dashboard.utils as dashboard_utils
import ray.dashboard.optional_utils as optional_utils
from ray.dashboard.modules.healthz.utils import HealthChecker
import ray.exceptions
from aiohttp.web import Request, Response

routes = optional_utils.ClassMethodRouteTable


class HealthzAgent(dashboard_utils.DashboardAgentModule):
    """Health check in the agent.

    This module adds health check related endpoint to the agent to check
    local components' health.
    """

    def __init__(self, dashboard_agent):
        super().__init__(dashboard_agent)
        self._health_checker = HealthChecker(
            dashboard_agent.gcs_aio_client,
            f"{dashboard_agent.ip}:{dashboard_agent.node_manager_port}",
        )

    @routes.get("/api/local_raylet_healthz")
    async def health_check(self, req: Request) -> Response:
        try:
            alive = await self._health_checker.check_local_raylet_liveness()
            if alive is False:
                return Response(status=503, text="Local Raylet failed")
        except ray.exceptions.RpcError as e:
            # We only consider the error other than GCS unreachable as raylet failure
            # to avoid false positive.
            # In case of GCS failed, Raylet will crash eventually if GCS is not back
            # within a given time and the check will fail since agent can't live
            # without a local raylet.
            # Note: we want to cehck the rpc error code, but we can't import grpc since
            # this is a minimal module. I hardcode these error codes because grpc status
            # codes are relatively stable. Here is the reference link:
            # https://grpc.github.io/grpc/core/md_doc_statuscodes.html
            if e.rpc_code not in (
                14,  # grpc.StatusCode.UNAVAILABLE,
                2,  # grpc.StatusCode.UNKNOWN,
                4,  # grpc.StatusCode.DEADLINE_EXCEEDED,
            ):
                return Response(status=503, text=f"Health check failed due to: {e}")

        return Response(
            text="success",
            content_type="application/text",
        )

    async def run(self, server):
        pass

    @staticmethod
    def is_minimal_module():
        return True
