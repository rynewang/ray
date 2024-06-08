import argparse
import os

import ray
from ray._private.test_utils import get_ray_default_worker_file_path

parser = argparse.ArgumentParser()
parser.add_argument("--image", type=str, help="The docker image to use for Ray worker")
parser.add_argument(
    "--use-image-uri-api",
    action="store_true",
    help="Whether to use the new `image_uri` API instead of the old `container` API.",
)
args = parser.parse_args()

worker_pth = get_ray_default_worker_file_path()


if args.use_image_uri_api:
    runtime_env = {"image_uri": args.image}
else:
    runtime_env = {"container": {"image": args.image, "worker_path": worker_pth}}


@ray.remote(runtime_env=runtime_env)
def f():
    return os.environ.get("RAY_TEST_ABC")


@ray.remote(runtime_env=runtime_env)
def g():
    return os.environ.get("TEST_ABC")


assert ray.get(f.remote()) == "1"
assert ray.get(g.remote()) is None
