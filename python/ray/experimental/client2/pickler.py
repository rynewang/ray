"""Implements Pickling for client2.

Idea is from the original Ray Client: special treatment to the ObjectRef, but much
simpler.

On the server side: real ray.ObjectRef, with references kept in the server.
On the fly: PickledObjectRef which is just bytes
On the client side: still ray.ObjectRef, but without any references.
"""
import io
import sys
from typing import Any, Dict, NamedTuple, Tuple

import ray
import ray.cloudpickle as cloudpickle

if sys.version_info < (3, 8):
    try:
        import pickle5 as pickle  # noqa: F401
    except ImportError:
        import pickle  # noqa: F401
else:
    import pickle  # noqa: F401


# Represents an ObjectRef in transit.
class PickledObjectRef(NamedTuple("PickledObjectRef", [("ref_id", bytes)])):
    def __reduce__(self):
        # PySpark's namedtuple monkey patch breaks compatibility with
        # cloudpickle. Thus we revert this patch here if it exists.
        return object.__reduce__(self)


# Represents an ActorHandle in transit.
# Server -> Client: the whole "state" and the "current_session_and_job"
# Client -> Server: only state["actor_id"] which contains a binary id (bytes).
class PickledActorHandle(
    NamedTuple(
        "PickledActorHandle",
        [
            # the state as defined in ActorHandle._serialization_helper in local mode.
            ("state", Dict),
            # TODO: what is this current_session_and_job anyway and can we just set it None?
            (
                "current_session_and_job",
                Tuple,
            ),  # the thing that's missing in the serializer.
        ],
    )
):
    def __reduce__(self):
        # PySpark's namedtuple monkey patch breaks compatibility with
        # cloudpickle. Thus we revert this patch here if it exists.
        return object.__reduce__(self)


class ServerToClientPickler(cloudpickle.CloudPickler):
    def __init__(self, server, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.server = server

    def persistent_id(self, obj):
        if isinstance(obj, ray.ObjectRef):
            obj_id = obj.binary()
            if obj_id not in self.server.object_refs:
                # We're passing back a reference, probably inside a reference.
                # Let's hold onto it.
                self.server.object_refs[obj_id] = obj
            return PickledObjectRef(ref_id=obj_id)
        if isinstance(obj, ray.actor.ActorHandle):
            actor_id = obj._ray_actor_id.binary()
            if actor_id not in self.server.actor_refs:
                self.server.actor_refs[actor_id] = obj
            return PickledActorHandle(
                state=obj._local_serialization_helper(),
                current_session_and_job=ray._private.worker.global_worker.current_session_and_job,
            )
        return None


class ServerToClientUnpickler(pickle.Unpickler):
    def persistent_load(self, pid):
        if isinstance(pid, PickledObjectRef):
            return ray.ObjectRef(pid.ref_id)
        if isinstance(pid, PickledActorHandle):
            return ray.actor.ActorHandle._local_deserialization_helper(
                pid.state[0], pid.current_session_and_job
            )
        raise pickle.UnpicklingError("unknown type")


class ClientToServerPickler(cloudpickle.CloudPickler):
    # TODO: "ray" and more?
    def persistent_id(self, obj):
        if isinstance(obj, ray.ObjectRef):
            obj_id = obj.binary()
            return PickledObjectRef(ref_id=obj_id)
        if isinstance(obj, ray.actor.ActorHandle):
            return PickledActorHandle(
                state={
                    "actor_id": obj._ray_actor_id.binary(),
                },
                current_session_and_job=None,
            )
        return None


class ClientToServerUnpickler(pickle.Unpickler):
    def __init__(self, server, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.server = server

    def persistent_load(self, pid):
        if isinstance(pid, PickledObjectRef):
            return self.server.object_refs[pid.ref_id]
        if isinstance(pid, PickledActorHandle):
            actor_id_binary = pid.state["actor_id"]
            return self.server.actor_refs[actor_id_binary]
        raise pickle.UnpicklingError("unknown type")


def dumps_with_pickler_cls(cls, *args, **kwargs):
    # Usage:
    # my_dumps = dumps_with_pickler_cls(ServerToClientPickler, server)
    # bytes = my_dumps(value)
    def dumps(obj: Any):
        with io.BytesIO() as file:
            pickler = cls(file=file, *args, **kwargs)
            pickler.dump(obj)
            return file.getvalue()

    return dumps


def loads_with_unpickler_cls(cls, *args, **kwargs):
    # Usage:
    # my_loads = loads_with_unpickler_cls(ClientToServerUnpickler, server)
    # value = my_loads(bytes)
    def loads(data: bytes):
        file = io.BytesIO(data)
        unpickler = cls(file=file, *args, **kwargs)
        return unpickler.load()

    return loads
