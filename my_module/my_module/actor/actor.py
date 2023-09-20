import traceback
import uuid
from typing import Type, TypeVar

import ray

T = TypeVar("T")


class Actor:
    """
    Define an Actor class that acts as a Proxy for Ray Actors

    We use this to wrap the Ray Actor and provide a more intuitive API as well as resolve cython compatibility issues
    more specifically it provides:
    1. Not having to use `.remote`
    2. Not having to use `ray.get` to get the result
    3. Supporting async methods natively, which are converted to sync methods always.
        This mainly resolves nested serialization issues causing pickle issues (ref. cannot pickle `_cython_3_2_2.coroutine`)
    """

    def __init__(self, cls: Type[T], *actor_args, **actor_kwargs):
        self._cls = cls
        self._name = f"{cls.__name__.lower()}_{str(uuid.uuid4())}"
        self._handle = (
            ray.remote(cls).options(name=self._name).remote(*actor_args, **actor_kwargs)
        )

    def __getattr__(self, attr_name) -> T:
        """
        Execute this method on the Ray Actor Instance
        """
        method = getattr(self._handle, attr_name)

        # Check if the method is async
        # we can detect this in self._cls if it's marked as a coroutine
        # attr = getattr(self._cls, attr_name)
        # is_async = cython_util.is_coroutine(attr)

        # if is_async:
        # return self._make_async_proxy(method)
        # else:
        return self._make_sync_proxy(method)

    # def _make_async_proxy(self, method):
    #     event_loop = asyncio.get_event_loop()

    #     def proxy(*args, **kwargs):
    #         # return event_loop.run_until_complete(method.remote(*args, **kwargs))

    #         async def await_obj_ref():
    #             await method.remote(*args, **kwargs)
    #             await asyncio.wait([method.remote()])

    #         return asyncio.run(await_obj_ref())

    #         # Avoid pickling by creating a future task and waiting for it to complete
    #         # future = event_loop.create_task(method.remote(*args, **kwargs))
    #         # await asyncio.wait([future])  # Wait for the future to complete
    #         # return future.result()

    #     return proxy

    def _make_sync_proxy(self, method):
        def proxy(*args, **kwargs):
            result_id = method.remote(*args, **kwargs)

            try:
                return ray.get(result_id)
            except ray.exceptions.RayTaskError as e:
                print(e)
                traceback.print_stack()
                # Handle the exception here, e.g., by re-raising it or returning an error value
                raise e
            except Exception as e:
                print(e)
                traceback.print_stack()
                # Handle the exception here, e.g., by re-raising it or returning an error value
                raise e

        return proxy

    def terminate(self):
        """
        Terminate the actor

        This uses a undocumented featured named `__ray_terminate__`
        https://github.com/ray-project/ray/blob/0e77916b0415ce95550610f8b61764eeadf7aa6c/python/ray/actor.py#L1340
        """
        self._handle.__ray_terminate__.remote()

    def __getstate__(self):
        return {"_name": self._name, "_handle": self._handle, "_cls": self._cls}

    def __setstate__(self, state):
        self._name = state["_name"]
        self._handle = state["_handle"]
        self._cls = state["_cls"]
