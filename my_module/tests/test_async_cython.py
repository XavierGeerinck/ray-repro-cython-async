import pickle
import pytest
import ray

from my_module.actor.actor import Actor
from my_module.demo import Demo
from my_module.demo_complex_object import DemoComplexObject

DEMO_CONFIG = {"config": "test"}


@pytest.mark.asyncio
async def test_actor_with_demo():
    ray.init(ignore_reinit_error=True)

    # Create an instance of the Actor class with the Demo class
    demo_actor = Actor(Demo, DEMO_CONFIG)

    # We should be able to call a sync method that prints
    demo_actor.sync_void()

    # We should be able to call an async method that prints
    await demo_actor.async_void()

    # We should be able to call a sync method that returns
    result = demo_actor.sync_return()
    assert result == DEMO_CONFIG

    # We should be able to call an async method that returns
    result = await demo_actor.async_return()
    assert result == DEMO_CONFIG

    # We should be able to raise an error in a sync method
    with pytest.raises(ValueError) as excinfo:
        demo_actor.sync_throw()

    assert "This is a test error" in str(excinfo.value)

    # We should be able to raise an error in an async method
    with pytest.raises(ValueError) as excinfo:
        await demo_actor.async_throw()

    assert "This is a test error" in str(excinfo.value)

    # We should be able to call a sync method on a nested actor
    result = demo_actor.nested_outer_method_sync(DemoComplexObject("test"))
    assert result.data == "test processed"

    # We should be able to call an async method on a nested actor
    result = await demo_actor.nested_outer_method_async(DemoComplexObject("test"))
    assert result.data == "test processed"

    # We should be able to pickle the actors
    actor_serialized = pickle.dumps(demo_actor)
    actor_deserialized = pickle.loads(actor_serialized)
    actor_deserialized_res = actor_deserialized.nested_outer_method_sync(
        DemoComplexObject("test_after_pickling")
    )
    assert actor_deserialized_res.data == "test_after_pickling processed"

    # Terminate the actor
    demo_actor.terminate()

    ray.shutdown()
