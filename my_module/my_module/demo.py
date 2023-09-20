from my_module.actor.actor import Actor
from my_module.demo_nested import DemoNested


class Demo:
    def __init__(self, config):
        self.config = config

        # Create a nested actor
        self.nested_actor = Actor(DemoNested, {"is_nested": True})

    def sync_void(self):
        print("Hello, world!")

    def sync_return(self):
        return self.config

    def sync_throw(self):
        raise ValueError("This is a test error")

    async def async_void(self):
        print("Hello, world!")

    async def async_return(self):
        return self.config

    async def async_throw(self):
        raise ValueError("This is a test error")

    def nested_outer_method_sync(self, complex_obj):
        return self.nested_actor.nested_method_sync(complex_obj)

    async def nested_outer_method_async(self, complex_obj):
        return await self.nested_actor.nested_method_async(complex_obj)
