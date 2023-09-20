from my_module.demo_complex_object import DemoComplexObject


class DemoNested:
    def __init__(self, config):
        self.config = config

    def nested_method_sync(self, complex_obj):
        return DemoComplexObject(complex_obj.data + " processed")

    async def nested_method_async(self, complex_obj):
        return DemoComplexObject(complex_obj.data + " processed")
