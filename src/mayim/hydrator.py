from typing import Any, List, Mapping, Type, Union
from typing import Any


class Hydrator:
    def _make(self, model: Type[object]):
        def factory(data: Union[Mapping[str, Any], List[Mapping[str, Any]]]):
            if isinstance(data, list):
                return [self.hydrate(row, model=model) for row in data]
            return self.hydrate(data, model=model)

        return factory

    def hydrate(self, data: Mapping[str, Any], model: Type[object] = dict):
        return model(**data)
