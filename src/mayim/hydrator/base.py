from inspect import Parameter
from typing import Any, Dict, List, Type, Union


class Hydrator:
    fallback = dict

    def _make(self, model: Type[object]):
        def factory(data: Union[Dict[str, Any], List[Dict[str, Any]]]):
            if model is None:
                return None
            if isinstance(data, list):
                return [self.hydrate(row, model=model) for row in data]
            return self.hydrate(data, model=model)

        return factory

    def hydrate(
        self, data: Dict[str, Any], model: Type[object] = Parameter.empty
    ):
        if model is Parameter.empty:
            model = self.fallback
        elif model in (str, int, float, bool):
            return model(*data.values())
        return model(**data)
