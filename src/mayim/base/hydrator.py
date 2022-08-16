from inspect import Parameter
from typing import Any, Dict, List, Type, Union


class Hydrator:
    """Object responsible for casting from the data layer to a model"""

    fallback: Type[object] = dict
    """The model type that will be used if there is none passed in the
    hydrate method"""

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
        """Perform casting operation

        Args:
            data (Dict[str, Any]): Raw data from the source
            model (Type[object], optional): The model that will do the
                casting. If no value is passed, it will use whatever the
                Hydrator's fallback value is set to. Defaults to
                `Parameter.empty`.

        Returns:
            _type_: The data cast into the model
        """
        if model is Parameter.empty:
            model = self.fallback
        elif model in (str, int, float, bool):
            return model(*data.values())
        elif model.__name__ == "Dict":
            return data
        return model(**data)
