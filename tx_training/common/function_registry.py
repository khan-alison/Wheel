from typing import Dict, Any, Callable
from tx_training.helper.logger import LoggerSimple

logger = LoggerSimple.get_logger(__name__)


class FunctionRegistry:
    _transformations: Dict[any, Callable] = {}

    @classmethod
    def register(cls, name: str = None):
        def decorator(func: Callable):
            nonlocal name
            if name is None:
                name = func.__name__

            cls._transformations[name] = func
            logger.info(f"Registered transformation name: {name}")
            return func

        return decorator

    @classmethod
    def get_transformation(cls, name: str) -> Callable:
        if name not in cls._transformations:
            raise ValueError(
                f"Transformation function '{name}' not found from registry."
            )
        return cls._transformations[name]

    @classmethod
    def list_transformations(cls) -> list:
        return list(cls._transformations.keys())
