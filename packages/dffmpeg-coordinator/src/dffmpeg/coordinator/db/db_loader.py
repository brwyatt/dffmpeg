from importlib.metadata import entry_points
from typing import cast, Type, TypeVar

from dffmpeg.coordinator.db.engines import BaseDB
 

T = TypeVar("T", bound=BaseDB)


def load(group: str, engine, expected: Type[T] = BaseDB) -> Type[T]:
    available_entrypoints = entry_points(group=group)
    matching = [x for x in available_entrypoints if x.name == engine]

    if len(matching) != 1:
        available_names = ", ".join([x.name for x in available_entrypoints])
        raise ValueError(
            f"Invalid database engine \"{engine}\" for \"{group}\"! "
            f"Expected one of: {available_names}"
        )

    loaded = matching[0].load()

    if not isinstance(loaded, type) or not issubclass(loaded, expected):
        raise TypeError(
            f"Entrypoint {engine} loaded {matching[0].name}, "
            f"which is not a subclass of {expected}"
        )

    return cast(Type[T], loaded)
