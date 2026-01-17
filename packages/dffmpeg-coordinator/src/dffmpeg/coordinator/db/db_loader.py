from importlib.metadata import entry_points


def load(group: str, engine):
    available_entrypoints = entry_points(group=group)
    matching = [x for x in available_entrypoints if x.name == engine]
    if len(matching) != 1:
        available_names = ", ".join([x.name for x in available_entrypoints])
        raise ValueError(f"Invalid database engine \"{engine}\" for \"{group}\"! Expected one of: {available_names}")
    loaded = matching[0].load()
    return loaded
