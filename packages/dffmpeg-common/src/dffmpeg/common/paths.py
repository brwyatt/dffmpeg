import os
from pathlib import Path
from typing import Dict, List, Tuple


def map_path(raw_path: str, path_map: Dict[str, str]) -> Tuple[str, str | None]:
    """
    Translates an absolute path to a mapped path variable if it falls within a configured path map.
    Returns (processed_path, used_path_variable)

    If the path does not match any map, it returns the original path and None for the used_path_variable.
    """
    prefix = ""
    path_to_process = raw_path
    if raw_path.startswith("file:"):
        prefix = "file:"
        path_to_process = raw_path[5:]

    if not path_to_process.startswith("/"):
        return raw_path, None

    try:
        abs_path = str(Path(path_to_process).resolve())
    except Exception:
        abs_path = str(Path(path_to_process).absolute())

    sorted_paths = sorted(path_map.items(), key=lambda x: len(x[1]), reverse=True)

    for var_name, local_path in sorted_paths:
        if abs_path.startswith(local_path):
            remainder = abs_path[len(local_path) :]
            if not remainder or remainder.startswith(os.sep):
                new_arg = f"{prefix}${var_name}{remainder}"
                return new_arg, var_name

    return raw_path, None


def map_arguments(raw_args: List[str], path_map: Dict[str, str]) -> Tuple[List[str], List[str]]:
    """
    Processes a list of arguments to identify and replace local paths with path variables.
    Returns (processed_args, used_path_variables)
    """
    processed_args = []
    used_paths = set()

    for arg in raw_args:
        processed_arg, used_var = map_path(arg, path_map)
        processed_args.append(processed_arg)
        if used_var is not None:
            used_paths.add(used_var)

    return processed_args, list(used_paths)


def resolve_path(mapped_path: str, path_map: Dict[str, str]) -> str:
    """
    Resolves a path mapped with a $Variable back into an absolute path on the current system using path_map. If the
    path does not start with $ (or file:$), or if the variable is not in the path_map, it returns the path unaltered.
    """
    prefix = ""
    var_part = mapped_path
    if mapped_path.startswith("file:$"):
        prefix = "file:"
        var_part = mapped_path[5:]

    if var_part.startswith("$"):
        # Extract variable name (up to first / or end of string)
        parts = var_part.split("/", 1)
        variable_with_prefix = parts[0]
        variable = variable_with_prefix[1:]  # Strip $

        if variable in path_map:
            base_path = path_map[variable]
            suffix = ("/" + parts[1]) if len(parts) > 1 else ""
            # Ensure we don't end up with double slashes if base_path ends with /
            if base_path.endswith("/") and suffix.startswith("/"):
                resolved_arg = base_path + suffix[1:]
            else:
                resolved_arg = base_path + suffix
            return f"{prefix}{resolved_arg}"

    return mapped_path


def resolve_arguments(mapped_args: List[str], path_map: Dict[str, str]) -> List[str]:
    """
    Resolves a list of mapped arguments back to system-local paths.
    """
    return [resolve_path(arg, path_map) for arg in mapped_args]
