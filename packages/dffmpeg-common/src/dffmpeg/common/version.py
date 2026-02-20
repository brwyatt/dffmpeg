import importlib.metadata


def get_package_version(package_name: str) -> str:
    """
    Retrieves the version of the specified package.

    Args:
        package_name (str): The name of the package.

    Returns:
        str: The version of the package, or "unknown" if not found.
    """
    try:
        return importlib.metadata.version(package_name)
    except importlib.metadata.PackageNotFoundError:
        return "unknown"
