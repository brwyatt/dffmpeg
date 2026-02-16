class Colors:
    RESET = "\033[0m"
    RED = "\033[31m"
    GREEN = "\033[32m"
    YELLOW = "\033[33m"
    BLUE = "\033[34m"
    CYAN = "\033[36m"
    MAGENTA = "\033[35m"


def colorize(text: str, color: str) -> str:
    """Wraps text in ANSI color codes."""
    return f"{color}{text}{Colors.RESET}"


STATUS_COLORS = {
    "online": Colors.GREEN,
    "offline": Colors.RED,
    "running": Colors.BLUE,
    "completed": Colors.GREEN,
    "failed": Colors.RED,
    "canceled": Colors.YELLOW,
}


def colorize_status(status: str) -> str:
    """Colorizes a status string based on predefined mappings."""

    # Split off and color only the first word.
    # This is to handle statuses such as `failed (79)`.
    parts = status.split(" ", 1)
    color = STATUS_COLORS.get(parts[0].lower())
    if color:
        # Colorize and put back together
        return colorize(parts[0], color) + " ".join([""] + parts[1:])
    return status
