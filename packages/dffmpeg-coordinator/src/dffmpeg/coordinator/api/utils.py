import ipaddress
from typing import List

from dffmpeg.common.models import CIDR


def is_ip_allowed(client_host: str, allowed_cidrs: List[CIDR]) -> bool:
    """
    Checks if a given client host IP is within any of the allowed CIDRs.
    """
    try:
        client_ip = ipaddress.ip_address(client_host)
        for cidr in allowed_cidrs:
            if client_ip in cidr:
                return True
        return False
    except ValueError:
        return False


def get_negotiated_transport(client_transports: List[str], server_transports: List[str]) -> str:
    """
    Finds the first transport method supported by both client and server.

    Args:
        client_transports: List of transports supported by the client.
        server_transports: List of transports supported by the server.

    Returns:
        str: The name of the negotiated transport.

    Raises:
        ValueError: If no common transport is found.
    """
    for client_transport in client_transports:
        if client_transport in server_transports:
            return client_transport
    raise ValueError("Cannot find mutually supported transport!")
