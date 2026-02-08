import asyncio
import logging
import os
import re
from pathlib import Path
from typing import Dict, List, Set, Union

from dffmpeg.worker.config import MountConfig, MountManagementConfig

logger = logging.getLogger(__name__)


def path_to_unit_name(path: Union[str, Path]) -> str:
    """
    Converts a filesystem path to a systemd mount unit name.
    e.g., /mnt/media -> mnt-media.mount
    e.g., /mnt/media-original -> mnt-media\x2doriginal.mount
    """
    path_str = str(path).strip("/")
    if not path_str:
        return "-.mount"  # Root mount

    # systemd escapes '-' with \x2d
    escaped_path = path_str.replace("-", "\\x2d")

    # systemd replaces / with -
    unit = re.sub(r"/+", "-", escaped_path)
    return f"{unit}.mount"


class MountNode:
    """
    Represents a single mount point in a dependency tree.
    """

    def __init__(self, path: str):
        self.path = os.path.abspath(path)
        self.unit_name = path_to_unit_name(self.path)
        self.dependencies: Set["MountNode"] = set()
        self.dependants: Set["MountNode"] = set()
        self.is_locally_mounted = False

    def add_dependency(self, node: "MountNode"):
        self.dependencies.add(node)
        node.dependants.add(self)

    def refresh_self_health(self):
        """Checks if THIS path is currently mounted."""
        self.is_locally_mounted = os.path.ismount(self.path)
        return self.is_locally_mounted


class MountManager:
    """
    Manages filesystem mount points using a dependency tree.
    Handles checking, recovery via systemctl, and target-specific health checks.
    """

    def __init__(self, config: MountManagementConfig):
        self.config = config
        self.nodes: Dict[str, MountNode] = {}
        self._build_tree(config.mounts)

    def _build_tree(self, configs: List[Union[str, MountConfig]]):
        # 1. Create all nodes first
        for cfg in configs:
            path = cfg.path if isinstance(cfg, MountConfig) else cfg
            abs_path = os.path.abspath(path)
            if abs_path not in self.nodes:
                self.nodes[abs_path] = MountNode(abs_path)

        # 2. Link dependencies (Lineage + Explicit)
        for cfg in configs:
            path = cfg.path if isinstance(cfg, MountConfig) else cfg
            abs_path = os.path.abspath(path)
            node = self.nodes[abs_path]

            # Explicit dependencies
            if isinstance(cfg, MountConfig):
                for dep_path in cfg.dependencies:
                    abs_dep = os.path.abspath(dep_path)
                    if abs_dep in self.nodes:
                        node.add_dependency(self.nodes[abs_dep])
                    else:
                        logger.warning(f"Mount {abs_path} depends on {abs_dep} which is not managed by MountManager.")

            # Implicit lineage dependencies (parent paths)
            for other_path, other_node in self.nodes.items():
                if other_path == abs_path:
                    continue
                # If other_path is a parent of abs_path, it's a dependency
                if self._is_relative_to(abs_path, other_path):
                    node.add_dependency(other_node)

    async def refresh_and_recover(self):
        """
        Refreshes local mount status and attempts recovery in topological order.
        """
        # Update self-health for everyone
        for node in self.nodes.values():
            node.refresh_self_health()

        # Recovery attempt (Top-down)
        if self.config.recovery:
            to_recover = sorted(self.nodes.values(), key=lambda n: self._get_depth(n))

            for node in to_recover:
                # We recover if we are not mounted AND our lineage/dependencies are healthy
                if not node.is_locally_mounted:
                    # To check if we CAN recover this node, we check if all its dependencies are mounted.
                    # We use a simple check of is_locally_mounted for dependencies here
                    # because we are iterating in topological order.
                    if all(dep.is_locally_mounted for dep in node.dependencies):
                        logger.warning(f"Mount point {node.path} is unhealthy. Attempting recovery via systemctl...")
                        await self._try_mount(node)
                        node.refresh_self_health()

    def is_target_healthy(self, target_path: str) -> bool:
        """
        Determines if a specific path is healthy relative to the mount tree.
        A target is healthy if all related managed mounts (ancestors, descendants, dependencies) are mounted.
        """
        abs_target = os.path.abspath(target_path)

        # We must check all related managed mounts
        for mount_path, node in self.nodes.items():
            # If mount is an ancestor of target
            if self._is_relative_to(abs_target, mount_path):
                if not node.is_locally_mounted:
                    return False
                # If target is healthy, all its explicit dependencies must be healthy too
                if not self._check_dependencies_recursive(node, set()):
                    return False

            # If mount is a descendant of target
            if self._is_relative_to(mount_path, abs_target):
                if not node.is_locally_mounted:
                    return False

        return True

    def _check_dependencies_recursive(self, node: MountNode, visited: Set[MountNode]) -> bool:
        """Helper to verify recursive dependencies are mounted."""
        if node in visited:
            return True
        visited.add(node)

        if not node.is_locally_mounted:
            return False

        for dep in node.dependencies:
            if not self._check_dependencies_recursive(dep, visited):
                return False
        return True

    def _get_depth(self, node: MountNode, visited=None) -> int:
        if visited is None:
            visited = set()
        if node in visited:
            return 0
        visited.add(node)
        if not node.dependencies:
            return 0
        return 1 + max(self._get_depth(dep, visited) for dep in node.dependencies)

    async def _try_mount(self, node: MountNode):
        """Attempts to start the systemd mount unit."""
        try:
            cmd = ["systemctl", "start", node.unit_name]
            if self.config.sudo:
                cmd = ["sudo"] + cmd

            logger.info(f"Executing: {' '.join(cmd)}")
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await process.communicate()

            if process.returncode == 0:
                logger.info(f"Successfully started mount unit {node.unit_name} for {node.path}")
            else:
                logger.error(f"Failed to start mount unit {node.unit_name}: {stderr.decode().strip()}")
        except Exception as e:
            logger.error(f"Exception while attempting to mount {node.path}: {e}")

    def get_healthy_paths(self, configured_paths: Dict[str, str]) -> Dict[str, str]:
        """Filters path mappings based on context-aware health."""
        healthy_paths = {}

        for var_name, local_path in configured_paths.items():
            if self.is_target_healthy(local_path):
                healthy_paths[var_name] = local_path
            else:
                logger.warning(f"Pruning path variable '{var_name}' ({local_path}) due to unhealthy mount lineage.")

        return healthy_paths

    def _is_relative_to(self, path: str, base: str) -> bool:
        try:
            p = Path(path)
            b = Path(base)
            return p == b or b in p.parents
        except Exception:
            return False
