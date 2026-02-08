import asyncio
import os
from unittest.mock import MagicMock, patch

import pytest

from dffmpeg.worker.config import MountConfig, MountManagementConfig
from dffmpeg.worker.mounts import MountManager, path_to_unit_name


def test_path_to_unit_name():
    assert path_to_unit_name("/mnt/media") == "mnt-media.mount"
    assert path_to_unit_name("/mnt/data/media") == "mnt-data-media.mount"
    assert path_to_unit_name("/") == "-.mount"
    # Hyphen escaping
    assert path_to_unit_name("/mnt/media-original") == "mnt-media\\x2doriginal.mount"


@pytest.fixture
def mount_manager():
    # Setup a tree:
    # /mnt/nas (Root)
    # /mnt/nas/media/tv (Child 1)
    # /mnt/nas/media/movies (Child 2)
    # /mnt/bind (Explicit depends on /mnt/nas/media/movies)
    configs = [
        "/mnt/nas",
        "/mnt/nas/media/tv",
        "/mnt/nas/media/movies",
        MountConfig(path="/mnt/bind", dependencies=["/mnt/nas/media/movies"]),
    ]
    mgmt_cfg = MountManagementConfig(mounts=configs)
    return MountManager(mgmt_cfg)


@pytest.mark.asyncio
async def test_tree_structure(mount_manager):
    nas = mount_manager.nodes[os.path.abspath("/mnt/nas")]
    tv = mount_manager.nodes[os.path.abspath("/mnt/nas/media/tv")]
    movies = mount_manager.nodes[os.path.abspath("/mnt/nas/media/movies")]
    bind = mount_manager.nodes[os.path.abspath("/mnt/bind")]

    assert nas in tv.dependencies
    assert nas in movies.dependencies
    assert movies in bind.dependencies
    assert bind in movies.dependants


@pytest.mark.asyncio
async def test_contextual_health_isolation(mount_manager):
    nas = mount_manager.nodes[os.path.abspath("/mnt/nas")]
    tv = mount_manager.nodes[os.path.abspath("/mnt/nas/media/tv")]
    movies = mount_manager.nodes[os.path.abspath("/mnt/nas/media/movies")]
    bind = mount_manager.nodes[os.path.abspath("/mnt/bind")]

    nas.is_locally_mounted = True
    movies.is_locally_mounted = True
    bind.is_locally_mounted = True
    tv.is_locally_mounted = False

    assert mount_manager.is_target_healthy("/mnt/nas/media/movies") is True
    assert mount_manager.is_target_healthy("/mnt/bind") is True
    assert mount_manager.is_target_healthy("/mnt/nas/media/tv") is False
    assert mount_manager.is_target_healthy("/mnt/nas") is False


@pytest.mark.asyncio
async def test_ordered_recovery(mount_manager):
    nas = mount_manager.nodes[os.path.abspath("/mnt/nas")]
    movies = mount_manager.nodes[os.path.abspath("/mnt/nas/media/movies")]
    tv = mount_manager.nodes[os.path.abspath("/mnt/nas/media/tv")]

    with patch("os.path.ismount", return_value=False) as mock_ismount:
        with patch.object(mount_manager, "_try_mount", return_value=None) as mock_try_mount:

            async def fake_try_mount(node):
                node.is_locally_mounted = True

            mock_try_mount.side_effect = fake_try_mount

            def ismount_mock(path):
                abs_p = os.path.abspath(path)
                if abs_p in mount_manager.nodes:
                    return mount_manager.nodes[abs_p].is_locally_mounted
                return False

            mock_ismount.side_effect = ismount_mock

            await mount_manager.refresh_and_recover()

            assert mock_try_mount.call_count >= 3
            calls = [call.args[0].path for call in mock_try_mount.call_args_list]
            assert nas.path in calls
            assert movies.path in calls
            assert tv.path in calls
            assert calls.index(nas.path) < calls.index(movies.path)


@pytest.mark.asyncio
async def test_recovery_disabled():
    mgmt_cfg = MountManagementConfig(mounts=["/mnt/test"], recovery=False)
    mm = MountManager(mgmt_cfg)

    with patch("os.path.ismount", return_value=False):
        with patch.object(mm, "_try_mount", return_value=None) as mock_try_mount:
            await mm.refresh_and_recover()
            # Recovery is disabled, should NOT call _try_mount
            assert mock_try_mount.call_count == 0


@pytest.mark.asyncio
async def test_sudo_recovery():
    mgmt_cfg = MountManagementConfig(mounts=["/mnt/test"], sudo=True)
    mm = MountManager(mgmt_cfg)
    node = mm.nodes[os.path.abspath("/mnt/test")]

    with patch("asyncio.create_subprocess_exec") as mock_exec:
        mock_process = MagicMock()
        mock_process.communicate.return_value = (b"", b"")
        mock_process.returncode = 0
        mock_exec.return_value = mock_process

        await mm._try_mount(node)

        mock_exec.assert_called_once_with(
            "sudo",
            "systemctl",
            "start",
            "mnt-test.mount",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

    @pytest.mark.asyncio
    async def test_pruning_contextual(mount_manager):
        tv = mount_manager.nodes[os.path.abspath("/mnt/nas/media/tv")]
        nas = mount_manager.nodes[os.path.abspath("/mnt/nas")]
        movies = mount_manager.nodes[os.path.abspath("/mnt/nas/media/movies")]
        bind = mount_manager.nodes[os.path.abspath("/mnt/bind")]

        # Everything but TV is mounted
        nas.is_locally_mounted = True
        tv.is_locally_mounted = False
        movies.is_locally_mounted = True
        bind.is_locally_mounted = True

        configured_paths = {
            "Movies": "/mnt/nas/media/movies",  # Healthy (lineage is NAS->Movies)
            "TV": "/mnt/nas/media/tv",  # Unhealthy
            "NAS": "/mnt/nas",  # Unhealthy (incomplete view due to broken descendant TV)
        }

        healthy = mount_manager.get_healthy_paths(configured_paths)
        assert "Movies" in healthy
        assert "TV" not in healthy
        assert "NAS" not in healthy
