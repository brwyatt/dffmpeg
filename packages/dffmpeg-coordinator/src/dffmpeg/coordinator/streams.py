import asyncio
import logging
import os
import shutil
import time
from pathlib import Path
from typing import AsyncGenerator, Optional

from dffmpeg.common.models import EOFPayload

logger = logging.getLogger(__name__)


class StreamStorageManager:
    def __init__(self, storage_root: str):
        self.storage_root = Path(storage_root)

    def _get_stream_dir(self, job_id: str, stream_name: str) -> Path:
        # Resolve target path and verify target containment to prevent path traversal
        target = (self.storage_root / str(job_id) / stream_name).resolve()
        if not target.is_relative_to(self.storage_root.resolve()):
            raise ValueError(f"Invalid stream path resolution: {target}")
        return target

    def _get_pruned_bytes_path(self, stream_dir: Path) -> Path:
        return stream_dir / ".pruned_bytes"

    def get_pruned_bytes(self, job_id: str, stream_name: str) -> int:
        """
        Public method to retrieve pruned bytes total using job_id and stream_name.
        """
        return self._get_pruned_bytes_by_dir(self._get_stream_dir(job_id, stream_name))

    def _get_pruned_bytes_by_dir(self, stream_dir: Path) -> int:
        """
        Internal path-based helper (avoids redundant path reconstruction).
        """
        path = self._get_pruned_bytes_path(stream_dir)
        if not path.exists():
            return 0
        try:
            return int(path.read_text().strip())
        except Exception as e:
            logger.error(f"Failed to read .pruned_bytes at {path}: {e}")
            return 0

    def _set_pruned_bytes(self, stream_dir: Path, bytes_count: int) -> None:
        path = self._get_pruned_bytes_path(stream_dir)
        tmp_path = path.with_suffix(".tmp")
        try:
            tmp_path.write_text(str(bytes_count))
            tmp_path.replace(path)
        except Exception as e:
            logger.error(f"Failed to write .pruned_bytes at {path}: {e}")

    async def write_chunk(self, job_id: str, stream_name: str, seq: int, data: bytes) -> None:
        """
        Atomically writes a binary chunk for a specific job stream.
        """
        stream_dir = self._get_stream_dir(job_id, stream_name)
        # Ensure directories exist
        await asyncio.to_thread(os.makedirs, stream_dir, exist_ok=True)

        part_file = stream_dir / f"{seq:08d}.part"
        chunk_file = stream_dir / f"{seq:08d}.chunk"

        def _write_atomic():
            part_file.write_bytes(data)
            part_file.rename(chunk_file)

        await asyncio.to_thread(_write_atomic)
        logger.debug(f"Wrote chunk {seq:08d} for job {job_id}/{stream_name} (size={len(data)}B)")

    async def write_eof(self, job_id: str, stream_name: str, payload: EOFPayload) -> None:
        """
        Writes the EOF marker containing final stream metadata using the shared EOFPayload model.
        """
        stream_dir = self._get_stream_dir(job_id, stream_name)
        await asyncio.to_thread(os.makedirs, stream_dir, exist_ok=True)

        eof_file = stream_dir / "EOF"

        def _write():
            eof_file.write_text(payload.model_dump_json())

        await asyncio.to_thread(_write)
        logger.debug(f"Wrote EOF marker for job {job_id}/{stream_name}: {payload}")

    async def prune_progress(
        self, job_id: str, stream_name: str, bytes_read: int, safety_margin_bytes: int = 10 * 1024 * 1024
    ) -> None:
        """
        Sliding-window pruning based on client bytes_read reports.
        Deletes sequential chunks up to (bytes_read - safety_margin_bytes).
        """
        stream_dir = self._get_stream_dir(job_id, stream_name)
        if not stream_dir.exists():
            return

        def _prune():
            pruned_bytes = self._get_pruned_bytes_by_dir(stream_dir)
            target_prune_limit = bytes_read - safety_margin_bytes
            if target_prune_limit <= pruned_bytes:
                return

            # Read all available chunks and sort them
            chunk_files = sorted(stream_dir.glob("*.chunk"), key=lambda p: p.name)

            current_accumulated = pruned_bytes
            for chunk_file in chunk_files:
                try:
                    chunk_size = chunk_file.stat().st_size
                except FileNotFoundError:
                    continue

                if current_accumulated + chunk_size <= target_prune_limit:
                    try:
                        chunk_file.unlink()
                        current_accumulated += chunk_size
                        logger.info(f"Pruned chunk {chunk_file.name} for job {job_id}/{stream_name}")
                    except FileNotFoundError:
                        pass
                    except Exception as e:
                        logger.error(f"Error unlinking chunk {chunk_file.name}: {e}")
                        break
                else:
                    break

            if current_accumulated > pruned_bytes:
                self._set_pruned_bytes(stream_dir, current_accumulated)

        await asyncio.to_thread(_prune)

    async def stream_chunks(self, job_id: str, stream_name: str, start_offset: int = 0) -> AsyncGenerator[bytes, None]:
        """
        Sequential chunk streaming generator supporting offset resume and filesystem polling.
        """
        stream_dir = self._get_stream_dir(job_id, stream_name)

        # Wait for the stream directory to be created if not exists
        while not stream_dir.exists():
            # Check if job is terminal? In routes, we will check if job is finished and directory doesn't exist.
            await asyncio.sleep(0.1)

        # Read pruned bytes to understand the baseline
        pruned_bytes = await asyncio.to_thread(self._get_pruned_bytes_by_dir, stream_dir)
        if start_offset < pruned_bytes:
            raise ValueError(
                f"Requested start_offset {start_offset} is less than pruned baseline of {pruned_bytes} bytes "
                "(data no longer available on disk)."
            )
        cumulative_bytes = pruned_bytes

        # Fast scan to find starting sequence
        def _get_start_seq():
            chunk_files = sorted(stream_dir.glob("*.chunk"))
            if not chunk_files:
                return 0, pruned_bytes

            # Walk through available chunks to find where start_offset fits
            running_bytes = pruned_bytes
            for cf in chunk_files:
                try:
                    size = cf.stat().st_size
                    # Check if start_offset lies inside this chunk
                    if running_bytes + size > start_offset:
                        try:
                            seq = int(cf.name.split(".")[0])
                            return seq, running_bytes
                        except ValueError:
                            pass
                    running_bytes += size
                except FileNotFoundError:
                    pass

            # If start_offset is beyond all currently written chunks, start from the latest sequence
            if chunk_files:
                try:
                    latest_seq = int(chunk_files[-1].name.split(".")[0])
                    return latest_seq, running_bytes - chunk_files[-1].stat().st_size
                except Exception:
                    pass
            return 0, pruned_bytes

        current_seq, cumulative_bytes = await asyncio.to_thread(_get_start_seq)
        logger.debug(f"Starting chunk stream for {job_id}/{stream_name} at seq={current_seq}, offset={start_offset}")

        while True:
            chunk_file = stream_dir / f"{current_seq:08d}.chunk"
            eof_file = stream_dir / "EOF"

            # Check if chunk file exists
            if await asyncio.to_thread(chunk_file.exists):

                def _read_chunk():
                    return chunk_file.read_bytes()

                data = await asyncio.to_thread(_read_chunk)
                chunk_len = len(data)

                # Slice data if we are resuming within this chunk
                if start_offset > cumulative_bytes:
                    slice_start = start_offset - cumulative_bytes
                    if slice_start < chunk_len:
                        yield data[slice_start:]
                else:
                    yield data

                cumulative_bytes += chunk_len
                current_seq += 1
                continue

            # If chunk file does not exist, check if a part file is currently being written
            # Or if the EOF exists and we've processed all sequential chunks
            if await asyncio.to_thread(eof_file.exists):

                def _read_eof_meta() -> Optional[EOFPayload]:
                    try:
                        return EOFPayload.model_validate_json(eof_file.read_text())
                    except Exception:
                        return None

                eof_meta = await asyncio.to_thread(_read_eof_meta)
                if eof_meta:
                    final_seq = eof_meta.final_sequence
                    if (
                        eof_meta.total_bytes == 0
                        or final_seq is None
                        or final_seq == -1
                        or current_seq > final_seq
                        or cumulative_bytes >= eof_meta.total_bytes
                    ):
                        logger.debug(f"Stream {job_id}/{stream_name} completed EOF at seq {final_seq}")
                        break

            # Sleep briefly and poll again
            await asyncio.sleep(0.1)

    async def clean_stale_streams(self, active_job_ids: set[str], retention_minutes: int) -> None:
        """
        Janitor sweep: Deletes job stream directories that are stale or orphaned.
        """
        if not self.storage_root.exists():
            return

        def _sweep():
            now = time.time()
            retention_seconds = retention_minutes * 60

            # Scan the storage root directory
            try:
                for job_dir in self.storage_root.iterdir():
                    if not job_dir.is_dir():
                        continue

                    job_id = job_dir.name
                    # Orphan check: job_id is not in active_job_ids
                    # Stale check: last modified time of directory is older than retention window
                    is_active = job_id in active_job_ids

                    # Get modification time of directory, walking child files to find the latest update
                    try:

                        def _get_latest_mtime(p: Path) -> float:
                            latest = p.stat().st_mtime
                            try:
                                for entry in p.rglob("*"):
                                    try:
                                        latest = max(latest, entry.stat().st_mtime)
                                    except Exception:
                                        pass
                            except Exception:
                                pass
                            return latest

                        mtime = _get_latest_mtime(job_dir)
                    except Exception:
                        mtime = now

                    if not is_active:
                        # Orphaned job stream folder or finished job
                        if now - mtime > retention_seconds:
                            logger.info(f"Janitor unlinking expired/orphaned stream dir: {job_dir}")
                            shutil.rmtree(job_dir, ignore_errors=True)
            except Exception as e:
                logger.error(f"Error during clean_stale_streams sweep: {e}")

        await asyncio.to_thread(_sweep)

    async def delete_stream(self, job_id: str, stream_name: str) -> None:
        """
        Explicitly deletes a specific stream's directory immediately (e.g. upon client ACK).
        If the job's parent directory becomes completely empty, cleans that up too.
        """
        stream_dir = self._get_stream_dir(job_id, stream_name)

        def _delete():
            if stream_dir.exists():
                shutil.rmtree(stream_dir, ignore_errors=True)
                logger.info(f"Successfully deleted stream dir immediately: {stream_dir}")

            parent_dir = stream_dir.parent
            try:
                if parent_dir.exists() and not any(parent_dir.iterdir()):
                    parent_dir.rmdir()
                    logger.info(f"Cleaned up empty job parent streams dir: {parent_dir}")
            except Exception:
                pass

        await asyncio.to_thread(_delete)
