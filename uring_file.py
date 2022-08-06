from __future__ import annotations
import ctypes
import asyncio
import atexit
from contextlib import asynccontextmanager
import os
from typing import Iterable, Literal, Optional, AsyncGenerator, Union, overload
import liburing


libc = ctypes.CDLL(None)

# classes for type hinting


class UringType:
    flags: int
    ring_fd: int


class SQEType:
    opcode: int
    flags: int
    ioprio: int
    fd: int
    user_data: int


class CQEType:
    user_data: int
    res: int
    flags: int


class TimeStampType:
    tv_sec: int
    tv_nsec: int


class StatXType:
    stx_mask: int
    stx_blksize: int
    stx_attributes: int
    stx_nlink: int
    stx_uid: int
    stx_gid: int
    stx_mode: int
    stx_ino: int
    stx_size: int
    stx_blocks: int
    stx_attributes_mask: int
    stx_atime: TimeStampType
    stx_btime: TimeStampType
    stx_ctime: TimeStampType
    stx_mtime: TimeStampType
    stx_rdev_major: int
    stx_rdev_minor: int


class NoSubmitQueueSpaceError(Exception):
    pass


class Uring:
    def __init__(self, sq_size: int = 128, cq_size: int = 256, session_sq_size: int = 4):
        self._uring: UringType = liburing.io_uring()
        self._eventfd: int = libc.eventfd(0, os.O_NONBLOCK | os.O_CLOEXEC)
        self._waiting_sqes: dict[int, UringSQE] = {}
        self._sq_size: int = sq_size
        self._cq_size: int = cq_size
        self._session_sq_size: int = session_sq_size
        self._setup_done: bool = False
        self._loop: asyncio.AbstractEventLoop
        self._lock: asyncio.Lock = asyncio.Lock()
        self._waiting_sessions: int = 0
        # Prevent error "Device or resource busy"
        self._sem: asyncio.Semaphore = asyncio.Semaphore(self._cq_size)

    def setup(self):
        if self._setup_done is False:
            params = liburing.io_uring_params()
            params.cq_entries = self._cq_size
            params.flags = liburing.IORING_SETUP_CQSIZE
            liburing.io_uring_queue_init_params(
                self._sq_size, self._uring, params
            )
            liburing.io_uring_register_eventfd(self._uring, self._eventfd)
            self._loop = asyncio.get_event_loop()
            self._loop.add_reader(self._eventfd, self._eventfd_callback)
            atexit.register(self._cleanup)
            self._setup_done = True

    def _cleanup(self):
        liburing.io_uring_unregister_eventfd(self._uring)
        liburing.io_uring_queue_exit(self._uring)
        self._loop.remove_reader(self._eventfd)
        self._setup_done = False

    def _eventfd_callback(self):
        libc.eventfd_read(self._eventfd, os.O_NONBLOCK | os.O_CLOEXEC)
        cqes: list[CQEType] = liburing.io_uring_cqes()
        while True:
            try:
                liburing.io_uring_peek_cqe(self._uring, cqes)
            except BlockingIOError:
                break
            cqe: CQEType = cqes[0]
            liburing.io_uring_cqe_seen(self._uring, cqe)
            self._waiting_sqes[cqe.user_data].set_result(cqe.res)
            del self._waiting_sqes[cqe.user_data]
            self._sem.release()

    async def aquire_lock(self):
        self._waiting_sessions += 1
        try:
            await self._lock.acquire()
        finally:
            self._waiting_sessions -= 1

    def release_lock(self):
        self._lock.release()

    def sq_space_left(self) -> int:
        return liburing.io_uring_sq_space_left(self._uring)

    def _get_sqe(self) -> UringSQE:
        if self.sq_space_left() == 0:
            raise NoSubmitQueueSpaceError
        _sqe: SQEType = liburing.io_uring_get_sqe(self._uring)
        return UringSQE(_sqe)

    async def submit(self, sqes: Iterable[UringSQE]):
        for sqe in sqes:
            await self._sem.acquire()
            sqe._sqe.user_data = id(sqe._future)
            self._waiting_sqes[sqe._sqe.user_data] = sqe
        if self.sq_space_left() < self._session_sq_size or self._waiting_sessions == 0:
            liburing.io_uring_submit(self._uring)
            await asyncio.sleep(0)  # Update self._waiting_sessions

    def session(self):
        return UringSession(self)


class UringSQE:
    def __init__(self, sqe: SQEType) -> None:
        self._sqe: SQEType = sqe
        self._future: asyncio.Future[int] = asyncio.Future()

    def set_result(self, data: int):
        self._future.set_result(data)

    def result(self) -> int:
        return self._future.result()

    async def wait(self) -> int:
        return liburing.trap_error(await self._future)

    def link(self):
        self._sqe.flags |= liburing.IOSQE_IO_LINK
        return self

    def fixed_file(self):
        self._sqe.flags |= liburing.IOSQE_FIXED_FILE
        return self

    def prep_openat(self, dir_fd: int, path: bytes, flags: int, mode: int):
        liburing.io_uring_prep_openat(
            self._sqe, dir_fd, path, flags, mode
        )
        return self

    def prep_close(self, fd: int):
        liburing.io_uring_prep_close(self._sqe, fd)
        return self

    def prep_readv(self, fd: int, iovecs, nr_vecs: int, offset: int):
        liburing.io_uring_prep_readv(
            self._sqe, fd, iovecs, nr_vecs, offset
        )
        return self

    def prep_writev(self, fd: int, iovecs, nr_vecs: int, offset: int):
        liburing.io_uring_prep_writev(
            self._sqe, fd, iovecs, nr_vecs, offset
        )
        return self

    def prep_write(self, fd: int, buf, nbytes: int, offset: int):
        liburing.io_uring_prep_write(
            self._sqe, fd, buf, nbytes, offset
        )
        return self

    def prep_statx(self, fd: int, path: bytes, flags: int, mask: int, statx_buf: list[StatXType]):
        liburing.io_uring_prep_statx(
            self._sqe, fd, path, flags, mask, statx_buf
        )
        return self


_DEFAULT_URING: Uring = Uring()


def set_default_uring(uring: Uring):
    global _DEFAULT_URING
    _DEFAULT_URING = uring


def get_default_uring():
    global _DEFAULT_URING
    return _DEFAULT_URING


class UringSession:
    def __init__(self, uring: Optional[Uring] = None):
        if uring is None:
            uring = get_default_uring()
        uring.setup()
        self._uring: Uring = uring
        self._sqes: set[UringSQE] = set()
        self._closed: bool = True

    async def __aenter__(self):
        return await self.open()

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()

    async def open(self) -> UringSession:
        await self._uring.aquire_lock()
        self._closed = False
        self._sqes.clear()
        return self

    async def close(self):
        self._closed = True
        await self._uring.submit(self._sqes)
        self._uring.release_lock()
        [await sqe.wait() for sqe in self._sqes]

    def get_sqe(self) -> UringSQE:
        assert self._closed is False
        sqe = self._uring._get_sqe()
        self._sqes.add(sqe)
        return sqe


class UringFile:
    def __init__(self, path: Union[os.PathLike, str], uring: Optional[Uring] = None):
        if uring is None:
            uring = get_default_uring()
        uring.setup()
        self._uring: Uring = uring
        self._path: str = os.path.normpath(path)
        self._fd: Optional[int] = None
        self._offset: int = 0
        self._closed: bool = True
        self._workers: set[asyncio.Future] = set()
        self._worker_closed: bool = True

    async def __aenter__(self):
        assert self._closed is False
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()

    async def open(self, flags: int = os.O_RDONLY, mode: int = 0o644) -> UringFile:
        assert self._fd is None
        assert self._closed is True

        path: bytes = self._path.encode()
        async with self._uring.session() as session:
            sqe = session.get_sqe()
            sqe.prep_openat(liburing.AT_FDCWD, path, flags, mode)
        self._fd = sqe.result()
        self._closed = False
        return self

    async def close(self):
        assert self._fd is not None
        if self._closed is False:
            self._closed = True
            await self.close_workers()
            async with self._uring.session() as session:
                assert self._fd is not None
                session.get_sqe().prep_close(self._fd)
            self._fd = None
            self._offset = 0

    async def stat(self) -> StatXType:
        assert self._fd is not None
        stxs: list[StatXType] = liburing.statx()
        async with self._uring.session() as session:
            session.get_sqe().prep_statx(
                self._fd, "".encode(), liburing.AT_EMPTY_PATH | liburing.AT_SYMLINK_NOFOLLOW,
                liburing.STATX_ALL, stxs
            )
        return stxs[0]

    async def read(self, size: Optional[int] = None) -> bytes:
        assert self._fd is not None
        if size is None:
            size = (await self.stat()).stx_size
        array = bytearray(size)
        iovecs = liburing.iovec(array)
        async with self._uring.session() as session:
            sqe = session.get_sqe()
            sqe.prep_readv(self._fd, iovecs, len(iovecs), self._offset)
        self._offset += sqe.result()
        return bytes(array[:sqe.result()])

    async def readline(self) -> bytes:
        line = bytearray()
        while (chunk := await self.read(32)):
            if (nl := chunk.find(b'\n')) != -1:
                line += chunk[:nl+1]
                self._offset += nl + 1 - len(chunk)
                break
            else:
                line += chunk
        return bytes(line)

    async def __aiter__(self):
        while line := await self.readline():
            yield line

    async def write(self, data: bytes):
        assert self._fd is not None
        iov = liburing.iovec(data)
        async with self._uring.session() as session:
            sqe = session.get_sqe()
            sqe.prep_writev(
                self._fd, iov, len(iov), self._offset
            )
        self._offset += sqe.result()

    @asynccontextmanager
    async def start_async_write(self):
        self.open_workers()
        try:
            yield self
        finally:
            await self.close_workers()

    def open_workers(self):
        self._worker_closed = False

    async def close_workers(self):
        if self._worker_closed is False:
            self._worker_closed = True
            [await worker for worker in self._workers.copy()]

    def add_worker(self, task: asyncio.Future):
        self._workers.add(task)

        async def close_worker():
            try:
                await task
            finally:
                self._workers.discard(task)

        asyncio.create_task(close_worker())

    def write_nowait(self, data: bytes):
        assert self._worker_closed is False

        async def task_func():
            assert self._fd is not None
            iovecs = liburing.iovec(data)
            offset = self._offset
            self._offset += len(data)
            async with self._uring.session() as session:
                session.get_sqe().prep_writev(
                    self._fd, iovecs, len(iovecs), offset
                )
        self.add_worker(asyncio.create_task(task_func()))

    def seek(self, offset: int):
        self._offset = offset

    def fileno(self) -> Optional[int]:
        return self._fd


def get_flags(mode: str):
    flags = os.O_RDONLY
    if "w" in mode or "a" in mode:
        flags = os.O_CREAT
        if "+" in mode:
            flags |= os.O_RDWR
        else:
            flags |= os.O_WRONLY
        if "a" in mode:
            flags |= os.O_APPEND
    elif "r" in mode and "+" in mode:
        flags = os.O_RDWR
    return flags


class TextFile:
    def __init__(self, path: Union[os.PathLike, str]) -> None:
        self.uring_file = UringFile(path)

    async def open(self, mode: Literal["r", "r+", "w", "w+", "a", "a+"]):
        await self.uring_file.open(get_flags(mode))
        return self

    async def close(self):
        await self.uring_file.close()

    async def stat(self):
        return await self.uring_file.stat()

    async def read(self, size: Optional[int] = None) -> str:
        return (await self.uring_file.read(size)).decode()

    async def readline(self) -> str:
        return (await self.uring_file.readline()).decode()

    async def __aiter__(self):
        while line := await self.readline():
            yield line

    async def write(self, data: str):
        await self.uring_file.write(data.encode())

    def start_async_write(self):
        return self.uring_file.start_async_write()

    def write_nowait(self, data: str):
        self.uring_file.write_nowait(data.encode())

    def seek(self, offset):
        self.uring_file.seek(offset)

    def fileno(self):
        return self.uring_file.fileno()


class BinaryFile:
    def __init__(self, path: Union[os.PathLike, str]) -> None:
        self.uring_file = UringFile(path)

    async def open(self, mode: Literal["rb", "rb+", "wb", "wb+", "ab", "ab+"]):
        await self.uring_file.open(get_flags(mode))
        return self

    async def close(self):
        await self.uring_file.close()

    async def stat(self):
        return await self.uring_file.stat()

    async def read(self, size: Optional[int] = None) -> bytes:
        return await self.uring_file.read(size)

    async def readline(self) -> bytes:
        return await self.uring_file.readline()

    async def __aiter__(self):
        while line := await self.readline():
            yield line

    async def write(self, data: bytes):
        await self.uring_file.write(data)

    def start_async_write(self):
        return self.uring_file.start_async_write()

    def write_nowait(self, data: bytes):
        self.uring_file.write_nowait(data)

    def seek(self, offset):
        self.uring_file.seek(offset)

    def fileno(self) -> Optional[int]:
        return self.uring_file.fileno()


@overload
@asynccontextmanager
async def open(path: Union[str, os.PathLike], mode: Literal["r", "r+", "w", "w+", "a", "a+"] = "r") -> AsyncGenerator[TextFile, None]:
    ...


@overload
@asynccontextmanager
async def open(path: Union[str, os.PathLike], mode: Literal["rb", "rb+", "wb", "wb+", "ab", "ab+"]) -> AsyncGenerator[BinaryFile, None]:
    ...


@asynccontextmanager
async def open(path: Union[str, os.PathLike], mode: Literal["r", "r+", "w", "w+", "a", "a+", "rb", "rb+", "wb", "wb+", "ab", "ab+"] = "r"):
    if "b" in mode:
        file = BinaryFile(path)
    else:
        file = TextFile(path)
    try:
        yield await file.open(mode)
    finally:
        await file.close()
