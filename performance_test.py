# pip install anyio aiofile aiofiles
import asyncio
import random
from aiofile import async_open
import aiofiles
import anyio
import time
from typing import Callable, Coroutine
import uring_file
from contextlib import contextmanager


@contextmanager
def meas_time(name: str):
    start = time.perf_counter()
    start_process = time.process_time()
    try:
        yield
    finally:
        print(f"{name}:", time.perf_counter() - start, "[s]")
        print(f"{name}(Process):", time.process_time() - start_process, "[s]")


async def io_test(func_write: Callable[[str], Coroutine], func_read: Callable[[str], Coroutine], test_dir: str, file_num: int):
    await anyio.Path(test_dir).mkdir(exist_ok=True)

    print("2 seconds sleep...\n")
    await asyncio.sleep(2)
    print("start write test")
    with meas_time(func_write.__name__):
        await asyncio.gather(
            *[func_write(f"{test_dir}/{str(i)}.txt") for i in range(file_num)]
        )
    print("end write test\n")

    print("2 seconds sleep...\n")
    await asyncio.sleep(2)
    print("start read test")
    with meas_time(func_read.__name__):
        await asyncio.gather(
            *[func_read(f"{test_dir}/{str(i)}.txt") for i in range(file_num)]
        )
    print("end read test\n")

    print("deleting files...")
    await asyncio.gather(
        *[anyio.Path(f"{test_dir}/{str(i)}.txt").unlink(missing_ok=True) for i in range(file_num)]
    )
    print("deleted\n")


async def run_performance_test():
    line_num = 100
    file_num = 100
    write_data = 'helloworldhelloworldhelloworldhelloworldhelloworldhelloworldhelloworld\n'

    async def random_wait():
        await asyncio.sleep(random.uniform(0, 0.001))

    # anyio
    async def anyio_write(filen_name: str):
        await random_wait()
        async with await anyio.open_file(filen_name, "w") as f:
            for _ in range(line_num):
                await random_wait()
                await f.write(write_data)

    async def anyio_read(filen_name: str):
        await random_wait()
        async with await anyio.open_file(filen_name, "r") as f:
            await f.read()

    await io_test(anyio_write, anyio_read, "test_files", file_num)

    # aiofiles
    async def aiofiles_write(filen_name: str):
        await random_wait()
        async with aiofiles.open(filen_name, "w") as f:
            for _ in range(line_num):
                await random_wait()
                await f.write(write_data)

    async def aiofiles_read(filen_name: str):
        await random_wait()
        async with aiofiles.open(filen_name, "r") as f:
            await f.read()

    await io_test(aiofiles_write, aiofiles_read, "test_files", file_num)

    # aiofile
    async def aiofile_write(filen_name: str):
        await random_wait()
        async with async_open(filen_name, "w") as f:
            for _ in range(line_num):
                await random_wait()
                await f.write(write_data)

    async def aiofile_read(filen_name: str):
        await random_wait()
        async with async_open(filen_name, "r") as f:
            await f.read()

    await io_test(aiofile_write, aiofile_read, "test_files", file_num)

    # uring_file
    async def uring_file_write(filen_name: str):
        await random_wait()
        async with uring_file.open(filen_name, "w") as f:
            for _ in range(line_num):
                await random_wait()
                await f.write(write_data)

    async def uring_file_read(filen_name: str):
        await random_wait()
        async with uring_file.open(filen_name, "r") as f:
            await f.read()

    await io_test(uring_file_write, uring_file_read, "test_files", file_num)


if __name__ == '__main__':
    asyncio.run(run_performance_test())
