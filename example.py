import asyncio
import uring_file
import liburing
import os


async def test_read_write():
    # Asynchronous file I/O APIs

    f = uring_file.TextFile('hello.txt')
    await f.open("w")
    await f.write('hello\nworld\n')
    await f.close()

    async with uring_file.open('hello.txt', "a") as f:
        await f.write('hello\nworld\n')

    async with uring_file.open('hello.txt', "ab") as f:
        await f.write(b'hello\nworld\n')

    async with uring_file.open('hello2.txt', "w") as f:
        async with f.start_async_write():
            for _ in range(10):
                f.write_nowait('hello\nworld\n')

    async with uring_file.open('hello.txt', "r") as f:
        async for line in f:
            print(line, end="")

    # Low level Uring APIs

    # Create new Uring
    uring = uring_file.Uring(sq_size=8, cq_size=64, session_sq_size=4)

    # Get SQE and submit (Open file)
    async with uring.session() as session:
        sqe = session.get_sqe()
        sqe.prep_openat(liburing.AT_FDCWD, b"hello.txt", os.O_RDONLY, 0o644)

    # Get result of CQE
    fd = sqe.result()
    print("FD:", fd)

    # Close file
    async with uring.session() as session:
        # Raw SQE object can be accessed from the _sqe property
        liburing.io_uring_prep_close(session.get_sqe()._sqe, fd)
        # Same expression as session.get_sqe().prep_close(fd)

    # Get default Uring
    default_uring = uring_file.get_default_uring()


if __name__ == '__main__':
    asyncio.run(test_read_write())
