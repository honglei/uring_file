### Requires

- python 3.8+
- linux kernel 5.7+

### Usage

```python
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
```

### Links

- [Python wrapper](https://github.com/YoSTEALTH/Liburing) for liburing
- [Lord of the io_uring](https://unixism.net/loti/) tutorial
