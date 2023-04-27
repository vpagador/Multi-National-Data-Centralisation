import requests
import time
import asyncio


async def counter(until:int = 10) -> None:
    now = time.perf_counter()
    print("started counter")
    for i in range(0, until):
        last = now
        await asyncio.sleep(0.01)
        now = time.perf_counter()
        print(f"{i}: was asleep for {now - last}")

def send_request(url:str) -> int:
    print('sending http request')
    response = requests.get(url)
    return response.status_code

async def send_async_request(url:str) -> int:
    return await asyncio.to_thread(send_request, url)

async def main() -> None:
    status_code, _ = await asyncio.gather(
        send_async_request("https://github.com/vpagador"), counter()
    )

