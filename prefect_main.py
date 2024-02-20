import asyncio

from prefect import deploy, flow, task
from prefect.filesystems import LocalFileSystem


@flow(
    persist_result=True,
    result_storage=LocalFileSystem(basepath=".my-results"),
    result_serializer="json",
)
async def say_hello_flow(name: str) -> str:
    return f"Hello {name}"


@task
async def sum_values_task(values: list[int | float]) -> float:
    return float(sum(values))


@flow(
    persist_result=True,
    result_storage=LocalFileSystem(basepath=".my-results"),
    result_serializer="json",
)
async def sum_values_flow(values: list[int | float]) -> float:
    tasks = []
    for chunk in to_chunks(values, 3):
        tasks.append((await sum_values_task.submit(chunk)).result())
    return sum(await asyncio.gather(*tasks))


def to_chunks(ls: list, n: int):
    for i in range(0, len(ls), n):
        yield ls[i : i + n]


async def main():
    await deploy(
        await say_hello_flow.from_source(
            source="https://github.com/SquakR/prefect-test.git",
            entrypoint="prefect_main.py",
        ).to_deployment(name="say_hello"),
        await sum_values_flow.from_source(
            source="https://github.com/SquakR/prefect-test.git",
            entrypoint="prefect_main.py",
        ).to_deployment(name="process-pool"),
        work_pool_name="process-pool",
        push=False,
    )


if __name__ == "__main__":
    asyncio.run(main())
