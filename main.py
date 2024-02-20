from fastapi import FastAPI
from prefect.client.orchestration import get_client
from prefect.client.schemas.filters import WorkPoolFilter, WorkPoolFilterName
from prefect.deployments import run_deployment
from prefect_main import say_hello_flow

app = FastAPI()


@app.get("/hello")
async def hello(name: str) -> str:
    result = await run_deployment(
        name="say-hello-flow/say_hello", parameters={"name": name}
    )
    return await result.state.result().get()


@app.post("/deploy")
async def deploy(name: str) -> None:
    await say_hello_flow.from_source(
        source="https://github.com/SquakR/prefect-test.git",
        entrypoint="prefect_main.py",
    ).deploy(
        name="dynamic_deploy",
        work_pool_name="process-pool",
        parameters={"name": name},
        cron="* * * * *",
    )


@app.post("/delete_deployments")
async def delete_deployments() -> None:
    async with get_client() as client:
        deployments = await client.read_deployments(
            work_pool_filter=WorkPoolFilter(
                name=WorkPoolFilterName(any_=["docker-pool"])
            )
        )
        for d in deployments:
            await client.delete_deployment(d.id)


@app.post("/sum")
async def sum(values: list[int]) -> float:
    result = await run_deployment(
        name="sum-values-flow/sum_values", parameters={"values": values}
    )
    return await result.state.result().get()
