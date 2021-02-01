import os

from prefect import task, Flow, Parameter
from prefect.tasks.prefect.flow_run import StartFlowRun
from prefect.tasks.control_flow.conditional import switch
from prefect.storage import GitHub
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import DockerRun


@task
def run_locally_or_in_cloud(input_string, manual_switch):
    if manual_switch:
        return manual_switch

    length = len(input_string)

    if length > 10:
        return "local"

    return "cloud"


cloud = StartFlowRun(
    flow_name="ETL - Docker",
    project_name="PGR Examples",
)
local = StartFlowRun(
    flow_name="ETL - Local",
    project_name="PGR Examples",
)

with Flow(
    "Orchestrator Flow",
    storage=GitHub(
        repo="dylanbhughes/pgr_examples_3",
        path="orchestrator.py",
        secrets=["GITHUB_ACCESS_TOKEN"],
    ),
    run_config=DockerRun(image="prefecthq/prefect:latest", labels=["pgr docker"]),
    executor=LocalDaskExecutor(scheduler="threads", num_workers=3),
) as flow:
    input_string = Parameter(name="input_string", required=True)
    manual_switch = Parameter(name="cloud_or_local", required=False, default=None)
    cloud_or_local_result = run_locally_or_in_cloud(
        input_string=input_string, manual_switch=manual_switch
    )

    switch(
        cloud_or_local_result,
        dict(
            cloud=cloud(parameters={"input_string": input_string}),
            local=local(parameters={"input_string": input_string}),
        ),
    )
