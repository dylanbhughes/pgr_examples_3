from prefect import task, Flow, Parameter
from prefect.storage import GitHub
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import LocalRun


@task(log_stdout=True)
def extract(input_string):
    print(input_string)
    return [1, 2, 3, 4, 5, 6]


@task
def transform(number):
    return number * 2


@task
def load(numbers):
    print(f"Uploaded {numbers} to Snowflake")


with Flow(
    "ETL - Local",
    storage=GitHub(
        repo="dylanbhughes/pgr_examples_3",
        path="local_flow.py",
        secrets=["GITHUB_ACCESS_TOKEN"],
    ),
    run_config=LocalRun(labels=["pgr local"]),
    executor=LocalDaskExecutor(scheduler="threads", num_workers=3),
) as flow:
    input_string = Parameter(name="input_string", required=True)
    numbers = extract(input_string=input_string)
    tranformed_numbers = transform.map(numbers)
    result = load(numbers=tranformed_numbers)
