import time
from prometheus_client import Histogram


def sql_wrapper(namespace: str):
    sql_request = Histogram(
        "sql_request_total",
        "SQL Requests",
        ["query"],
        namespace=namespace
    )

    def middle(execute):
        async def execute_wrapper(query, *args, **kwargs):
            start_time = time.time()
            try:
                return await execute(query, *args, **kwargs)
            finally:
                end_time = time.time() - start_time
                sql_request.labels(query=query).observe(end_time)

        return execute_wrapper
    return middle
