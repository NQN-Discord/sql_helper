import time
from prometheus_client import Histogram
from sentry_sdk import add_breadcrumb


def sql_wrapper(namespace: str):
    sql_request = Histogram(
        "sql_request_total",
        "SQL Requests",
        ["query"],
        namespace=namespace
    )

    def middle(execute):
        async def execute_wrapper(query, *args, **kwargs):
            start_time = time.perf_counter()
            try:
                return await execute(query, *args, **kwargs)
            finally:
                total_time = time.perf_counter() - start_time
                sql_request.labels(query=query).observe(total_time)

                add_breadcrumb(
                    message="Made SQL query",
                    category="sql",
                    data={
                        "query": query,
                        "params": kwargs.get("parameters"),
                        "total_time": total_time
                    }
                )

        return execute_wrapper
    return middle
