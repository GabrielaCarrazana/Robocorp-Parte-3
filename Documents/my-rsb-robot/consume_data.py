from robocorp.tasks import task  # pyright: ignore[reportUnknownVariableType]
from robocorp import workitems  # pyright: ignore[reportMissingTypeStubs]
from typing import Any
import requests

COUNTRY_KEY = "SpatialDim"
YEAR_KEY = "TimeDim"
RATE_KEY = "NumericValue"
GENDER_KEY = "Dim1"


@task
def consume_traffic_data():
    for item in workitems.inputs:
        payload: workitems._types.JSONType = (  # type: ignore
            item.payload
        )  # pyright: ignore[reportPrivateUsage]
        traffic_data: dict[str, Any] = (
            payload[  # pyright: ignore[reportCallIssue,reportArgumentType,reportAssignmentType,reportOptionalSubscript,reportIndexIssue]
                "traffic_data"
            ]
        )

        if validate_traffic_data(traffic_data=traffic_data):
            status_code: int | dict[str, Any] = 0
            response: int | dict[str, int | str] = {}
            status_code, response = post_traffic_data_to_sales_system(
                traffic_data=traffic_data
            )
            if status_code == 200:
                item.done()
            else:
                item.fail(
                    exception_type="APPLICATION",
                    code="TRAFFIC DATA POST FAILED",
                    message=str(response.get("message")),  # type: ignore
                )
                continue
        else:
            item.fail(
                exception_type="BUSINESS",
                code="INVALID TRAFFIC DATA ",
                message=str(item.payload),
            )
            continue


def validate_traffic_data(traffic_data: dict[str, Any]) -> bool:
    country: str = traffic_data["country"]
    return len(country) == 3


def post_traffic_data_to_sales_system(
    traffic_data: dict[str, dict[str, list[Any]]]
) -> tuple[int, dict[str, int | str]]:
    url: str = "https://robocorp.com/inhuman-insurance-inc/sales-system-api"
    response: requests.Response = requests.post(url, json=traffic_data)
    status_code: int = response.status_code
    return int(status_code), response.json()
