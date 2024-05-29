from robocorp.tasks import task  # type:ignore
from RPA.HTTP import HTTP  # type:ignore
from RPA.JSON import JSON  # type:ignore
from RPA.Tables import Tables  # type:ignore
from robocorp import workitems  # type:ignore

http = HTTP()
json = JSON()
table = Tables()


TRAFFIC_JSON_FILE_PATH = "output/traffic.json"


@task
def produce_traffic_data():
    """
    Inhuman Insurance, Inc. Artificial Intelligence System automation.
    Produces traffic data work items.
    """
    http.download(  # type: ignore
        url="https://github.com/robocorp/inhuman-insurance-inc/raw/main/RS_198.json",
        target_file=TRAFFIC_JSON_FILE_PATH,
        overwrite=True,
    )
    traffic_data = load_traffic_data_as_table()
    table.write_table_to_csv(traffic_data, "output/test.csv")
    data_filtered = filter_and_sort_traffic_data(traffic_data)
    data_filtered = get_latest_data_by_country(data_filtered)  # type: ignore
    playloads = create_playload(data_filtered)  # type: ignore
    backup_playloads(playloads)


def load_traffic_data_as_table():
    json_data: json = json.load_json_from_file(TRAFFIC_JSON_FILE_PATH)  # type: ignore
    return table.create_table(json_data["value"])  # type: ignore


def filter_and_sort_traffic_data(data_table):  # type: ignore
    rate_key = "NumericValue"
    max_rate = 5.0
    gender_key = "Dim1"
    both_genders = "BTSX"
    year_key = "TimeDim"
    table.filter_table_by_column(data_table, rate_key, "<", max_rate)  # type: ignore
    table.filter_table_by_column(data_table, gender_key, "==", both_genders)  # type: ignore
    table.sort_table_by_column(data_table, year_key, False)  # type: ignore
    return data_table  # type: ignore


def get_latest_data_by_country(data):  # type: ignore
    country_key = "SpatialDim"
    data = table.group_table_by_column(data, country_key)  # type: ignore
    latest_data_by_country = []
    for group in data:
        first_row = table.pop_table_row(group)  # type: ignore
        latest_data_by_country.append(first_row)  # type: ignore
    return latest_data_by_country  # type: ignore


def create_playload(traffic_data):  # type: ignore
    playloads = []
    for row in traffic_data:  # type: ignore
        playload = {}
        playload["country"] = row["SpatialDim"]  # type: ignore
        playload["year"] = row["TimeDim"]
        playload["rate"] = row["NumericValue"]
        playloads.append(playload)  # type: ignore
    return playloads  # type: ignore


def backup_playloads(playloads):  # type: ignore
    variable = {}
    for item in playloads:  # type: ignore
        variable["traffic_data"] = item  # type: ignore
        workitems.outputs.create(variable)  # type: ignore
