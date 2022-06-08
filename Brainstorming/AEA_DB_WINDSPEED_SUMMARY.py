import os
import pandas as pd
import psycopg2


def read_tab_table(lines, table_name):
    """
    Parameters:     lines       = list of strings from file (output of readlines())
                    table_name  = name of table to extract data from
    Output:         df          = data formatted as dataframe
    """
    for i, line in enumerate(
        lines
    ):  # changes tab-separated file to comma-separated and removes newline characters
        lines[i] = line.replace("\t", ",").replace("\n", "")
    for i, line in enumerate(lines):
        if line.count(table_name):  # finds start of table by searching for table name
            start = i
            break
    start += 1
    end = len(lines)
    for i, line in enumerate(lines[start:]):
        if len(line) > 0:
            if line[0].isalpha():  # finds end of table by searching for next character
                end = i
                break
        else:
            end = i
            break
    end = start + end

    df = pd.DataFrame([x.split(",") for x in lines[start:end]])  # dataframe formatting
    df.columns = df.iloc[0]
    df = df[1:]
    df.set_index(df.columns[1], inplace=True)
    if "ALL" in df.columns:
        df.drop("ALL", axis=1, inplace=True)
    if "ALL" in df.index:
        df.drop(["ALL"], axis=0, inplace=True)
    df.dropna(axis=1)
    df.columns.name = ""
    return df


def data_types(lines):
    data_types = []
    for line in lines[3:]:
        if len(line) > 0:
            if line[0].isalpha():  # finds end of table by searching for next character
                data_types.append(line.replace("\t", "").replace("\n", "").strip())
    return data_types


def historic_sql_formatter(input_df, sql_df, col_name):
    for row in input_df.iterrows():
        for month, val in enumerate(row[1].values[1:]):
            month += 1
            sql_df.loc[
                (sql_df["year"] == row[0]) & (sql_df["month"] == month),
                col_name,
            ] = val


def cyclic_sql_formatter(input_df, sql_df, col_name):
    for row in input_df.iterrows():
        for month, val in enumerate(row[1].values[1:]):
            month += 1
            sql_df.loc[
                (sql_df["hour"] == int(row[0])) & (sql_df["month"] == month),
                col_name,
            ] = val


def frequency_direction_sql_formatter(input_df, sql_df, col_name):
    for row in input_df.iterrows():
        for direction, val in enumerate(row[1].values[1:]):
            if direction == 0:
                direction = -1
            else:
                direction = 10 * (direction - 1)
            sql_df.loc[
                (sql_df["month"] == int(row[0])) & (sql_df["direction"] == direction),
                col_name,
            ] = val


def frequency_speed_sql_formatter(input_df, sql_df, col_name):
    for row in input_df.iterrows():
        for speed, val in enumerate(row[1].values[1:]):
            sql_df.loc[
                (sql_df["month"] == int(row[0])) & (sql_df["speed"] == speed),
                col_name,
            ] = val


def prevailing_direction_sql_formatter(input_df, sql_df, col_name):
    for row in input_df.iterrows():
        for hour, val in enumerate(row[1].values[1:]):
            sql_df.loc[
                (sql_df["month"] == int(row[0])) & (sql_df["hour"] == hour),
                col_name,
            ] = val


conn = psycopg2.connect(host="localhost", database="root", user="root", password="root")
cur = conn.cursor()

loc_df = pd.DataFrame(columns=["Site Name", "Coordinates", "Elevation", "Altitude"])

site_dict = {}

data_type_map = {
    "SPEED BY YEAR": "aea_historic_wind_data",
    "POWER BY YEAR": "aea_historic_wind_data",
    "NUMBER OF SPEED OBSERVATIONS BY YEAR": "aea_historic_wind_data",
    "NUMBER OF SPEED OBSERVATIONS BY HOUR": "aea_cyclic_hour_data",
    "SPEED BY HOUR": "aea_cyclic_hour_data",
    "POWER BY HOUR": "aea_cyclic_hour_data",
    "SPEED BY DIRECTION": "aea_frequency_direction",
    "FREQUENCY BY DIRECTION": "aea_frequency_direction",
    "FREQUENCY OF SPEED": "aea_frequency_speed",
    "PERCENT OF POWER BY SPEED": "aea_frequency_speed",
    "PREVAILING DIRECTION BY HOUR": "aea_prevailing_direction_data",
    "SPEED FOR PREVAILING DIRECTION BY HOUR": "aea_prevailing_direction_data",
}
historic_sql_df = pd.DataFrame(
    columns=[
        "site_name",
        "year",
        "month",
        "wind_speed",
        "power",
        "speed_observations",
    ]
)
cyclic_sql_df = pd.DataFrame(
    columns=[
        "site_name",
        "hour",
        "month",
        "speed_observations",
        "speed",
        "power",
    ]
)

frequency_direction_sql_df = pd.DataFrame(
    columns=[
        "site_name",
        "month",
        "direction",
        "speed",
        "frequency",
    ]
)
frequency_speed_sql_df = pd.DataFrame(
    columns=["site_name", "month", "speed", "frequency", "percent_power"]
)
prevailing_direction_sql_df = pd.DataFrame(
    columns=[
        "site_name",
        "month",
        "hour",
        "prevailing_direction",
        "speed_for_prevailing",
    ]
)
for filename in os.listdir("./AEA_DATA"):
    with open(
        os.path.join(
            "./AEA_DATA",
            filename,
        ),
        "r",
    ) as f:  # open in readonly mode

        longterm = True if filename[-17:-4] == "longterm-data" else False

        site_name = filename[: filename.find("_")]

        # site_dict[filname] = {}
        if longterm:
            lines = f.readlines()
            for title in data_types(lines):
                df = read_tab_table(lines, title)
                years = df.index.values
                table = data_type_map[title]
                match table:
                    case "aea_historic_wind_data":
                        if site_name not in list(
                            set(historic_sql_df["site_name"].values)
                        ):
                            for year in years:
                                for month in range(1, 13):
                                    historic_sql_df.loc[len(historic_sql_df.index)] = [
                                        site_name,
                                        year,
                                        month,
                                        0,
                                        0,
                                        0,
                                    ]
                        match title:
                            case "SPEED BY YEAR":
                                historic_sql_formatter(
                                    df, historic_sql_df, "wind_speed"
                                )
                            case "POWER BY YEAR":
                                historic_sql_formatter(df, historic_sql_df, "power")
                            case "NUMBER OF SPEED OBSERVATIONS BY YEAR":
                                historic_sql_formatter(
                                    df, historic_sql_df, "speed_observations"
                                )
                    case "aea_cyclic_hour_data":
                        if site_name not in list(
                            set(cyclic_sql_df["site_name"].values)
                        ):
                            for hour in range(0, 24):
                                for month in range(1, 13):
                                    cyclic_sql_df.loc[len(cyclic_sql_df.index)] = [
                                        site_name,
                                        hour,
                                        month,
                                        0,
                                        0,
                                        0,
                                    ]
                        match title:
                            case "NUMBER OF SPEED OBSERVATIONS BY HOUR":
                                cyclic_sql_formatter(
                                    df, cyclic_sql_df, "speed_observations"
                                )
                            case "SPEED BY HOUR":
                                cyclic_sql_formatter(df, cyclic_sql_df, "speed")
                            case "POWER BY HOUR":
                                cyclic_sql_formatter(df, cyclic_sql_df, "power")
                    case "aea_frequency_direction":
                        if site_name not in list(
                            set(frequency_direction_sql_df["site_name"].values)
                        ):
                            for month in range(1, 13):
                                frequency_direction_sql_df.loc[
                                    len(frequency_direction_sql_df.index)
                                ] = [
                                    site_name,
                                    month,
                                    -1,
                                    0,
                                    0,
                                ]
                                for direction in range(0, 370, 10):
                                    frequency_direction_sql_df.loc[
                                        len(frequency_direction_sql_df.index)
                                    ] = [
                                        site_name,
                                        month,
                                        direction,
                                        0,
                                        0,
                                    ]
                        match title:
                            case "SPEED BY DIRECTION":
                                frequency_direction_sql_formatter(
                                    df, frequency_direction_sql_df, "speed"
                                )
                            case "FREQUENCY BY DIRECTION":
                                frequency_direction_sql_formatter(
                                    df, frequency_direction_sql_df, "frequency"
                                )
                    case "aea_frequency_speed":
                        if site_name not in list(
                            set(frequency_speed_sql_df["site_name"].values)
                        ):
                            for month in range(1, 13):
                                for speed in range(0, 26):
                                    frequency_speed_sql_df.loc[
                                        len(frequency_speed_sql_df.index)
                                    ] = [site_name, month, speed, 0, 0]
                        match title:
                            case "FREQUENCY OF SPEED":
                                frequency_speed_sql_formatter(
                                    df, frequency_speed_sql_df, "frequency"
                                )
                            case "PERCENT OF POWER BY SPEED":
                                frequency_speed_sql_formatter(
                                    df, frequency_speed_sql_df, "percent_power"
                                )
                    case "aea_prevailing_direction_data":
                        if site_name not in list(
                            set(prevailing_direction_sql_df["site_name"].values)
                        ):
                            for month in range(1, 13):
                                for hour in range(0, 24):
                                    prevailing_direction_sql_df.loc[
                                        len(prevailing_direction_sql_df.index)
                                    ] = [site_name, month, hour, 0, 0]
                        match title:
                            case "PREVAILING DIRECTION BY HOUR":
                                prevailing_direction_sql_formatter(
                                    df,
                                    prevailing_direction_sql_df,
                                    "prevailing_direction",
                                )
                            case "SPEED FOR PREVAILING DIRECTION BY HOUR":
                                prevailing_direction_sql_formatter(
                                    df,
                                    prevailing_direction_sql_df,
                                    "speed_for_prevailing",
                                )
for row in historic_sql_df.iterrows():
    data_input = row[1].values
    cur.execute(
        "INSERT INTO aea_historic_wind_data (site_name, year, month, wind_speed, power, speed_observations) VALUES (%s, %s, %s, %s, %s, %s)",
        (
            data_input[0],
            data_input[1],
            data_input[2],
            data_input[3],
            data_input[4],
            data_input[5],
        ),
    )
for row in cyclic_sql_df.iterrows():
    data_input = row[1].values
    cur.execute(
        "INSERT INTO aea_cyclic_hour_data (site_name, hour, month, speed_observations, speed, power) VALUES (%s, %s, %s, %s, %s, %s)",
        (
            data_input[0],
            data_input[1],
            data_input[2],
            data_input[3],
            data_input[4],
            data_input[5],
        ),
    )
for row in frequency_direction_sql_df.iterrows():
    data_input = row[1].values
    cur.execute(
        "INSERT INTO aea_frequency_direction (site_name, month, direction, speed, frequency) VALUES (%s, %s, %s, %s, %s)",
        (data_input[0], data_input[1], data_input[2], data_input[3], data_input[4]),
    )
for row in frequency_speed_sql_df.iterrows():
    data_input = row[1].values
    cur.execute(
        "INSERT INTO aea_frequency_speed (site_name, month, speed, frequency, percent_power) VALUES (%s, %s, %s, %s, %s)",
        (data_input[0], data_input[1], data_input[2], data_input[3], data_input[4]),
    )
for row in prevailing_direction_sql_df.iterrows():
    data_input = row[1].values
    cur.execute(
        "INSERT INTO aea_prevailing_direction_data (site_name, month, hour, prevailing_direction, speed_for_prevailing) VALUES (%s, %s, %s, %s, %s)",
        (data_input[0], data_input[1], data_input[2], data_input[3], data_input[4]),
    )
conn.commit()
