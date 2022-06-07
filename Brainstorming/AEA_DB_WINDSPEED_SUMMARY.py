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
    df.columns.name = ""
    return df


def data_types(lines):
    data_types = []
    for line in lines[3:]:
        if len(line) > 0:
            if line[0].isalpha():  # finds end of table by searching for next character
                data_types.append(line.replace("\t", "").replace("\n", "").strip())
    return data_types


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

for filename in os.listdir("./AEA_DATA"):
    with open(
        os.path.join(
            "./AEA_DATA",
            filename,
        ),
        "r",
    ) as f:  # open in readonly mode
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
                        if len(historic_sql_df) == 0:
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
                                for row in df.iterrows():
                                    for month, val in enumerate(row[1].values[1:]):
                                        historic_sql_df.loc[
                                            (historic_sql_df["year"] == row[0])
                                            & (historic_sql_df["month"] == month),
                                            "wind_speed",
                                        ] = val

                    # case "aea_cyclic_hour_data":
                    #     print()
                    # case "aea_frequency_direction":
                    #     print()
                    # case "aea_frequency_speed":
                    #     print()
                    # case "aea_prevailing_direction_data":
                    #     print()
        print(historic_sql_df)
