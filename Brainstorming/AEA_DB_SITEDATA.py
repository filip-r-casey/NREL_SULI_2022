import os
import pandas as pd
import psycopg2

conn = psycopg2.connect(host="localhost", database="root", user="root", password="root")
cur = conn.cursor()

loc_df = pd.DataFrame(columns=["Site Name", "Coordinates", "Elevation", "Altitude"])

for filename in os.listdir("./AEA_DATA"):
    with open(
        os.path.join(
            "./AEA_DATA",
            filename,
        ),
        "r",
    ) as f:  # open in readonly mode
        longterm = True if filename[-17:-4] == "longterm-data" else False
        shortterm = True if filename[-19:-4] == "1YearSythesized" else False
        if longterm:
            lines = f.readlines()
            location = lines[1][1 : lines[1].find("m") + 1]
            latitude = float(location[:3]) + float(location[10:12]) / 60
            if location[14] == "S":
                latitude = -latitude
            longitude = float(location[17:20]) + float(location[28:30]) / 60
            if location[32] == "W":
                longitude = -longitude
            elevation = int(
                location[location.find("Elev") + len("Elev") + 3 : location.find("m")]
            )
            loc_df.loc[len(loc_df.index)] = [
                filename[: filename.find("_")],
                str(round(latitude, 3)) + " , " + str(round(longitude, 3)),
                elevation,
                0,
            ]
            altitude = 0
            cur.execute(
                "SELECT * FROM aea_sites WHERE site_name = %s",
                (filename[: filename.find("_")],),
            )
            if cur.fetchall():
                cur.execute(
                    "UPDATE aea_sites SET latitude = %s, longitude = %s, elevation = %s, altitude = %s WHERE site_name = %s",
                    (
                        round(latitude, 3),
                        round(longitude, 3),
                        elevation,
                        altitude,
                        filename[: filename.find("_")],
                    ),
                )
                conn.commit()
            else:
                cur.execute(
                    "INSERT INTO aea_sites (site_name, latitude, longitude, elevation, altitude) VALUES (%s, %s, %s, %s, %s)",
                    (
                        filename[: filename.find("_")],
                        round(latitude, 3),
                        round(longitude, 3),
                        elevation,
                        altitude,
                    ),
                )
                conn.commit()
        elif shortterm:
            lines = f.readlines()
            location = lines[2]  # takes third line from file as the location string
            south = False
            west = False
            if location.count("S"):
                south = True
            if location.count("W"):
                west = True
            stats = location.split(",")
            stats[-1] = stats[-1].replace("\t\n", "")
            latitude = stats[0]
            while not latitude[
                -1
            ].isnumeric():  # removes any extra spaces from the end of the string
                latitude = latitude[:-1]
            while not latitude[
                0
            ].isnumeric():  # removes any extra spaces from the end of the string
                latitude = latitude[1:]
            if " degrees" in latitude:  # removes the "degrees" unit if it is present
                latitude = latitude.replace(" degrees", "")
            if "'" in latitude:  # removes the ' unit if it is present
                latitude = latitude.replace("'", "")
            if '"' in latitude:  # removes the " unit if it is present
                latitude = latitude.replace('"', "")
            if latitude.count(
                " "
            ):  # if latitude is formatted degrees, minutes, (seconds)
                if latitude.count(" ") == 2:  # degrees, minutes, seconds
                    latitude = (
                        float(latitude.split()[0])
                        + float(latitude.split()[1]) / 60
                        + float(latitude.split()[2]) / (60**2)
                    )
                else:  # degrees, minutes
                    latitude = (
                        float(latitude.split()[0]) + float(latitude.split()[1]) / 60
                    )
            elif latitude.count(
                "-"
            ):  # if latitude is formatted with dashes instead of spaces
                if latitude.count("-") == 2:
                    latitude = (
                        float(latitude.split("-")[0])
                        + float(latitude.split("-")[1]) / 60
                        + float(latitude.split("-")[2]) / (60**2)
                    )
                else:
                    latitude = (
                        float(latitude.split("-")[0])
                        + float(latitude.split("-")[1]) / 60
                    )
            latitude = float(
                latitude
            )  # catch all float conversion if string is formatted as a decimal
            if south:
                latitude = -latitude
            longitude = stats[1]
            while not longitude[
                -1
            ].isnumeric():  # removes any extra spaces from the end of the string
                longitude = longitude[:-1]
            while not longitude[
                0
            ].isnumeric():  # removes any extra spaces from the end of the string
                longitude = longitude[1:]
            if " degrees" in longitude:  # removes the "degrees" unit if it is present
                longitude = longitude.replace(" degrees", "")
            if "'" in longitude:  # removes the ' unit if it is present
                longitude = longitude.replace("'", "")
            if '"' in longitude:  # removes the " unit if it is present
                longitude = longitude.replace('"', "")
            if longitude.count(
                " "
            ):  # if longitude is formatted degrees, minutes, (seconds)
                if longitude.count(" ") == 2:  # degrees, minutes, seconds
                    longitude = (
                        float(longitude.split()[0])
                        + float(longitude.split()[1]) / 60
                        + float(longitude.split()[2]) / (60**2)
                    )
                else:  # degrees, minutes
                    longitude = (
                        float(longitude.split()[0]) + float(longitude.split()[1]) / 60
                    )
            elif longitude.count(
                "-"
            ):  # if longitude is formatted with dashes instead of spaces
                if longitude.count("-") == 2:
                    longitude = (
                        float(longitude.split("-")[0])
                        + float(longitude.split("-")[1]) / 60
                        + float(longitude.split("-")[2]) / (60**2)
                    )
                else:
                    longitude = (
                        float(longitude.split("-")[0])
                        + float(longitude.split("-")[1]) / 60
                    )
            longitude = float(
                longitude
            )  # catch all float conversion if string is formatted as a decimal
            if west:
                longitude = -longitude
            if "Altitude" in stats[2]:
                altitude = stats[2]
                while not altitude[-1].isnumeric():
                    altitude = altitude[:-1]
                while not altitude[0].isnumeric():
                    altitude = altitude[1:]
                elevation = stats[3]
                while not elevation[-1].isnumeric():
                    elevation = elevation[:-1]
                while not elevation[0].isnumeric():
                    elevation = elevation[1:]
            else:
                elevation = stats[2]
                while not elevation[-1].isnumeric():
                    elevation = elevation[:-1]
                while not elevation[0].isnumeric():
                    elevation = elevation[1:]
            loc_df.loc[len(loc_df.index)] = [
                filename[: filename.find("_")],
                str(round(latitude, 3)) + " , " + str(round(longitude, 3)),
                elevation,
                altitude,
            ]

            print(filename[: filename.find("_")])
            cur.execute(
                "SELECT * FROM aea_sites WHERE site_name = %s",
                (filename[: filename.find("_")],),
            )
            if cur.fetchall():
                print("UPDATE")
                cur.execute(
                    "UPDATE aea_sites SET latitude = %s, longitude = %s, elevation = %s, altitude = %s WHERE site_name = %s",
                    (
                        round(latitude, 3),
                        round(longitude, 3),
                        elevation,
                        altitude,
                        filename[: filename.find("_")],
                    ),
                )
                conn.commit()
            else:
                print("INSERT")
                cur.execute(
                    "INSERT INTO aea_sites (site_name, latitude, longitude, elevation, altitude) VALUES (%s, %s, %s, %s, %s)",
                    (
                        filename[: filename.find("_")],
                        round(latitude, 3),
                        round(longitude, 3),
                        elevation,
                        altitude,
                    ),
                )
                conn.commit()
print(loc_df)
loc_df.set_index("Site Name")
loc_df.to_csv("./Site_Locations.csv")
