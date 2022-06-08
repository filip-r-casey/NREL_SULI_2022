import os
import psycopg2

conn = psycopg2.connect(host="localhost", database="root", user="root", password="root")
cur = conn.cursor()

for filename in os.listdir("./AEA_DATA"):
    with open(
        os.path.join(
            "./AEA_DATA",
            filename,
        ),
        "r",
    ) as f:
        shortterm = True if filename[-19:-4] == "1YearSythesized" else False

        if shortterm:
            site_name = filename[: filename.find("_")]

            lines = f.readlines()

            for i, line in enumerate(lines):
                if line.count("Timestamp"):
                    start = i + 1
                    break

            for line in lines[start:]:
                line = line.split()
                if len(line) == 3:
                    cur.execute(
                        "INSERT INTO aea_hourly_synthesized (site_name, time, wind_speed) VALUES (%s, %s, %s)",
                        (site_name, "1970/" + line[0] + " " + line[1], float(line[2])),
                    )
conn.commit()
