import json
from shillelagh.backends.apsw.db import connect
from fastapi import FastAPI
from fastapi.responses import PlainTextResponse
import io
import csv

app = FastAPI()

@app.get("/api/jobs", response_class=PlainTextResponse)
def get_jobs():
    connection = connect(":memory:")
    cursor = connection.cursor()

    SQL = """
    SELECT Date, URL, Company, Title, Location, Technologies, "Years of Experience"
    FROM "https://docs.google.com/spreadsheets/d/1Rp1yFWi6yJMhUGq2fkfGUhkmteScma5r9XkUEbqhq14/edit#gid=1442393429"
    WHERE "Years of Experience" < 3 AND Blacklist != 'True' AND TS_SCI != 'True' AND Mid != 'True' AND "Senior" != 'True'
    """
    rows = [row for row in cursor.execute(SQL)]
    field_names = [i[0] for i in cursor.description]
    output = io.StringIO(newline='\n')
    writer = csv.writer(output)
    writer.writerow(field_names)
    writer.writerows(rows)
    return output.getvalue().replace('\r\n', '\n').encode('utf-8')

@app.get("/api/stats", response_class=PlainTextResponse)
def get_stats():
    connection = connect(":memory:")
    cursor = connection.cursor()

    job_db = "https://docs.google.com/spreadsheets/d/1Rp1yFWi6yJMhUGq2fkfGUhkmteScma5r9XkUEbqhq14/edit#gid=1442393429"
    metadata_db = "https://docs.google.com/spreadsheets/d/1Rp1yFWi6yJMhUGq2fkfGUhkmteScma5r9XkUEbqhq14/edit#gid=2060895389"
    SQL = f"""
    SELECT
        l.last_updated,
        counts.*
    FROM (
        SELECT
            COUNT(*) as total_count,
            SUM(Senior) as senior_count,
            SUM(Mid) as mid_count,
            SUM(TS_SCI) as ts_sci_count,
            SUM(Intern) as intern_count,
            SUM(Blacklist) as blacklist_count
        FROM "{job_db}"
    ) AS counts
    CROSS JOIN (
        SELECT last_updated
        FROM "{metadata_db}"
    ) AS l
    """
    rows = [row for row in cursor.execute(SQL)]
    field_names = [i[0] for i in cursor.description]
    data_dict = {field_names[i]: rows[0][i] for i in range(len(field_names))}
    return json.dumps(data_dict)