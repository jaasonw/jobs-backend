from shillelagh.backends.apsw.db import connect
from fastapi import FastAPI
from fastapi.responses import PlainTextResponse
import io
import csv

app = FastAPI()

@app.get("/api/python")
def test():
    connection = connect(":memory:")
    cursor = connection.cursor()

    SQL = """
    SELECT Company
    FROM "https://docs.google.com/spreadsheets/d/1Rp1yFWi6yJMhUGq2fkfGUhkmteScma5r9XkUEbqhq14/edit#gid=0"
    """
    rows = [row for row in cursor.execute(SQL)]
    return {"rows": rows}

@app.get("/api/jobs", response_class=PlainTextResponse)
def get_jobs():
    connection = connect(":memory:")
    cursor = connection.cursor()

    SQL = """
    SELECT Date, URL, Company, Title, Location, Technologies, "Years of Experience"
    FROM "https://docs.google.com/spreadsheets/d/1Rp1yFWi6yJMhUGq2fkfGUhkmteScma5r9XkUEbqhq14/edit#gid=0"
    """
    rows = [row for row in cursor.execute(SQL)]
    field_names = [i[0] for i in cursor.description]
    output = io.StringIO(newline='\n')
    writer = csv.writer(output)
    writer.writerow(field_names)
    writer.writerows(rows)
    return output.getvalue().replace('\r\n', '\n').encode('utf-8')