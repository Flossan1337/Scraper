from google.cloud import bigquery

print("Auth OK, project:", bigquery.Client().project)

# Minimal query (kostar 0): returnerar 1 rad
client = bigquery.Client()
sql = "SELECT 1 AS ok"
print("Running test query…")
rows = list(client.query(sql).result())
print("Result:", rows[0]["ok"])
