import requests
import json

# Connector configuration
config = {
    "name": "csv-source-connector",
    "config": {
        "connector.class": "com.github.jcustenborder.kafka.connect.spooldir.SpoolDirCsvSourceConnector",
        "tasks.max": "1",
        "topic": "csv-topic",
        "input.path": "/data/input",
        "finished.path": "/data/finished",
        "error.path": "/data/error",
        "input.file.pattern": ".*\\.csv",
        "csv.first.row.as.header": "true",
        "schema.generation.enabled": "true"
    }
}

# Send to Kafka Connect REST API
resp = requests.post(
    "http://localhost:8083/connectors",
    headers={"Content-Type": "application/json"},
    data=json.dumps(config)
)

print(resp.status_code, resp.text)
