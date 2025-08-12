#!/bin/bash
set -e

NOW=$(date '+%F_%H_%M')
fname=via_rail_${NOW}.json

# Download original JSON from VIA Rail
curl --silent --output ${fname} "https://tsimobile.viarail.ca/data/allData.json"

# Add the train_id and collected_at timestamp to each record
python3 - <<EOF
import json
import datetime

timestamp = "${NOW}"
with open("${fname}", "r") as f:
    data = json.load(f)

output = [dict(train_id=k, collected_at=timestamp, **v) for k, v in data.items()]

with open("${fname}", "w") as f:
    json.dump(output, f)
EOF

# Upload to Google Cloud Storage
gcloud storage cp ${fname} gs://viarail-json-datalake/datafiles/${fname} --quiet

# Clean up local file
rm -f ${fname}
