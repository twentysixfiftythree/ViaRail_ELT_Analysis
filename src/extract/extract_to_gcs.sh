#!/bin/bash
set -e

NOW=$(date '+%F_%H_%M')
fname=via_rail_${NOW}.json

# Download original JSON from VIA Rail
curl --silent --output ${fname} "https://tsimobile.viarail.ca/data/allData.json"

# add the train_id to each record
python3 - <<EOF
import json
with open("${fname}", "r") as f:
    data = json.load(f)
output = [dict(train_id=k, **v) for k, v in data.items()]
with open("${fname}", "w") as f:
    json.dump(output, f)
EOF


gcloud storage cp ${fname} gs://viarail-json-datalake/datafiles/${fname} --quiet


rm -f ${fname}
