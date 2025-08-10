FROM google/cloud-sdk:slim

# Install Python 3.11 and curl (if needed)
RUN apt-get update && \
    apt-get install -y python3 python3-pip curl && \
    ln -sf /usr/bin/python3 /usr/bin/python

WORKDIR /app

COPY extract_to_gcs.sh /app/extract_to_gcs.sh

RUN chmod +x /app/extract_to_gcs.sh
