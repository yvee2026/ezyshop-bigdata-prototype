#!/bin/sh
# Starts a single-node SeaweedFS (master + volume + filer + S3 gateway).
# The S3 identity is generated from env vars so no credentials live in the repo.
set -eu

mkdir -p /etc/seaweedfs /data
cat > /etc/seaweedfs/s3.json <<EOF
{
  "identities": [
    {
      "name": "ezyshop",
      "credentials": [{ "accessKey": "${S3_ACCESS_KEY}", "secretKey": "${S3_SECRET_KEY}" }],
      "actions": ["Admin", "Read", "Write", "List", "Tagging"]
    }
  ]
}
EOF

exec weed server \
  -dir=/data \
  -ip=seaweedfs \
  -ip.bind=0.0.0.0 \
  -master.volumeSizeLimitMB=1024 \
  -volume.max=0 \
  -filer \
  -s3 \
  -s3.port=8333 \
  -s3.config=/etc/seaweedfs/s3.json
