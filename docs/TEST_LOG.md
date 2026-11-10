# Test Log: EzyShop Big Data Prototype

Real issues found while building and testing the prototype, in the order they occurred.
Every "Evidence" value was observed when the command was run; nothing is estimated.

| ID | Date | Phase | Test / Activity | Issue observed | Root cause | Resolution | Evidence (before → after) | Status |
|----|------|-------|-----------------|----------------|------------|------------|---------------------------|--------|
| T01 | 2026-11-10 | 1 | Verify pinned image tags exist for arm64 (`docker manifest inspect`) | `minio/minio` and `minio/mc` could not be pulled: Docker Hub returned `denied: requested access to the resource is denied`; the Hub tag API returned `object not found`; `quay.io/minio/minio` had no manifest either | MinIO no longer publishes pullable images on the public registries the spec assumed | Replaced MinIO with **SeaweedFS 4.48** (`chrislusf/seaweedfs`, Apache-2.0, upstream image, multi-arch). It exposes the same S3 API, so Spark's S3A connector and Delta Lake are unchanged; only the endpoint differs. Bucket created with `weed shell s3.bucket.create` instead of `mc mb` | `docker manifest inspect minio/minio:latest` → `denied`; `chrislusf/seaweedfs:4.48` → arm64 + amd64 manifest present | Fixed (substituted) |
