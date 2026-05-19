import json
from .s3_client import s3
from vectordb.serverless_vectordb import ServerlessVectorDB


def delete_indexes(bucket, dataset_name):
    prefix = f"indexes/{dataset_name}/"
    paginator = s3.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        if "Contents" in page:
            objects_to_delete = [
                {"Key": obj["Key"]}
                for obj in page["Contents"]
                if not obj["Key"].endswith("config.json")
            ]

            if objects_to_delete:
                s3.delete_objects(
                    Bucket=bucket,
                    Delete={"Objects": objects_to_delete}
                )


def delete_index_configs(bucket, dataset_name):
    prefix = f"indexes/{dataset_name}/"
    paginator = s3.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        if "Contents" in page:
            configs_to_delete = [
                {"Key": obj["Key"]}
                for obj in page["Contents"]
                if obj["Key"].endswith("config.json")
            ]

            if configs_to_delete:
                s3.delete_objects(
                    Bucket=bucket,
                    Delete={"Objects": configs_to_delete}
                )


def save_index_config(bucket, dataset_name, params: dict):
    implementation = params["implementation"]
    num_index = params.get("num_index", 16)

    key = f"indexes/{dataset_name}/{implementation}/config.json"

    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(params).encode("utf-8"),
        ContentType="application/json"
    )


def load_index_config(bucket, dataset_name, implementation, num_index):
    key = f"indexes/{dataset_name}/{implementation}/config.json"

    response = s3.get_object(Bucket=bucket, Key=key)

    config = json.loads(response["Body"].read().decode("utf-8"))

    return config


def reindex_single_config(bucket, dataset_name, config, num_workers=16):

    sv_vectordb = ServerlessVectorDB(**config)

    total_times = sv_vectordb.indexing(
        f"datasets/{dataset_name}/source.csv",
        num_workers
    )

    return total_times


def reindex_after_update(bucket, dataset_name, num_workers=16):

    prefix = f"indexes/{dataset_name}/"
    paginator = s3.get_paginator("list_objects_v2")

    configs = []

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):

        if "Contents" not in page:
            continue

        for obj in page["Contents"]:

            key = obj["Key"]

            if key.endswith("config.json"):
                response = s3.get_object(Bucket=bucket, Key=key)

                config = json.loads(
                    response["Body"].read().decode("utf-8")
                )

                configs.append(config)

    if not configs:
        print("No stored index configuration found.")
        return []

    delete_indexes(bucket, dataset_name)

    results = []

    for config in configs:

        res = reindex_single_config(
            bucket,
            dataset_name,
            config,
            num_workers
        )

        results.append(res)

    return results


def list_indexes(bucket, dataset_name):
    prefix = f"indexes/{dataset_name}/"
    paginator = s3.get_paginator("list_objects_v2")

    indexes = set()

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        if "Contents" not in page:
            continue

        for obj in page["Contents"]:
            key = obj["Key"]

            if key.endswith("config.json"):
                parts = key.split("/")
                implementation = parts[2]
                num_index = parts[3]

                indexes.add((implementation, num_index))

    return list(indexes)