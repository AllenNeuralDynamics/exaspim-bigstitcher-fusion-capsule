#!/usr/bin/env python3
"""Submit and monitor the BigStitcher affine fusion job on EMR Serverless."""

from __future__ import annotations

import argparse
import json
import shlex
import sys
import time
import uuid
from typing import Any

import boto3


DEFAULT_JOB_ROLE_ARN = "arn:aws:iam::467914378000:role/aind-emr-role"
DEFAULT_LOG_URI = "s3://aind-scratch-data/exaspim-emr-fusion-logs/"
SUCCESS_STATES = {"SUCCESS"}
FAILURE_STATES = {"FAILED", "CANCELLED", "CANCELED", "TIMEOUT", "TIMED_OUT"}


class DefaultsFormatter(argparse.HelpFormatter):
    def _get_help_string(self, action: argparse.Action) -> str:
        help_text = action.help or ""
        has_default = (
            action.option_strings
            and action.default is not argparse.SUPPRESS
            and action.default is not None
        )
        if has_default and "%(default)" not in help_text:
            help_text = f"{help_text} (default: %(default)s)"
        return help_text


class DefaultsArgumentParser(argparse.ArgumentParser):
    def add_argument(self, *args: Any, **kwargs: Any) -> argparse.Action:
        is_optional = any(isinstance(arg, str) and arg.startswith("-") for arg in args)
        if is_optional and kwargs.get("help") is None:
            kwargs["help"] = "(default: %(default)s)"
        return super().add_argument(*args, **kwargs)


def parse_args() -> argparse.Namespace:
    parser = DefaultsArgumentParser(
        description="Submit a BigStitcher Spark affine fusion job to EMR Serverless.",
        formatter_class=DefaultsFormatter,
    )
    parser.add_argument(
        "zarr_location",
        nargs="?",
        help="Output Zarr location to pass to Spark.",
    )
    parser.add_argument("--job-name", default="fusion-job")
    parser.add_argument("--job-role-arn", default=DEFAULT_JOB_ROLE_ARN)
    parser.add_argument("--release-label", default="emr-7.11.0")
    parser.add_argument("--architecture", default="X86_64")
    parser.add_argument(
        "--spark-jar",
        required=True,
        help="Spark jar S3 URI to submit to EMR Serverless.",
    )
    parser.add_argument(
        "--spark-class",
        default="net.preibisch.bigstitcher.spark.SparkAffineFusion",
    )
    parser.add_argument("--s3-region", default="us-west-2")
    parser.add_argument("--storage-format", default="ZARR")
    parser.add_argument("--fusion-mode", default="AVG_BLEND")
    parser.add_argument("--block-scale", default="2,2,1")
    parser.add_argument("--log-uri", default=DEFAULT_LOG_URI)
    parser.add_argument("--poll-seconds", type=int, default=10)
    parser.add_argument("--execution-timeout-minutes", type=int, default=2880)

    parser.add_argument("--driver-worker-count", type=int, default=1)
    parser.add_argument("--driver-worker-cpu", default="8vCPU")
    parser.add_argument("--driver-worker-memory", default="32GB")
    parser.add_argument("--executor-worker-count", type=int, default=1)
    parser.add_argument("--executor-worker-cpu", default="8vCPU")
    parser.add_argument("--executor-worker-memory", default="32GB")

    parser.add_argument("--driver-cores", type=int, default=8)
    parser.add_argument("--driver-memory", default="24G")
    parser.add_argument("--executor-cores", type=int, default=8)
    parser.add_argument("--executor-memory", default="24G")
    parser.add_argument(
        "--dynamic-allocation-enabled",
        action=argparse.BooleanOptionalAction,
        default=True,
    )
    parser.add_argument("--dynamic-allocation-min-executors", type=int, default=1)
    parser.add_argument("--dynamic-allocation-initial-executors", type=int, default=2)
    parser.add_argument("--dynamic-allocation-max-executors", type=int, default=100)
    parser.add_argument("--aws-retry-mode", default="adaptive")
    parser.add_argument("--aws-max-attempts", type=int, default=15)
    parser.add_argument("--spark-task-max-failures", type=int, default=15)
    parser.add_argument(
        "--hive-metastore-client-factory-class",
        default="com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the EMR request payloads without calling AWS.",
    )
    args = parser.parse_args()
    if not args.zarr_location:
        parser.error("zarr_location is required")
    return args


def build_initial_capacity(args: argparse.Namespace) -> dict[str, Any]:
    return {
        "DRIVER": {
            "workerCount": args.driver_worker_count,
            "workerConfiguration": {
                "cpu": args.driver_worker_cpu,
                "memory": args.driver_worker_memory,
            },
        },
        "EXECUTOR": {
            "workerCount": args.executor_worker_count,
            "workerConfiguration": {
                "cpu": args.executor_worker_cpu,
                "memory": args.executor_worker_memory,
            },
        },
    }


def build_spark_submit_parameters(args: argparse.Namespace) -> str:
    conf = {
        "spark.driver.cores": args.driver_cores,
        "spark.driver.memory": args.driver_memory,
        "spark.executor.cores": args.executor_cores,
        "spark.executor.memory": args.executor_memory,
        "spark.dynamicAllocation.enabled": str(args.dynamic_allocation_enabled).lower(),
        "spark.dynamicAllocation.minExecutors": args.dynamic_allocation_min_executors,
        "spark.dynamicAllocation.initialExecutors": args.dynamic_allocation_initial_executors,
        "spark.dynamicAllocation.maxExecutors": args.dynamic_allocation_max_executors,
        "spark.executorEnv.AWS_REGION": args.s3_region,
        "spark.emr-serverless.driverEnv.AWS_REGION": args.s3_region,
        "spark.executorEnv.AWS_RETRY_MODE": args.aws_retry_mode,
        "spark.emr-serverless.driverEnv.AWS_RETRY_MODE": args.aws_retry_mode,
        "spark.executorEnv.AWS_MAX_ATTEMPTS": args.aws_max_attempts,
        "spark.emr-serverless.driverEnv.AWS_MAX_ATTEMPTS": args.aws_max_attempts,
        "spark.task.maxFailures": args.spark_task_max_failures,
        "spark.hadoop.hive.metastore.client.factory.class": (
            args.hive_metastore_client_factory_class
        ),
    }
    parts = ["--class", args.spark_class]
    for key, value in conf.items():
        parts.extend(["--conf", f"{key}={value}"])
    return shlex.join(parts)


def build_entry_point_arguments(args: argparse.Namespace) -> list[str]:
    return [
        "-o",
        args.zarr_location,
        "-s",
        args.storage_format,
        "--s3Region",
        args.s3_region,
        "-f",
        args.fusion_mode,
        "--blockScale",
        args.block_scale,
    ]


def build_create_application_request(
    args: argparse.Namespace,
    client_token: str,
) -> dict[str, Any]:
    return {
        "type": "SPARK",
        "name": args.job_name,
        "releaseLabel": args.release_label,
        "architecture": args.architecture,
        "initialCapacity": build_initial_capacity(args),
        "clientToken": client_token,
    }


def build_start_job_run_request(
    args: argparse.Namespace,
    application_id: str,
    client_token: str,
) -> dict[str, Any]:
    return {
        "applicationId": application_id,
        "clientToken": client_token,
        "executionRoleArn": args.job_role_arn,
        "name": args.job_name,
        "jobDriver": {
            "sparkSubmit": {
                "entryPoint": args.spark_jar,
                "entryPointArguments": build_entry_point_arguments(args),
                "sparkSubmitParameters": build_spark_submit_parameters(args),
            }
        },
        "configurationOverrides": {
            "monitoringConfiguration": {
                "s3MonitoringConfiguration": {
                    "logUri": args.log_uri,
                }
            }
        },
        "executionTimeoutMinutes": args.execution_timeout_minutes,
    }


def build_dry_run_payload(args: argparse.Namespace) -> dict[str, Any]:
    application_id = "<application-id>"
    return {
        "create_application": build_create_application_request(
            args,
            "<create-application-client-token>",
        ),
        "start_application": {"applicationId": application_id},
        "start_job_run": build_start_job_run_request(
            args,
            application_id,
            "<start-job-run-client-token>",
        ),
        "stop_application": {"applicationId": application_id},
        "poll_seconds": args.poll_seconds,
    }


def wait_for_application_state(
    client: Any,
    application_id: str,
    target_states: set[str],
    failure_states: set[str],
    poll_seconds: int,
) -> str:
    while True:
        response = client.get_application(applicationId=application_id)
        state = response["application"]["state"]
        print(f"Application {application_id}: {state}", flush=True)
        if state in target_states:
            return state
        if state in failure_states:
            raise RuntimeError(
                f"Application {application_id} reached unexpected state {state}"
            )
        time.sleep(poll_seconds)


def wait_for_job_run(
    client: Any,
    application_id: str,
    job_run_id: str,
    poll_seconds: int,
) -> str:
    while True:
        response = client.get_job_run(
            applicationId=application_id,
            jobRunId=job_run_id,
        )
        state = response["jobRun"]["state"]
        print(f"Job run {job_run_id}: {state}", flush=True)
        if state in SUCCESS_STATES:
            return state
        if state in FAILURE_STATES:
            raise RuntimeError(f"Job run {job_run_id} reached state {state}")
        time.sleep(poll_seconds)


def run(args: argparse.Namespace) -> int:
    if args.dry_run:
        print(json.dumps(build_dry_run_payload(args), indent=2))
        return 0

    client = boto3.client("emr-serverless", region_name=args.s3_region)
    application_id = None

    try:
        create_response = client.create_application(
            **build_create_application_request(
                args,
                f"create-{uuid.uuid4().hex}",
            )
        )
        application_id = create_response["applicationId"]
        print(f"My application's ID: {application_id}", flush=True)

        wait_for_application_state(
            client,
            application_id,
            target_states={"CREATED"},
            failure_states={"TERMINATED"},
            poll_seconds=args.poll_seconds,
        )

        client.start_application(applicationId=application_id)
        print("Application start requested", flush=True)
        wait_for_application_state(
            client,
            application_id,
            target_states={"STARTED"},
            failure_states={"STOPPED", "TERMINATED"},
            poll_seconds=args.poll_seconds,
        )

        job_response = client.start_job_run(
            **build_start_job_run_request(
                args,
                application_id,
                f"job-{uuid.uuid4().hex}",
            )
        )
        job_run_id = job_response["jobRunId"]
        print(f"Job run ID: {job_run_id}", flush=True)

        wait_for_job_run(
            client,
            application_id,
            job_run_id,
            poll_seconds=args.poll_seconds,
        )
        print("Job complete", flush=True)
        return 0
    finally:
        if application_id is not None:
            print(f"Stopping application {application_id}", flush=True)
            try:
                client.stop_application(applicationId=application_id)
            except Exception as err:  # noqa: BLE001
                print(
                    f"WARNING: Failed to stop application {application_id}: {err}",
                    file=sys.stderr,
                    flush=True,
                )


def main() -> int:
    args = parse_args()
    try:
        return run(args)
    except Exception as err:  # noqa: BLE001
        print(f"ERROR: {err}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
