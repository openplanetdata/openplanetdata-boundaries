"""Test DAGs - Verify email alerting on task failure."""

import logging

from airflow.sdk import DAG, task
from kubernetes.client import models as k8s
from openplanetdata.airflow.defaults import EMAIL_ALERT_RECIPIENTS

log = logging.getLogger(__name__)

EXECUTOR_CONFIG = {
    "pod_override": k8s.V1Pod(
        spec=k8s.V1PodSpec(
            node_selector={
                "workload": "worker",
                "node.kubernetes.io/instance-type": "cx33",
            },
            tolerations=[
                k8s.V1Toleration(
                    key="workload",
                    operator="Equal",
                    value="worker",
                    effect="NoSchedule",
                ),
            ],
            containers=[k8s.V1Container(name="base")],
        )
    )
}

with DAG(
    dag_id="test-email-on-failure",
    default_args={
        "email": ["airflow@elaunira.com"],
        "email_on_failure": True,
        "owner": "openplanetdata",
    },
    description="Test email alerting on task failure (worker node)",
    schedule=None,
    tags=["openplanetdata", "test"],
) as dag:

    @task(executor_config=EXECUTOR_CONFIG)
    def fail() -> None:
        """Deliberately fail to trigger an email alert."""
        log.info("Starting test task on worker node")
        log.info("About to raise deliberate failure for email alert testing")
        raise RuntimeError("Test failure to verify email alerting")

    fail()

with DAG(
    dag_id="test-email-on-failure-edge",
    default_args={
        "email": ["airflow@elaunira.com"],
        "email_on_failure": True,
        "executor": "airflow.providers.edge3.executors.EdgeExecutor",
        "owner": "openplanetdata",
        "queue": "cortex",
    },
    description="Test email alerting on task failure (cortex edge worker)",
    schedule=None,
    tags=["openplanetdata", "test"],
) as dag:

    @task
    def fail_edge() -> None:
        """Deliberately fail to trigger an email alert."""
        raise RuntimeError("Test failure to verify email alerting")

    fail_edge()
