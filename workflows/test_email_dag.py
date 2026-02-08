"""Test DAGs - Verify email alerting on task failure."""

from airflow.sdk import DAG, task
from openplanetdata.airflow.defaults import EMAIL_ALERT_RECIPIENTS

with DAG(
    dag_id="test-email-on-failure",
    default_args={
        "email": ["airflow@elaunira.com"],
        "email_on_failure": True,
        "owner": "openplanetdata",
    },
    description="Test email alerting on task failure (standard node)",
    schedule=None,
    tags=["openplanetdata", "test"],
) as dag:

    @task
    def fail() -> None:
        """Deliberately fail to trigger an email alert."""
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
