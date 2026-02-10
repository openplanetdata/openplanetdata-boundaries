"""DAG run utilities."""


def check_dag_run_failures(context: dict) -> None:
    """Raise if any task in the current DAG run has failed."""
    failed = [ti for ti in context["dag_run"].get_task_instances() if ti.state == "failed"]
    if failed:
        raise RuntimeError(f"{len(failed)} task(s) failed: {', '.join(ti.task_id for ti in failed)}")
