from airflow.models import DagBag


def test_dag_loaded(dagbag: DagBag) -> None:
    dag = dagbag.get_dag(dag_id="weather")
    assert dagbag.import_errors == {}
    assert dag is not None
    assert len(dag.tasks) == 5
