from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    """ Class to run data quality checks.  Currently executes a check on the number of records in
    the table and raises a value error if the table has 0 records

    Parameters
    ----------
    redshift_conn_id : str
        connection string for Redshift
    dq_checks : dict
        Python dictionary containing the data quality checks and expected results

    Raises
    -------
    ValueError
        If data quality check does not match the expected results

    """

    ui_color = "#89DA59"

    @apply_defaults
    def __init__(self, redshift_conn_id="", dq_checks="", *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.dq_checks = dq_checks

    def execute(self, context):

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        for check in self.dq_checks:
            sql = check.get("check_sql")
            exp_result = check.get("expected_result")

            records = redshift.get_records(sql)[0]

            if exp_result != records[0]:
                error_count += 1
                failing_tests.append(sql)

        if error_count > 0:
            logging.info("Tests failed")
            logging.info(failing_tests)
            raise ValueError("Data quality check failed")