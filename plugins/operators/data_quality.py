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

    Raises
    -------
    ValueError
        If table returns no results
    ValueError
        If table has 0 records
    
    """


    ui_color = "#89DA59"

    @apply_defaults
    def __init__(self, table="", redshift_conn_id="", *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")
        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError(f"Data quality check failed. {table} returned no results")
        num_records = records[0][0]

        if num_records < 1:
            raise ValueError(f"Data quality check failed. {table} has 0 records")

        logging.info(
            f"Data quality on table {table} check passed with {records[0][0]} records"
        )