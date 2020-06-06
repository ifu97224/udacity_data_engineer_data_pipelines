from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):
    """ Class to load fact tables

    Parameters
    ----------
    table : str
        name of the table to load
    redshift_conn_id : str
        connection string for Redshift
    truncate_table : bool
        boolean to identify if table should be cleared before loading
    sql_query : str
        string containing the SQL query to load the table

    """

    ui_color = "#F98866"

    @apply_defaults
    def __init__(
        self,
        table="",
        redshift_conn_id="",
        truncate_table="",
        sql_query="",
        *args,
        **kwargs,
    ):
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.truncate_table = truncate_table
        self.sql_query = sql_query

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if truncate_table:
            self.log.info(f"Clearing data from table {self.table} Redshift table")
            redshift.run("DELETE FROM {}".format(self.table))

        self.log.info(f"Loading fact table {self.table}")
        redshift.run(f"INSERT INTO {self.table} {self.sql_query}")
        self.log.info(f"Fact table {self.table} loaded")
