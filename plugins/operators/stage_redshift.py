from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook


class StageToRedshiftOperator(BaseOperator):
    """ Class to stage tables in s3 to redshift

    Parameters
    ----------
    table : str
        name of the table to load
    redshift_conn_id : str
        connection string for Redshift
    aws_credentials_id : str
        connection string to AWS credentials to connect to s3
    s3_bucket : str
        string containing name of the s3 bucket
    s3_key : str
        string containing the name of the s3 key
    JSON_log : str
        path for JSON mapping (or 'auto')
    """

    ui_color = "#358140"
    template_fields = ("s3_key",)

    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        FORMAT AS JSON '{}'
    """

    @apply_defaults
    def __init__(
        self,
        table="",
        redshift_conn_id="",
        aws_credentials_id="",
        s3_bucket="",
        s3_key="",
        JSON_log="",
        *args,
        **kwargs,
    ):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.JSON_log = JSON_log

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)

        self.log.info(
            f"Creating staging table for {self.table} from s3 location {s3_path}"
        )
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.JSON_log,
        )
        redshift.run(formatted_sql)





