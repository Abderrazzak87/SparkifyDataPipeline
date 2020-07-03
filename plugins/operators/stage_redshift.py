from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    """
    Copies JSON files from an S3 bucket into a Redshift staging table.
    The table is purged first if it exists.
    """
    ui_color = '#358140'

    copy_sql = """
        COPY {} FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION '{}'
        {} 'auto';
    """
    
    @apply_defaults
    def __init__(self,
                 redshift_credentials = "",
                 aws_credentials="",
                 table = "",
                 s3_path = "",
                 region= "us-west-2",
                 data_format = "",
                 *args, **kwargs):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_credentials = redshift_credentials
        self.aws_credentials = aws_credentials
        self.table = table
        self.s3_path = s3_path
        self.region = region
        self.data_format = data_format

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_credentials)
        self.log.info("Deleting data from the Redshift table destination")
        redshift.run("DELETE FROM {}".format(self.table))
        self.log.info("Copying data from S3 to the table destination")
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
                self.table, 
                self.s3_path, 
                credentials.access_key,
                credentials.secret_key, 
                self.region,
                self.data_format,
            )
        redshift.run(formatted_sql)





