from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """
    Performs data quality checks on a list of tables.
    The check makes sure that this tables have at least 1 row.
    redshift_conn_id is the connection id for redshift
    tables is a list of tables to check quality
    """

    ui_color = '#89DA59'
    
    @apply_defaults
    def __init__(self,
                 redshift_credentials = "",
                 tables = [],
                 *args, **kwargs):
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_credentials = redshift_credentials
        self.tables = tables
        
    def check_if_rows(self, redshift_hook, table):
        """
        Checks if a table has at least 1 row.
        redshift_hook is the redshift hook
        table is the name of the table to check
        """
        records = redshift_hook.get_records("SELECT COUNT(*) FROM {}".format(table))
        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError("Data quality check failed. {} returned no results".format(table))

        num_records = records[0][0]
        if num_records < 1:
            raise ValueError("Data quality check failed. {} contained 0 rows".format(table))

        self.log.info("Data quality check on table {} passed with {} records".format(records[0][0], table))
        

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_credentials)
        for tab in self.tables:
            self.log.info("Performing data quality checks on: {}.".format(tab))
            self.check_if_rows(redshift_hook, tab)