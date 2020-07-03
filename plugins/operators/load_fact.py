from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries

class LoadFactOperator(BaseOperator):
    """
    Insert data from staging tables into the fact table.
    The target table is purged first if it exists.
    """

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_credentials = "",
                 table = "",
                 sql = "",        
                 append_only = False,
                 *args, **kwargs):
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_credentials = redshift_credentials
        self.table = table
        self.sql = sql
        self.append_only = append_only

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_credentials)
        if not self.append_only:
            self.log.info("Delete Befor insert the  fact table {}".format(self.table))
            redshift.run("DELETE FROM {}".format(self.table))         
        self.log.info("Insert data from staging tables into the fact table {}".format(self.table))
        sqlQuery = getattr(SqlQueries,self.sql).format(self.table)
        redshift.run(sqlQuery)
