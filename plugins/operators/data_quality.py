from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    """
    This operator carries out data quality checks on the Redshift tables
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tables="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.tables = tables
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        # fetch the redshift hook
        redshift_hook = PostgresHook(self.redshift_conn_id)
        for table in self.tables:

            # Checks for NULL in the specified table and column
            records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table['name']} WHERE {table['column_name']} IS NULL")    
            if records[0] and records[0][0] and records[0][0] >= 1:
                raise ValueError(f"Data quality check failed. {table['name']} returned {records[0][0]} records with NULL for column {table['column_name']}.")
            else:          
                self.log.info(f"Data quality on table {table['name']} check passed with column {table['column_name']} containing a value for all records.")

            # Checks if the specified table contains records
            records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table['name']}") 
            if records[0] and records[0][0] and records[0][0] >= 1:
                self.log.info(f"Data quality on table {table['name']} check passed by containing records ({records[0][0]}).")
            else:
                raise ValueError(f"Data quality check failed. {table['name']} returned {records[0][0]} records")    

        
