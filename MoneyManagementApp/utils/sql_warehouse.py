from databricks import sql
from databricks.sdk.core import Config
import os

def get_connection():

    cfg = Config()  # Pull environment variables for auth
    return sql.connect(
        server_hostname=cfg.host,
        http_path=f"/sql/1.0/warehouses/{os.getenv('DATABRICKS_WAREHOUSE_ID')}",
        credentials_provider=lambda: cfg.authenticate
    )
