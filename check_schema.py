from client.clickhouse_client import ClickhouseClient
import yaml

def get_schema():
    with open('config.yaml', 'r') as f:
        config = yaml.safe_load(f)
    
    cfg = config['clickhouse']
    client = ClickhouseClient(
        host=cfg['host'],
        port=cfg['port'],
        user=cfg['user'],
        password=cfg['password'],
        database=cfg['database'],
        secure=cfg['secure'],
        verify=cfg['verify']
    )
    
    print("--- ods_standard_daily_billing ---")
    print(client.query_dataframe("DESCRIBE billing.ods_standard_daily_billing")[['name', 'type']])
    print("\n--- dim_contract ---")
    print(client.query_dataframe("DESCRIBE billing.dim_contract")[['name', 'type']])

if __name__ == "__main__":
    get_schema()
