from clickhouse_driver import Client
import pandas as pd

client = Client(host='34.21.0.33', port=9000, user='billing', password='billingPassw0rd.*2025', database='billing', secure=True, verify=False)

query = "SELECT 1 as a, 2 as b"
try:
    # Try with with_column_types=True
    iter_res = client.execute_iter(query, with_column_types=True)
    first = next(iter_res)
    print(f"First item: {first}")
    print(f"Type: {type(first)}")
except Exception as e:
    print(f"Error: {e}")

