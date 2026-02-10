from clickhouse_driver import Client
import pandas as pd

class ClickhouseClient:

    def __init__(self, client: Client = None, **kwargs):
        self._db_client = client if client is not None else self.__create_db_client(**kwargs)

    @staticmethod
    def __create_db_client(**kwargs):
        host = kwargs.get('host', '127.0.0.1')
        port = kwargs.get('port', 9000)
        user = kwargs.get('user', 'default')
        password = kwargs.get('password', '')
        database = kwargs.get('database', 'default')
        secure = kwargs.get('secure', False)
        verify = kwargs.get('verify', False)
        
        # print(f"ClickHouse host: {host}, port: {port}")
        return Client(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            secure=secure,
            verify=verify,

            connect_timeout=60,
            send_receive_timeout=300,   
            sync_request_timeout=300,   

            settings={
                "use_numpy": False,
                "max_block_size": 100000,
                "max_execution_time": 7200
            }
        )

    def get_client(self):
        return self._db_client
    
    def execute(self, query, params=None):
        """Execute a query and return the result."""
        return self._db_client.execute(query, params=params)

    def query_dataframe(self, query, params=None):
        """Execute a query and return the result as a DataFrame."""
        try:
            return self._db_client.query_dataframe(query, params=params)
        except Exception as e:
            print(f"Error executing query: {e}")
            raise

    def iterate(self, query, params=None, batch_size=10000):
        """Execute a query and yield batches of results as DataFrames."""
        try:
            # Execute with column types to get metadata
            # execute_iter yields rows. If with_column_types=True, the first item is column metadata.
            iter_res = self._db_client.execute_iter(query, params=params, with_column_types=True)
            
            try:
                # First item is column metadata
                columns_info = next(iter_res)
                columns = [c[0] for c in columns_info]
            except StopIteration:
                # Empty result
                return

            batch = []
            for row in iter_res:
                batch.append(row)
                if len(batch) >= batch_size:
                    yield pd.DataFrame(batch, columns=columns)
                    batch = []
            
            if batch:
                yield pd.DataFrame(batch, columns=columns)
                
        except Exception as e:
            print(f"Error executing query iterator: {e}")
            raise

    def insert_dataframe(self, query, df, settings=None):
        """Insert a DataFrame into the database."""
        try:
            # clickhouse-driver insert_dataframe does not support numpy arrays directly in recent versions
            # or if the data structure is complex. We can convert to list of dicts or values.
            # However, insert_dataframe is optimized for pandas.
            # The error 'Unsupported column type: <class 'numpy.ndarray'>' suggests that
            # the underlying driver expects list/tuple for columns but got numpy array (which pandas uses).
            
            # A common workaround is to use client.execute with a generator or list of tuples
            # OR ensure that we are using the driver correctly.
            # The 'clickhouse-driver' library's 'insert_dataframe' method SHOULD handle this.
            # If it fails, it might be due to a version mismatch or specific data types.
            
            # Let's try to pass settings={'use_numpy': True} if not already set,
            # BUT the error says "list or tuple is expected", which implies it fails to iterate over columns properly
            # or it's treating the whole dataframe as a numpy array?
            
            # Actually, the traceback shows:
            # File ".../clickhouse_driver/util/helpers.py", line 33, in column_chunks
            # raise TypeError("Unsupported column type: ...")
            
            # This happens when the driver iterates over columns and finds a numpy array instead of a list/tuple.
            # We can convert the dataframe to a list of dicts (records) and use execute, 
            # OR we can try to convert columns to lists.
            
            # Let's try converting the dataframe to a list of dicts and use execute, 
            # which is safer but might be slower. However, given the error, insert_dataframe is failing.
            
            # Alternatively, we can convert the dataframe columns to lists before passing to insert_dataframe?
            # No, insert_dataframe expects a DataFrame.
            
            # Let's switch to using execute with 'records' (list of dicts) 
            # because we don't know the exact column order vs schema without querying it,
            # but we constructed the dataframe to match the target table columns in the service.
            
            # Wait, if we use 'VALUES', we need to match the order.
            # The service layer already ordered the columns.
            
            # Convert to list of tuples for insertion
            data = df.to_dict('split')['data']
            return self._db_client.execute(query, data)
            
        except Exception as e:
            print(f"Error inserting dataframe: {e}")
            raise

    def disconnect(self):
        self._db_client.disconnect()
        
    def close(self):
        self._db_client.disconnect()
