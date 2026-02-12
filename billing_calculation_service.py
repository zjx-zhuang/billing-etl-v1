from datetime import datetime, timedelta
import pandas as pd
import yaml
import os
from client.clickhouse_client import ClickhouseClient
from calculate.service import CalculateService
# import main # Removed to fix circular dependency
from utils.logger import setup_logger
import csv
import requests
# Configure logging
logger = setup_logger()
pd.set_option('future.no_silent_downcasting', True)
class BillingCalculationService:
    def __init__(self, config_path='config.yaml'):
        self.config_path = config_path
        self.client = self._init_client(config_path)

    def log_failure_to_csv(self, usage_day, error_msg, log_source="billing_sync.log"):
        """
        Log failure details to a CSV file.
        """
        file_path = "billing_sync_failures.csv"
        file_exists = os.path.isfile(file_path)
        
        try:
            with open(file_path, mode='a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                if not file_exists:
                    writer.writerow(["Log Time", "usage_day", "Error Message", "Log Source"])
                log_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                writer.writerow([log_time, usage_day, str(error_msg), log_source])
        except Exception as e:
            logger.error(f"Failed to write to CSV log: {e}")


    def _init_client(self, config_path):
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Config file not found: {config_path}")
        
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f) or {}
            
        file_config = config.get('clickhouse', {})
        host = file_config.get('host', 'localhost')
        port = file_config.get('port', 9000)
        user = file_config.get('user', 'default')
        password = file_config.get('password', '')
        database = file_config.get('database', 'default')
        secure = file_config.get('secure', True)
        verify = file_config.get('verify', False)
        
        
        return ClickhouseClient(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            secure=secure,
            verify=verify
        )

    def process_monthly_billing(self, invoice_month):
        """
        Process billing for the given month.
        1. Determine min/max usage_day
        2. Iterate daily
        3. For each day, process each billing account
        """
        print(f"Starting billing process for month: {invoice_month}")
        
        # 2. Get min/max usage_day for the month
        min_day, max_day = self._get_min_max_usage_day(invoice_month)
        if not min_day or not max_day:
            print(f"No data found for month {invoice_month}")
            return

        print(f"Processing range: {min_day} to {max_day}")

        # 3. Interval loop (Daily)
        current_day = min_day
        # Ensure we cover the range properly. 
        # User logic: '2026-01-01'<=usage_day<'2026-01-02' (so usage_day_end is next day)
        # Assuming usage_day is a Date column, so we iterate by day.
        
        while current_day <= max_day:
            usage_day_start = current_day
            usage_day_end = current_day + timedelta(days=1)
            
            print(f"Processing day: {usage_day_start}")
            self._process_single_day(invoice_month, usage_day_start, usage_day_end)
            
            current_day += timedelta(days=1)

    def _get_min_max_usage_day(self, invoice_month):
        query = """
            SELECT min(_PARTITIONTIME), max(_PARTITIONTIME)
            FROM billing.ods_standard_daily_billing
            WHERE invoice_month = %(invoice_month)s
        """
        params = {'invoice_month': invoice_month}
        result = self.client.execute(query, params=params)
        if result and result[0]:
            return result[0][0], result[0][1]
        return None, None

    def _process_single_day(self, invoice_month, usage_day_start, usage_day_end):
        # 3.1 Get distinct billing_account_ids for the day
        accounts_df = self.get_billing_account_ids(invoice_month, usage_day_start, usage_day_end)
        
        if accounts_df.empty:
            print(f"No accounts found for {usage_day_start}")
            return

        billing_account_ids = accounts_df['billing_account_id'].tolist()
        
        # 3.2 Iterate billing_account_id
        for billing_account_id in billing_account_ids:
            # Get billing data
            df = self.get_standard_daily_billing(invoice_month, billing_account_id, usage_day_start, usage_day_end)
            
            if df.empty:
                continue

            # 3.3 Get contract data
            # Note: User's pseudo-code passes 'month' (derived from invoice_month likely, e.g. '2026-01')
            # invoice_month format is '202601', contract month format '2026-01'
            contract_month = f"{invoice_month[:4]}-{invoice_month[4:]}"
            dim_df = self.get_dim_contract(contract_month, billing_account_id)

            # 3.4 Calculate
            calculated_df = CalculateService.calculate_with_credits(df, dim_df)

            # 3.5 Insert into target table
            if not calculated_df.empty:
                self._insert_calculated_data(calculated_df)

    def get_billing_account_ids(self, invoice_month, usage_day_start, usage_day_end):
        """
        Query distinct billing_account_id from ods_standard_daily_billing table.
        """
        query = """
            SELECT DISTINCT billing_account_id 
            FROM billing.ods_standard_daily_billing 
            WHERE invoice_month = %(invoice_month)s 
            and usage_day >= %(usage_day_start)s 
            and usage_day < %(usage_day_end)s
        """
        params = {
            'invoice_month': invoice_month,
            'usage_day_start': usage_day_start,
            'usage_day_end': usage_day_end
        }
        return self.client.query_dataframe(query=query, params=params)
    
    def execute_sql(self, sql):
        return self.client.execute(sql)

    def get_standard_daily_billing(self, invoice_month, billing_account_id, usage_day_start, usage_day_end):
        """
        Query ods_standard_daily_billing table by invoice_month and usage_day range.
        """
        query = """
            select
                    invoice_month, billing_account_id, usage_day, project_id, service_id,service_description, sku_id, cost_type  
                    ,sum(usage_amount_in_pricing_units) as usage_amount_in_pricing_units   
                    ,sum(cost) as cost
                    ,sum(cost_at_list) as cost_at_list
                    ,sum(c_cud) as c_cud
                    ,sum(c_cud_db) as c_cud_db
                    ,sum(c_discount) as c_discount
                    ,sum(c_free_tier) as c_free_tier
                    ,sum(c_promotion) as c_promotion
                    ,sum(c_rm) as c_rm
                    ,sum(c_sub_benefit) as c_sub_benefit
                    ,sum(c_sud) as c_sud 
                    ,sum(internal_credits_cost) as internal_credits_cost
                    ,sum(internal_credits_consumption) as internal_credits_consumption
                   from   billing.ods_standard_daily_billing 
                   WHERE invoice_month = %(invoice_month)s 
	              AND billing_account_id = %(billing_account_id)s 
	              and usage_day >= %(usage_day_start)s 
	              and usage_day < %(usage_day_end)s
                   group by 
                   invoice_month, billing_account_id, usage_day, project_id, service_id, service_description,sku_id, cost_type   
        """
        params = {
            'invoice_month': invoice_month,
            'billing_account_id': billing_account_id,
            'usage_day_start': usage_day_start,
            'usage_day_end': usage_day_end
        }
        return self.client.query_dataframe(query=query, params=params)

    def get_standard_daily_billing_iterator(self, invoice_month, usage_day):
        """
        Query ods_standard_daily_billing table by invoice_month and usage_day range.
        Returns an iterator yielding DataFrames in batches.
        """
        query = """
            select
                    invoice_month, billing_account_id, usage_day , project_id, service_id,service_description, sku_id, cost_type  
                    ,sum(usage_amount_in_pricing_units) as usage_amount_in_pricing_units   
                    ,sum(cost) as cost
                    ,sum(cost_at_list) as cost_at_list
                    ,sum(c_cud) as c_cud
                    ,sum(c_cud_db) as c_cud_db
                    ,sum(c_discount) as c_discount
                    ,sum(c_free_tier) as c_free_tier
                    ,sum(c_promotion) as c_promotion
                    ,sum(c_rm) as c_rm
                    ,sum(c_sub_benefit) as c_sub_benefit
                    ,sum(c_sud) as c_sud 
                    ,sum(internal_credits_cost) as internal_credits_cost
                    ,sum(internal_credits_consumption) as internal_credits_consumption
                   from   billing.ods_standard_daily_billing 
                   WHERE invoice_month = %(invoice_month)s 
	               and usage_day = %(usage_day)s 
                   group by 
                   invoice_month, billing_account_id, usage_day, project_id, service_id, service_description,sku_id, cost_type   
        """
        params = {
            'invoice_month': invoice_month,
            'usage_day': usage_day
        }
        
        # Use a separate client for iteration to avoid "Simultaneous queries" error
        # when other queries (like inserts) are executed within the iteration loop.
        iter_client = self._init_client(self.config_path)
        return iter_client.iterate(query=query, params=params, batch_size=10000)


    def get_standard_daily_billing_test(self, invoice_month, billing_account_id, usage_day_start, usage_day_end):
            """
            Query ods_standard_daily_billing table by invoice_month and usage_day range.
            """
            query = """
                select
                        usage_day
                        ,invoice_month
                        ,billing_account_id
                        ,service_id
                        ,service_description
                        ,sku_id
                        ,sku_description
                        ,project_id
                        ,project_name
                        ,usage_pricing_unit
                        ,sum(usage_amount_in_pricing_units) as usage_amount_in_pricing_units
                        ,currency                      
                        ,currency_conversion_rate      
                        ,cost_type   
                        ,sum(cost) as cost
                        ,sum(cost_at_list) as cost_at_list
                        ,sum(c_cud) as c_cud
                        ,sum(c_cud_db) as c_cud_db
                        ,sum(c_discount) as c_discount
                        ,sum(c_free_tier) as c_free_tier
                        ,sum(c_promotion) as c_promotion
                        ,sum(c_rm) as c_rm
                        ,sum(c_sub_benefit) as c_sub_benefit
                        ,sum(c_sud) as c_sud 
                        ,sum(internal_credits_cost) as internal_credits_cost
                        ,sum(internal_credits_consumption) as internal_credits_consumption
                    from   billing.ods_standard_daily_billing 
                    WHERE invoice_month = %(invoice_month)s 
                    AND billing_account_id = %(billing_account_id)s 
                    and project_id='ai-period-tracker'
                    group by 
                    usage_day
                    ,invoice_month
                    ,billing_account_id
                    ,project_id
                    ,project_name
                    ,service_id
                    ,service_description
                    ,sku_id
                    ,sku_description
                    ,usage_pricing_unit
                    ,currency                      
                    ,currency_conversion_rate      
                    ,cost_type   
            """
            params = {
                'invoice_month': invoice_month,
                'billing_account_id': billing_account_id
            }
            return self.client.query_dataframe(query=query, params=params)

    def get_dim_contract(self, month, billing_account_id=None):
        """
        Query dim_contract table by month and billing_account_id.
        如果billing_account_id为None，则查询该月份所有合同
        """
        if billing_account_id is not None:
            query = """
                SELECT * 
                FROM billing.dim_contract 
                WHERE month = %(month)s 
                AND billing_account_id = %(billing_account_id)s
            """
            params = {
                'month': month,
                'billing_account_id': billing_account_id
            }
        else:
            query = """
                SELECT * 
                FROM billing.dim_contract 
                WHERE month = %(month)s
            """
            params = {
                'month': month
            }
                
        dfs = []
        total_rows = 0
        for batch_df in self.client.iterate(query=query, params=params, batch_size=10000):
            dfs.append(batch_df)
            total_rows += len(batch_df)
            logger.info(f"dim数据汇总:Batch {len(dfs)} fetched, rows in batch: {len(batch_df)}, total rows: {total_rows}")
            
        if not dfs:
            return pd.DataFrame()
            
        return pd.concat(dfs, ignore_index=True)


    def _insert_calculated_data(self, df,target_table='dwm_standard_daily_billing_calculated'):
        """
        Insert calculated data into target_table.
        """
        # Ensure DataFrame columns match the target table structure
        target_columns = [
            'usage_day', 'invoice_month', 'billing_account_id', 
            'customer_id', 'contract_id', 
            'service_id', 'service_description', 
            'sku_id', 'sku_description', 
            'project_id', 'project_name', 
            'usage_pricing_unit', 'usage_amount_in_pricing_units', 
            'currency', 'currency_conversion_rate', 
            'cost_type', 
            'cost', 'cost_at_list', 
            'c_cud', 'c_cud_db', 'c_discount', 'c_free_tier', 
            'c_promotion', 'c_rm', 'c_sub_benefit', 'c_sud', 
            'internal_credits_cost', 'internal_credits_consumption', 
            'internal_cost', 'internal_consumption', 
            'external_consumption', 'discount_amount', 
            'mode', 'price', 'discount', 
            'credit_fields', 'etl_time'
        ]
        
        # Add missing columns with default values if necessary
        for col in target_columns:
            if col not in df.columns:
                if col == 'etl_time':
                    df[col] = datetime.now()
                elif col in ['customer_id', 'contract_id']:
                     # These are Nullable(String), so keep as NaN or None if missing
                     pass
                else:
                    # Set sensible defaults for other missing columns to avoid errors
                    # String types -> ''
                    # Numeric -> 0
                    if col in ['service_id', 'service_description', 'sku_id', 'sku_description', 
                               'project_id', 'project_name', 'usage_pricing_unit', 'currency', 
                               'cost_type', 'credit_fields', 'invoice_month']:
                        df[col] = ''
                    else:
                        df[col] = 0.0

        # Ensure etl_time is set
        if 'etl_time' not in df.columns:
            df['etl_time'] = datetime.now()
            
        # Select and order columns to match target table
        # Filter out extra columns that might be in df but not in target table
        # (e.g. intermediate calculation columns)
        df_to_insert = df[target_columns].copy()
        
        # Fill NaNs for non-nullable string columns to avoid issues
        string_cols = ['billing_account_id', 'service_id', 'service_description', 
                       'sku_id', 'sku_description', 'project_id', 'project_name', 
                       'usage_pricing_unit', 'currency', 'cost_type', 'credit_fields', 'invoice_month']
        for col in string_cols:
             if col in df_to_insert.columns:
                 df_to_insert[col] = df_to_insert[col].fillna('')

        # Fill NaNs for numeric columns (except nullable customer_id/contract_id)
        numeric_cols = [c for c in target_columns if c not in string_cols and c not in ['customer_id', 'contract_id', 'etl_time', 'usage_day']]
        for col in numeric_cols:
             if col in df_to_insert.columns:
                 df_to_insert[col] = df_to_insert[col].fillna(0).infer_objects(copy=False)
                 
        # Explicit type conversion for ClickHouse compatibility
        
        # invoice_month: Ensure it is String
        if 'invoice_month' in df_to_insert.columns:
            df_to_insert['invoice_month'] = df_to_insert['invoice_month'].astype(str)
            # Check if any values look like floats (e.g., '202602.0') and fix them
            df_to_insert['invoice_month'] = df_to_insert['invoice_month'].apply(
                lambda x: x.split('.')[0] if '.' in x else x
            )

        # Ensure integers for ID columns
        int_cols = ['mode']
        for col in int_cols:
            if col in df_to_insert.columns:
                # Fill NaNs with 0 for mode (Int8 default 0)
                df_to_insert[col] = df_to_insert[col].fillna(0).astype(int)

        # Ensure Nullable(String) columns are handled
        nullable_str_cols = ['customer_id', 'contract_id']
        for col in nullable_str_cols:
            if col in df_to_insert.columns:
                 # Convert non-null values to string
                 mask = df_to_insert[col].notna()
                 df_to_insert.loc[mask, col] = df_to_insert.loc[mask, col].astype(str)
        
        # Ensure usage_day is date
        if 'usage_day' in df_to_insert.columns:
            # If it's datetime, convert to date
            if pd.api.types.is_datetime64_any_dtype(df_to_insert['usage_day']):
                 df_to_insert['usage_day'] = df_to_insert['usage_day'].dt.date

        try:
            self.client.insert_dataframe(
                f'INSERT INTO billing.{target_table} VALUES',
                df_to_insert
            )
        except Exception as e:
            print(f"Error inserting data: {e}")
            # Fallback or re-raise if needed
            raise
        
    def pipeline_billingaccount_day(self, invoice_month,df_contract, billing_account_id, usage_day_start, usage_day_end, dim_month):
        df=self.get_standard_daily_billing(invoice_month=invoice_month, billing_account_id=billing_account_id, usage_day_start=usage_day_start, usage_day_end=usage_day_end) 
        calculated =CalculateService.calculate_with_credits(df, df_contract)
        if not calculated.empty:
            self._insert_calculated_data(calculated)
            logger.info(f"Successfully inserted {len(calculated)} rows for billing account {billing_account_id} in usage day {usage_day_start} to {usage_day_end}")
        else:
            logger.info(f"No calculated data to insert for billing account {billing_account_id} in usage day {usage_day_start} to {usage_day_end}, skipping.")

    def pipeline_day(self, invoice_month,df_contract, usage_day_start,target_table='dwm_standard_daily_billing_calculated'):
        try:
            total_inserted = 0
            iterator = self.get_standard_daily_billing_iterator(invoice_month, usage_day_start)
            for batch_df in iterator:
            # batch_df 是包含最多 10000 行数据的 DataFrame
                if batch_df.empty:
                    logger.info(f"No data for usage day {usage_day_start}, skipping.")
                    continue
                calculated =CalculateService.calculate_with_credits(batch_df, df_contract)
                if not calculated.empty:
                    self._insert_calculated_data(calculated,target_table=target_table)
                    count = len(calculated)
                    total_inserted += count
                    logger.info(f"Successfully inserted {count} rows for usage day {usage_day_start}. Total inserted so far: {total_inserted}")
                else:
                    logger.info(f"No calculated data to insert for usage day {usage_day_start}, skipping.")
            logger.info(f"Completed pipeline for usage day {usage_day_start}. Total rows inserted: {total_inserted}")
        except Exception as e:
             # 记录失败信息
            logger.error(f"Processing failed: 当前处理天： {usage_day_start} , error: {e}", exc_info=True)
            self.send_feishu_alarm(f"Processing failed: 当前处理天： {usage_day_start} , error: {e}")
            return

    def send_feishu_alarm(self,content):

        """发送飞书消息的核心函数"""
        payload = {
            "msg_type": "text",
            "content": {
                "text": content
            }
        }
        try:
            response = requests.post("https://open.feishu.cn/open-apis/bot/v2/hook/5cb76227-0aba-429a-83d3-bf5443118150", json=payload)
            if response.json().get("code") != 0:
                logger.error(f"飞书发送失败: {response.text}")
        except Exception as e:
            logger.error(f"网络异常，无法连接飞书: {e}")    


