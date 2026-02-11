import argparse
import sys
import time
import csv
import os
import schedule
from datetime import datetime, timedelta
from billing_calculation_service import BillingCalculationService
from calculate.service import CalculateService
from utils.logger import setup_logger

# Configure logging
logger = setup_logger()

def log_failure_to_csv(billing_account_id, start_date, end_date, error_msg, log_source="billing_sync.log"):
    """
    Log failure details to a CSV file.
    """
    file_path = "billing_sync_failures.csv"
    file_exists = os.path.isfile(file_path)
    
    try:
        with open(file_path, mode='a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow(["Log Time", "Billing Account ID", "Date Range", "Error Message", "Log Source"])
                
            date_range = f"{start_date} to {end_date}"
            log_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            writer.writerow([log_time, billing_account_id, date_range, str(error_msg), log_source])
    except Exception as e:
        logger.error(f"Failed to write to CSV log: {e}")

def get_dim_month(invoice_month):
    """Convert invoice_month (YYYYMM) to dim_month format (YYYY-MM)."""
    return f"{invoice_month[:4]}-{invoice_month[4:]}"

def month_task_day(invoice_month: str,usage_day_start: datetime.date,usage_day_end: datetime.date,target_table: str, calc_service: BillingCalculationService):
    #invoice_month = '202601'
    start_time = time.time()
    dim_month = get_dim_month(invoice_month)
    
    logger.info(f"Processing invoice_month: {invoice_month}")
    if not usage_day_start or not usage_day_end:
        usage_day_start, usage_day_end = calc_service._get_min_max_usage_day(invoice_month=invoice_month)   
    logger.info(f"Usage day range: {usage_day_start} to {usage_day_end}")

    if not usage_day_start or not usage_day_end:
        logger.error(f"No usage data found for {invoice_month}")
        return
    df_contract=calc_service.get_dim_contract(month=dim_month)

    while usage_day_start <= usage_day_end:
        calc_service.pipeline_day(invoice_month,df_contract, usage_day_start,target_table=target_table)
        # 天数加 1 (Correctly using interval)
        usage_day_start += timedelta(days=1)

    elapsed = time.time() - start_time
    logger.info(f"month_task_day 总执行时间: {elapsed:.2f} 秒")




def month_task_billingid(invoice_month: str, calc_service: BillingCalculationService):
    #invoice_month = '202601'
    start_time = time.time()
    dim_month = get_dim_month(invoice_month)
    
    logger.info(f"Processing invoice_month: {invoice_month}")
    
    usage_day_start, usage_day_end = calc_service._get_min_max_usage_day(invoice_month=invoice_month)
    logger.info(f"Usage day range: {usage_day_start} to {usage_day_end}")

    if not usage_day_start or not usage_day_end:
        logger.error(f"No usage data found for {invoice_month}")
        return
    billing_account_id_list = calc_service.get_billing_account_ids(
        invoice_month=invoice_month,
        usage_day_start=usage_day_start,
        usage_day_end=usage_day_end
    )['billing_account_id'].values.tolist()
    
    logger.info(f"Found {len(billing_account_id_list)} billing accounts to process")

    for billing_account_id in billing_account_id_list:
        interval = 15
        if billing_account_id in [
            "01C21A-D27751-089D20",
            "01194A-900FE2-3CBAF8",
            "01F82E-571136-4A9147",
            "01CEEE-1EC3DF-9B4A3D",
            "017AC6-21D881-77FBCD",
            "017198-4BB4D2-CDF4EF",
            "01587C-263C61-84FBDB",
            "0177A8-AA1A62-456013",
            "013196-4D30CB-14E3E2",
            "015116-F46E8A-C292BB",
            "01B372-0ED33C-BC0E0B",
            "01F1D7-9F0400-E05D90",
            "01ACBD-4B4CE4-2D688D",
            "01A663-5EF6A3-8A9516"
        ]:
            interval = 1
            
        current_date = usage_day_start
        end_date = usage_day_end
        
        
        while current_date <= end_date:
            # 构造当天的起始和结束时间
            endtime = current_date + timedelta(days=interval)
            if endtime > end_date:
                endtime = end_date + timedelta(days=1)
                
            try:
                calc_service.pipeline_billingaccount_day(
                    invoice_month=invoice_month, 
                    df_contract=df_contract,
                    billing_account_id=billing_account_id, 
                    usage_day_start=current_date, 
                    usage_day_end=endtime, 
                    dim_month=dim_month
                )
                logger.info(f"Processed {billing_account_id} from {current_date} to {endtime}")

            except Exception as e:
                # 记录失败信息
                logger.error(f"Processing failed: billing_account_id={billing_account_id}, from {current_date} to {endtime}, error: {e}", exc_info=True)
                log_failure_to_csv(billing_account_id, current_date, endtime, e)
            
            # 天数加 1 (Correctly using interval)
            current_date += timedelta(days=interval)
    
    elapsed = time.time() - start_time
    logger.info(f"Total execution time: {elapsed:.2f} seconds")

def detail_billing_id_day(invoice_month: str, calc_service: BillingCalculationService, billing_account_id: str, usage_day_start: str, usage_day_end: str):
    logger.info("Starting billing calculation process")

    calc_service = BillingCalculationService()

    invoice_month='202601'
    usage_day_start='2026-01-01'
    usage_day_end='2026-02-02'
    dim_month=get_dim_month(invoice_month)
    billing_account_id='012700-35F6CD-34C971'
    df_contract=calc_service.get_dim_contract(month=dim_month, billing_account_id=billing_account_id)

    try:
        calc_service.pipeline_billingaccount_day(
            invoice_month=invoice_month, 
            df_contract=df_contract,
            billing_account_id=billing_account_id, 
            usage_day_start=usage_day_start, 
            usage_day_end=usage_day_end, 
            dim_month=dim_month
        )
    except Exception as e:
        logger.error(f"Processing failed: billing_account_id={billing_account_id}, from {usage_day_start} to {usage_day_end}, error: {e}", exc_info=True)
        log_failure_to_csv(billing_account_id, usage_day_start, usage_day_end, e)



def daily_cron_work():
    current_date = datetime.now().date()
    usage_day_start = current_date - timedelta(days=4)
    first_day=current_date.replace(day=1)
    if(usage_day_start<first_day):
        usage_day_start = first_day
    usage_day_end = current_date
    invoice_month = current_date.strftime('%Y%m')
    temp_table='dwm_standard_daily_billing_calculated_tmp'
    target_table='dwm_standard_daily_billing_calculated'
    calc_service = BillingCalculationService()

    #先清理临时表指定时间段分区
    sql_clkean_tmp=f"""
    ALTER TABLE {target_table}
    DELETE WHERE invoice_month='{invoice_month}'
    and usage_day >='{usage_day_start}'
    and usage_day <='{usage_day_end}'
    """
    calc_service.execute_sql(sql_clkean_tmp)
    month_task_day(invoice_month=invoice_month, usage_day_start=usage_day_start, usage_day_end=usage_day_end, target_table=temp_table, calc_service=calc_service)
     # 清理目标表
    sql_clean_target=f"""
    ALTER TABLE {target_table}
    DELETE WHERE invoice_month='{invoice_month}'
    and usage_day >='{usage_day_start}'
    and usage_day <='{usage_day_end}'
    """
    calc_service.execute_sql(sql_clean_target)
    
    # 合并数据到目标表
    sql_merge=f"""
    INSERT INTO {target_table}
    SELECT * FROM {temp_table}
    WHERE invoice_month='{invoice_month}'
    and usage_day >='{usage_day_start}'
    and usage_day <='{usage_day_end}'
    """
    calc_service.execute_sql(sql_merge)
    calc_service.send_feishu_alarm(f"今日任务执行结束： for invoice_month={invoice_month} from {usage_day_start} to {usage_day_end}")



def main():

    # Schedule the task to run every day at 07:00
    schedule.every().day.at("05:00").do(daily_cron_work)
    logger.info("Scheduler started. Waiting for next job execution at 05:00...")
    
    while True:
        schedule.run_pending()
        time.sleep(1) # Check every minute

    

    




    

if __name__ == "__main__":
    #正式任务
    #main()
    
    #测试日常任务
    #daily_cron_work()

    #整月手动同步任务到临时表
    calc_service = BillingCalculationService()
    invoice_month="202601"
    target_table="dwm_standard_daily_billing_calculated_tmp"
    month_task_day(invoice_month,usage_day_start=None,usage_day_end=None,target_table=target_table, calc_service=calc_service )   
    

