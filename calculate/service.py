import pandas as pd
from pandas import Series, DataFrame
import numpy as np

from utils.enum import PROJECT_ID, SERVICE_DESCRIPTION, SKU_ID, BILLING_ACCOUNT_ID


class CalculateService:

    @staticmethod
    def _calculate_credits_all_type(row: Series) -> Series:
        dicts_fields = {
            "COMMITTED_USAGE_DISCOUNT": "c_cud",
            "COMMITTED_USAGE_DISCOUNT_DOLLAR_BASE": "c_cud_db",
            "DISCOUNT": "c_discount",
            "FREE_TIER": "c_free_tier",
            "PROMOTION": "c_promotion",
            "RESELLER_MARGIN": "c_rm",
            "SUBSCRIPTION_BENEFIT": "c_sub_benefit",
            "SUSTAINED_USAGE_DISCOUNT": "c_sud",
        }
        z = zip(row["credits_type"], row["credits_amount"])
        result = {
            "c_cud": 0.0,
            "c_cud_db": 0.0,
            "c_discount": 0.0,
            "c_free_tier": 0.0,
            "c_promotion": 0.0,
            "c_rm": 0.0,
            "c_sub_benefit": 0.0,
            "c_sud": 0.0,
            "internal_credits_cost": 0.0,
            "internal_credits_consumption": 0.0
        }
        for i in z:
            if i[0] in dicts_fields:
                result[dicts_fields[i[0]]] = i[1] + result[dicts_fields[i[0]]]
        
        total_credits = sum(row["credits_amount"]) if row["credits_amount"] else 0.0
        result["internal_credits_cost"] = total_credits
        result["internal_credits_consumption"] = result["internal_credits_cost"] - result[
            dicts_fields["RESELLER_MARGIN"]]
        return pd.Series(result)

    @classmethod
    def _calculate_mode1(cls, data: DataFrame):
        # 模式1: （[cost]+[credits(exclude c_rm)]）* 客户折扣
        condition = (data["mode"] == 1)
        if condition.any():
            data.loc[condition, "external_consumption"] = (
                data.loc[condition, "internal_consumption"] * data.loc[condition, "discount"].astype(float)
            )
            data.loc[condition, "discount_amount"] = data.loc[condition, "internal_credits_consumption"]

    @classmethod
    def _calculate_mode2(cls, data: DataFrame):
        # 模式2:[usage.amount] * 单价
        condition = (data["mode"] == 2)
        if condition.any():
            data.loc[condition, "external_consumption"] = (
                data.loc[condition, "usage_amount_in_pricing_units"] * data.loc[condition, "price"].astype(float)
            )

    @classmethod
    def _calculate_mode3(cls, data: DataFrame):
        # 模式3: [usage.amount] * 单价 * 折扣
        condition = (data["mode"] == 3)
        if condition.any():
            data.loc[condition, "external_consumption"] = (
                data.loc[condition, "usage_amount_in_pricing_units"] * data.loc[condition, "price"].astype(float) * data.loc[condition, "discount"].astype(float)
            )

    @classmethod
    def _calculate_mode4(cls, data: DataFrame):
        # 模式4:（[cost_at_list]+(被选择的[credits]/原厂折扣)）* 客户折扣
        condition = (data["mode"] == 4)
        if condition.any():
            # 为避免 apply 过程中的类型冲突，先确保 external_consumption 为 float
            data.loc[condition, "external_consumption"] = data.loc[condition].apply(
                CalculateService._calculate_mode4_row, axis=1
            )["external_consumption"]

    @classmethod
    def _calculate_mode4_row(cls, row: Series) -> Series:
        try:
            credit_part = 0.0
            price_val = float(row["price"]) if row["price"] is not None else 1.0
            discount_val = float(row["discount"]) if row["discount"] is not None else 1.0
            
            if row["credit_fields"]:
                for f in str(row["credit_fields"]).split('/'):
                    # 避免除以 0
                    if price_val != 0:
                        credit_part += float(row[f]) / price_val
            
            row["external_consumption"] = (float(row["cost_at_list"]) * discount_val) + (credit_part * discount_val)
            row["discount_amount"] = credit_part
            return row
        except Exception as e:
            raise Exception(f"calculate mode4 error: {e}") from e

    @classmethod
    def add_rule_tag(cls, df: DataFrame, dim_df: DataFrame):
        """
        根据维度匹配规则打标签
        """
        # 初始化列，显式指定 dtype 避免 FutureWarning
        df["mode"] = None
        df["discount"] = pd.Series([np.nan] * len(df), dtype='float64')
        df["price"] = pd.Series([np.nan] * len(df), dtype='float64')
        df["credit_fields"] = None
        df["customer_id"] = None
        df["contract_id"] = None
        
        columns = ["mode", "discount", "price", "credit_fields", "customer_id", "contract_id"]
        
        rule1 = (dim_df[PROJECT_ID].isna() & dim_df[SERVICE_DESCRIPTION].isna() & dim_df[SKU_ID].isna(), [BILLING_ACCOUNT_ID])
        rule2 = (~dim_df[PROJECT_ID].isna() & dim_df[SERVICE_DESCRIPTION].isna() & dim_df[SKU_ID].isna(), [BILLING_ACCOUNT_ID, PROJECT_ID])
        rule3 = (dim_df[PROJECT_ID].isna() & ~dim_df[SERVICE_DESCRIPTION].isna() & dim_df[SKU_ID].isna(), [BILLING_ACCOUNT_ID, SERVICE_DESCRIPTION])
        rule4 = (~dim_df[PROJECT_ID].isna() & ~dim_df[SERVICE_DESCRIPTION].isna() & dim_df[SKU_ID].isna(), [BILLING_ACCOUNT_ID, PROJECT_ID, SERVICE_DESCRIPTION])
        rule5 = (dim_df[PROJECT_ID].isna() & dim_df[SERVICE_DESCRIPTION].isna() & ~dim_df[SKU_ID].isna(), [BILLING_ACCOUNT_ID, SKU_ID])
        rule6 = (~dim_df[PROJECT_ID].isna() & dim_df[SERVICE_DESCRIPTION].isna() & ~dim_df[SKU_ID].isna(), [BILLING_ACCOUNT_ID, PROJECT_ID, SKU_ID])
        rule7 = (dim_df[PROJECT_ID].isna() & ~dim_df[SERVICE_DESCRIPTION].isna() & ~dim_df[SKU_ID].isna(), [BILLING_ACCOUNT_ID, SERVICE_DESCRIPTION, SKU_ID])
        rule8 = (~dim_df[PROJECT_ID].isna() & ~dim_df[SERVICE_DESCRIPTION].isna() & ~dim_df[SKU_ID].isna(), [BILLING_ACCOUNT_ID, PROJECT_ID, SERVICE_DESCRIPTION, SKU_ID])
        
        conditions = [rule1, rule5, rule3, rule7, rule2, rule6, rule4, rule8]
        
        for condition in conditions:
            bool_condition = condition[0]
            keys = condition[1]
            sub_dim_rule = dim_df[bool_condition]
            if sub_dim_rule.empty:
                continue
            
            # 仅保留需要的列进行 merge
            rule_df = pd.merge(df[keys], sub_dim_rule[keys + columns], on=keys, how='left')
            # 使用 update 覆盖已有值
            df.update(rule_df[columns])

    @classmethod
    def calculate(cls, df, dim_contract_df):
        billing_account_ids = df[BILLING_ACCOUNT_ID].drop_duplicates().to_list()
        dim_data_condition = (dim_contract_df[BILLING_ACCOUNT_ID].isin(billing_account_ids))
        
        cls.add_rule_tag(df, dim_contract_df[dim_data_condition])
        
        credits_df = df.apply(cls._calculate_credits_all_type, axis=1)
        data = pd.concat([df, credits_df], axis=1)
        
        # 初始化结果列为 float
        data["external_consumption"] = 0.0
        data["discount_amount"] = 0.0
        
        data["internal_cost"] = data["cost"] + data["internal_credits_cost"]
        data["internal_consumption"] = data["cost"] + data["internal_credits_consumption"]
        
        cls.extra_discount(data)
        cls._calculate_mode1(data)
        cls._calculate_mode2(data)
        cls._calculate_mode3(data)
        cls._calculate_mode4(data)
        return data

    @classmethod
    def calculate_with_credits(cls, df, dim_contract_df):
        billing_account_ids = df[BILLING_ACCOUNT_ID].drop_duplicates().to_list()
        # 修正此处的过滤逻辑
        dim_subset = dim_contract_df[dim_contract_df[BILLING_ACCOUNT_ID].isin(billing_account_ids)]
        
        cls.add_rule_tag(df, dim_subset)
        
        data = df.copy()
        # 初始化结果列为 float
        data["external_consumption"] = 0.0
        data["discount_amount"] = 0.0
        
        data["internal_cost"] = data["cost"] + data["internal_credits_cost"]
        data["internal_consumption"] = data["cost"] + data["internal_credits_consumption"]
        
        cls.extra_discount(data)
        cls._calculate_mode1(data)
        cls._calculate_mode2(data)
        cls._calculate_mode3(data)
        cls._calculate_mode4(data)
        return data

    @classmethod
    def extra_discount(cls, data):
        discount_dict = {
            0.975: ["01F0DC-F91DC5-0F0CAB", "0143DC-442DB6-FDE892", "01FEE2-46994F-B32CB9", 
                    "01D111-877AA6-FC9006", "01368B-077E67-C11E2D", "01EB13-0127DF-324A48", 
                    "013EEC-7ED413-0F0733", "018D1D-AEDA58-9E382C"],
            0.965: ["01ACBD-4B4CE4-2D688D"],
            0.95: ["01281B-3D24E6-B4D363", "01587C-263C61-84FBDB", "015C37-EF4FBF-AE3E2C", "015336-4C0FAA-732523"],
            0.88: ['01BE65-4D6A90-81C9C9', '01AEFA-0E57C7-5D22AF', '012980-39DCA3-6B08CF', 
                    '01D80B-3126BB-D0C7C1', '01D977-BDDE3C-14BE03', '01EFBF-FE25D9-1F8A1C', 
                    '01B528-640F36-FF1F84', '013A21-83F145-1DE13D', '0134F7-148D6A-A3E367', '016577-4C47C6-43BEE5'],
            28.5/27.2: ["010EDC-72FE2A-79D4CC"]
        }

        account_discount_map = {}
        for rate, accounts in discount_dict.items():
            for account in accounts:
                account_discount_map[account] = rate

        discount_series = data[BILLING_ACCOUNT_ID].map(account_discount_map)
        mask = discount_series.notna()
        if mask.any():
            data.loc[mask, "internal_cost"] *= discount_series[mask]