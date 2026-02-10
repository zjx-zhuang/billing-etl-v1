def get_calculation_sql(invoice_month, dim_month):
    # SQL template for monthly billing calculation
    return f"""
INSERT INTO billing.dwm_standard_daily_billing_calculated
WITH 
    -- 1. Source Data Aggregation (matches get_standard_daily_billing logic)
    source AS (
        SELECT
            usage_day,
            invoice_month,
            billing_account_id,
            service_id,
            service_description,
            sku_id,
            sku_description,
            project_id,
            project_name,
            usage_pricing_unit,
            sum(usage_amount_in_pricing_units) as usage_amount_in_pricing_units,
            currency,
            currency_conversion_rate,
            cost_type,
            sum(cost) as cost,
            sum(cost_at_list) as cost_at_list,
            sum(c_cud) as c_cud,
            sum(c_cud_db) as c_cud_db,
            sum(c_discount) as c_discount,
            sum(c_free_tier) as c_free_tier,
            sum(c_promotion) as c_promotion,
            sum(c_rm) as c_rm,
            sum(c_sub_benefit) as c_sub_benefit,
            sum(c_sud) as c_sud,
            sum(internal_credits_cost) as internal_credits_cost,
            sum(internal_credits_consumption) as internal_credits_consumption
        FROM billing.ods_standard_daily_billing
        WHERE invoice_month = '{invoice_month}'
        and project_id='ai-period-tracker'

        GROUP BY
            usage_day, invoice_month, billing_account_id, project_id, project_name,
            service_id, service_description, sku_id, sku_description,
            usage_pricing_unit, currency, currency_conversion_rate, cost_type
    ),
    
    -- 2. Dimension Rules (Split by NULL conditions as per Python logic)
    -- Rule 1: All NULL
    r1 AS (SELECT * FROM billing.dim_contract WHERE month='{dim_month}' AND project_id IS NULL AND service_description IS NULL AND sku_id IS NULL),
    -- Rule 2: Project NOT NULL
    r2 AS (SELECT * FROM billing.dim_contract WHERE month='{dim_month}' AND project_id IS NOT NULL AND service_description IS NULL AND sku_id IS NULL),
    -- Rule 3: Service NOT NULL
    r3 AS (SELECT * FROM billing.dim_contract WHERE month='{dim_month}' AND project_id IS NULL AND service_description IS NOT NULL AND sku_id IS NULL),
    -- Rule 4: Project, Service NOT NULL
    r4 AS (SELECT * FROM billing.dim_contract WHERE month='{dim_month}' AND project_id IS NOT NULL AND service_description IS NOT NULL AND sku_id IS NULL),
    -- Rule 5: SKU NOT NULL
    r5 AS (SELECT * FROM billing.dim_contract WHERE month='{dim_month}' AND project_id IS NULL AND service_description IS NULL AND sku_id IS NOT NULL),
    -- Rule 6: Project, SKU NOT NULL
    r6 AS (SELECT * FROM billing.dim_contract WHERE month='{dim_month}' AND project_id IS NOT NULL AND service_description IS NULL AND sku_id IS NOT NULL),
    -- Rule 7: Service, SKU NOT NULL
    r7 AS (SELECT * FROM billing.dim_contract WHERE month='{dim_month}' AND project_id IS NULL AND service_description IS NOT NULL AND sku_id IS NOT NULL),
    -- Rule 8: All NOT NULL
    r8 AS (SELECT * FROM billing.dim_contract WHERE month='{dim_month}' AND project_id IS NOT NULL AND service_description IS NOT NULL AND sku_id IS NOT NULL),
    
    -- 3. Join and Match Rules
    matched AS (
        SELECT
            s.usage_day,
            s.invoice_month,
            s.billing_account_id,
            s.service_id,
            s.service_description,
            s.sku_id,
            s.sku_description,
            s.project_id,
            s.project_name,
            s.usage_pricing_unit,
            s.usage_amount_in_pricing_units,
            s.currency,
            s.currency_conversion_rate,
            s.cost_type,
            s.cost,
            s.cost_at_list,
            s.c_cud,
            s.c_cud_db,
            s.c_discount,
            s.c_free_tier,
            s.c_promotion,
            s.c_rm,
            s.c_sub_benefit,
            s.c_sud,
            s.internal_credits_cost,
            s.internal_credits_consumption,
            -- Coalesce based on priority: 8 > 4 > 6 > 2 > 7 > 3 > 5 > 1
            COALESCE(d8.mode, d4.mode, d6.mode, d2.mode, d7.mode, d3.mode, d5.mode, d1.mode) as mode,
            COALESCE(d8.discount, d4.discount, d6.discount, d2.discount, d7.discount, d3.discount, d5.discount, d1.discount) as rule_discount,
            COALESCE(d8.price, d4.price, d6.price, d2.price, d7.price, d3.price, d5.price, d1.price) as rule_price,
            COALESCE(d8.credit_fields, d4.credit_fields, d6.credit_fields, d2.credit_fields, d7.credit_fields, d3.credit_fields, d5.credit_fields, d1.credit_fields) as credit_fields,
            COALESCE(d8.customer_id, d4.customer_id, d6.customer_id, d2.customer_id, d7.customer_id, d3.customer_id, d5.customer_id, d1.customer_id) as customer_id,
            COALESCE(d8.contract_id, d4.contract_id, d6.contract_id, d2.contract_id, d7.contract_id, d3.contract_id, d5.contract_id, d1.contract_id) as contract_id,
            
            -- Internal Cost Calculation
            (s.cost + s.internal_credits_cost) as internal_cost,
            (s.cost + s.internal_credits_consumption) as internal_consumption_base,
            
            -- Extra Discount (Hardcoded in Python, moved here)
            CASE 
                WHEN s.billing_account_id IN ('01F0DC-F91DC5-0F0CAB', '0143DC-442DB6-FDE892', '01FEE2-46994F-B32CB9', '01D111-877AA6-FC9006', '01368B-077E67-C11E2D', '01EB13-0127DF-324A48', '013EEC-7ED413-0F0733', '018D1D-AEDA58-9E382C') THEN 0.975
                WHEN s.billing_account_id IN ('01ACBD-4B4CE4-2D688D') THEN 0.965
                WHEN s.billing_account_id IN ('01281B-3D24E6-B4D363', '01587C-263C61-84FBDB', '015C37-EF4FBF-AE3E2C', '015336-4C0FAA-732523') THEN 0.95
                WHEN s.billing_account_id IN ('01BE65-4D6A90-81C9C9', '01AEFA-0E57C7-5D22AF', '012980-39DCA3-6B08CF', '01D80B-3126BB-D0C7C1', '01D977-BDDE3C-14BE03', '01EFBF-FE25D9-1F8A1C', '01B528-640F36-FF1F84', '013A21-83F145-1DE13D', '0134F7-148D6A-A3E367', '016577-4C47C6-43BEE5') THEN 0.88
                WHEN s.billing_account_id IN ('010EDC-72FE2A-79D4CC') THEN (28.5/27.2)
                ELSE 1.0
            END as extra_discount_factor
            
        FROM source s
        LEFT JOIN r1 d1 ON s.billing_account_id = d1.billing_account_id
        LEFT JOIN r2 d2 ON s.billing_account_id = d2.billing_account_id AND s.project_id = d2.project_id
        LEFT JOIN r3 d3 ON s.billing_account_id = d3.billing_account_id AND s.service_description = d3.service_description
        LEFT JOIN r4 d4 ON s.billing_account_id = d4.billing_account_id AND s.project_id = d4.project_id AND s.service_description = d4.service_description
        LEFT JOIN r5 d5 ON s.billing_account_id = d5.billing_account_id AND s.sku_id = d5.sku_id
        LEFT JOIN r6 d6 ON s.billing_account_id = d6.billing_account_id AND s.project_id = d6.project_id AND s.sku_id = d6.sku_id
        LEFT JOIN r7 d7 ON s.billing_account_id = d7.billing_account_id AND s.service_description = d7.service_description AND s.sku_id = d7.sku_id
        LEFT JOIN r8 d8 ON s.billing_account_id = d8.billing_account_id AND s.project_id = d8.project_id AND s.service_description = d8.service_description AND s.sku_id = d8.sku_id
    ),
    
    -- 4. Apply Calculations
    calculated AS (
        SELECT
            *,
            -- Apply extra discount to internal_consumption
            (internal_consumption_base * extra_discount_factor) as internal_consumption_final,
            
            -- Helper for Mode 4 Credit Sum
            (
                if(has(splitByChar('/', coalesce(credit_fields, '')), 'c_cud'), c_cud, 0) +
                if(has(splitByChar('/', coalesce(credit_fields, '')), 'c_cud_db'), c_cud_db, 0) +
                if(has(splitByChar('/', coalesce(credit_fields, '')), 'c_discount'), c_discount, 0) +
                if(has(splitByChar('/', coalesce(credit_fields, '')), 'c_free_tier'), c_free_tier, 0) +
                if(has(splitByChar('/', coalesce(credit_fields, '')), 'c_promotion'), c_promotion, 0) +
                if(has(splitByChar('/', coalesce(credit_fields, '')), 'c_rm'), c_rm, 0) +
                if(has(splitByChar('/', coalesce(credit_fields, '')), 'c_sub_benefit'), c_sub_benefit, 0) +
                if(has(splitByChar('/', coalesce(credit_fields, '')), 'c_sud'), c_sud, 0)
            ) as mode4_credit_part
            
        FROM matched
    )

SELECT
    usage_day,
    invoice_month,
    billing_account_id,
    cast(customer_id as Nullable(String)),
    cast(contract_id as Nullable(String)),
    service_id,
    service_description,
    sku_id,
    sku_description,
    project_id,
    project_name,
    usage_pricing_unit,
    usage_amount_in_pricing_units,
    currency,
    currency_conversion_rate,
    cost_type,
    cost,
    cost_at_list,
    c_cud, c_cud_db, c_discount, c_free_tier, c_promotion, c_rm, c_sub_benefit, c_sud,
    internal_credits_cost,
    internal_credits_consumption,
    internal_cost,
    internal_consumption_base as internal_consumption, -- Original internal consumption
    
    -- external_consumption Calculation
    CASE
        -- Mode 1: ([cost] + [credits(exclude c_rm)]) * discount
        WHEN mode = 1 THEN internal_consumption_final * toFloat64(rule_discount)
        
        -- Mode 2: [usage.amount] * price
        WHEN mode = 2 THEN usage_amount_in_pricing_units * toFloat64(rule_price)
        
        -- Mode 3: [usage.amount] * price * discount
        WHEN mode = 3 THEN usage_amount_in_pricing_units * toFloat64(rule_price) * toFloat64(rule_discount)
        
        -- Mode 4: ([cost_at_list] + (selected_credits / price)) * discount
        WHEN mode = 4 THEN 
            (
                toFloat64(cost_at_list) * toFloat64(rule_discount) + 
                (
                    if(toFloat64(rule_price) != 0, mode4_credit_part / toFloat64(rule_price), 0)
                ) * toFloat64(rule_discount)
            )
            
        ELSE 0.0
    END as external_consumption,
    
    -- discount_amount Calculation
    CASE
        WHEN mode = 1 THEN internal_credits_consumption
        WHEN mode = 4 THEN if(toFloat64(rule_price) != 0, mode4_credit_part / toFloat64(rule_price), 0)
        ELSE 0.0
    END as discount_amount,
    
    coalesce(mode, 0) as mode,
    coalesce(rule_price, 0.0) as price,
    coalesce(rule_discount, 0.0) as discount,
    coalesce(credit_fields, '') as credit_fields,
    now() as etl_time

FROM calculated
"""
