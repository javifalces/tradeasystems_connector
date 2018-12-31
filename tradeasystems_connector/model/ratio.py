class Ratio:
    ratio = 'ratio'
    time = 'time'
    index = time
    columns = [ratio, time]
    # %%
    closeTemp = 'close_temp'
    # %% Yearly
    fundamental_ebitda_Y = 'fundamental_ebitda_Y'
    fundamental_net_asset_value_Y = 'fundamental_net_asset_value_Y'
    fundamental_earnings_per_share_Y = 'fundamental_earnings_per_share_Y'
    fundamental_cost_of_goods_sold_Y = 'fundamental_cost_of_goods_sold_Y'
    fundamental_current_ratio_Y = 'fundamental_current_ratio_Y'
    fundamental_enterprise_value_Y = 'fundamental_enterprise_value_Y'
    fundamental_gross_profit_Y = 'fundamental_gross_profit_Y'
    fundamental_assets_Y = 'fundamental_assets_Y'
    fundamental_debt_Y = 'fundamental_debt_Y'
    fundamental_liabilities_Y = 'fundamental_liabilities_Y'
    fundamental_current_liabilities_Y = 'fundamental_current_liabilities_Y'
    fundamental_current_assets_Y = 'fundamental_current_assets_Y'
    fundamental_net_income_Y = 'fundamental_net_income_Y'
    fundamental_revenue_Y = 'fundamental_revenue_Y'
    fundamental_market_capitalization_Y = 'fundamental_market_capitalization_Y'
    fundamental_shares_Y = 'fundamental_shares_Y'
    fundamental_book_value_per_share_Y = 'fundamental_book_value_per_share_Y'
    fundamental_free_cashflow_per_share_Y = 'fundamental_free_cashflow_per_share_Y'
    fundamental_operating_cashflow_Y = 'fundamental_operating_cashflow_Y'
    fundamental_operating_assets_Y = 'fundamental_operating_assets_Y'
    fundamental_operating_liabilities_Y = 'fundamental_operating_liabilities_Y'
    fundamental_day_sales_receivables_Y = 'fundamental_day_sales_receivables_Y'
    fundamental_non_current_assets_Y = 'fundamental_non_current_assets_Y'
    fundamental_non_current_liabilities_Y = 'fundamental_non_current_liabilities_Y'
    fundamental_sales_growth_Y = 'fundamental_sales_growth_Y'
    fundamental_depreciation_Y = 'fundamental_depreciation_Y'
    fundamental_working_capital_Y = 'fundamental_working_capital_Y'
    fundamental_book_value_liabilities_Y = 'fundamental_book_value_liabilities_Y'
    fundamental_cash_and_equivalents_Y = 'fundamental_cash_and_equivalents_Y'
    fundamental_adjusted_book_value_Y = 'fundamental_adjusted_book_value_Y'
    fundamental_ebit_Y = 'fundamental_ebit_Y'
    fundamental_return_over_assets_Y = 'fundamental_return_over_assets_Y'
    fundamental_return_over_capital_Y = 'fundamental_return_over_capital_Y'
    fundamental_return_over_equity_Y = 'fundamental_return_over_equity_Y'
    fundamental_book_market_Y = 'fundamental_book_market_Y'
    fundamental_free_cashflow_Y = 'fundamental_free_cashflow_Y'
    fundamental_long_term_debt_Y = 'fundamental_long_term_debt_Y'
    fundamental_short_term_debt_Y = 'fundamental_short_term_debt_Y'
    fundamental_net_equity_issuance_Y = 'fundamental_net_equity_issuance_Y'
    fundamental_receivables_Y = 'fundamental_receivables_Y'
    fundamental_inventory_Y = 'fundamental_inventory_Y'
    # fundamental_capital_expenditure_Y = 'fundamental_capital_expenditure_Y'
    fundamental_selling_general_administrative_expenses_Y = 'fundamental_selling_general_administrative_expenses_Y'

    # calculated
    fundamental_leverage_Y = 'fundamental_leverage_Y'
    fundamental_accrual_Y = 'fundamental_accrual_Y'
    fundamental_day_sales_receivables_index_Y = 'fundamental_day_sales_receivables_index_Y'
    fundamental_gross_margin_Y = 'fundamental_gross_margin_Y'
    fundamental_gross_margin_index_Y = 'fundamental_gross_margin_index_Y'
    fundamental_asset_quality_index_Y = 'fundamental_asset_quality_index_Y'
    fundamental_sales_growth_index_Y = 'fundamental_sales_growth_index_Y'
    fundamental_depreciation_index_Y = 'fundamental_depreciation_index_Y'
    fundamental_sgai_Y = 'fundamental_sgai_Y'
    fundamental_lvgi_Y = 'fundamental_lvgi_Y'
    fundamental_roa_8_year_avg_Y = 'fundamental_roa_8_year_avg_Y'
    fundamental_roc_8_year_avg_Y = 'fundamental_roc_8_year_avg_Y'
    fundamental_fcfa_Y = 'fundamental_fcfa_Y'
    fundamental_mg_Y = 'fundamental_mg_Y'
    fundamental_liquidity_Y = 'fundamental_liquidity_Y'
    fundamental_leverageDiff_Y = 'fundamental_leverageDiff_Y'
    fundamental_roaDiff_Y = 'fundamental_roaDiff_Y'
    fundamental_fcftaDiff_Y = 'fundamental_fcftaDiff_Y'
    fundamental_marginDiff_Y = 'fundamental_marginDiff_Y'
    fundamental_turnDiff_Y = 'fundamental_turnDiff_Y'
    fundamental_ms_Y = 'fundamental_ms_Y'

    quant_returnsDiff_120 = 'quant_returnsDiff_120'
    quant_return1YFrom20 = 'quant_return1YFrom20'
    quant_std1Y = 'quant_std1Y'
    quant_return1Y = 'quant_return1Y'

    quant_list = \
        [
            quant_returnsDiff_120,
            quant_return1YFrom20,
            quant_std1Y,
            quant_return1Y
        ]

    fundamental_list_Y = [
        fundamental_ebitda_Y,
        fundamental_net_asset_value_Y,
        fundamental_earnings_per_share_Y,
        fundamental_cost_of_goods_sold_Y,
        fundamental_current_ratio_Y,
        fundamental_enterprise_value_Y,
        fundamental_gross_profit_Y,
        fundamental_assets_Y,
        fundamental_debt_Y,
        fundamental_liabilities_Y,
        fundamental_net_income_Y,
        fundamental_revenue_Y,
        fundamental_market_capitalization_Y,
        fundamental_shares_Y,
        fundamental_book_value_per_share_Y,
        fundamental_free_cashflow_per_share_Y,
        fundamental_operating_cashflow_Y,
        fundamental_operating_assets_Y,
        fundamental_operating_liabilities_Y,
        fundamental_day_sales_receivables_Y,
        fundamental_non_current_assets_Y,
        fundamental_sales_growth_Y,
        fundamental_depreciation_Y,
        fundamental_working_capital_Y,
        fundamental_book_value_liabilities_Y,
        fundamental_cash_and_equivalents_Y,
        fundamental_adjusted_book_value_Y,
        fundamental_ebit_Y,
        fundamental_return_over_assets_Y,
        fundamental_return_over_capital_Y,
        fundamental_free_cashflow_Y,
        fundamental_long_term_debt_Y,
        fundamental_short_term_debt_Y,
        fundamental_net_equity_issuance_Y,
        fundamental_receivables_Y,
        fundamental_inventory_Y,
        # fundamental_capital_expenditure_Y,
        fundamental_selling_general_administrative_expenses_Y,

        ####calculated
        fundamental_leverage_Y,
        fundamental_accrual_Y,
        fundamental_day_sales_receivables_index_Y,
        fundamental_gross_margin_Y,
        fundamental_gross_margin_index_Y,
        fundamental_asset_quality_index_Y,
        fundamental_sales_growth_index_Y,
        fundamental_depreciation_index_Y,
        fundamental_sgai_Y,
        fundamental_lvgi_Y,
        fundamental_roa_8_year_avg_Y,
        fundamental_roc_8_year_avg_Y,
        fundamental_fcfa_Y,
        fundamental_mg_Y,
        fundamental_liquidity_Y,
        fundamental_leverageDiff_Y,
        fundamental_roaDiff_Y,
        fundamental_fcftaDiff_Y,
        fundamental_marginDiff_Y,
        fundamental_turnDiff_Y,
        fundamental_ms_Y,

    ]
    # %%
    allRatios = fundamental_list_Y + quant_list
