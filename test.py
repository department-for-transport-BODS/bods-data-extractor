import funcs

def test_otc_db_cols():

    otc = funcs.fetch_otc_db()
    cols = ['reg_no', 'variation_number', 'service_number', 'current_traffic_area',
       'lic_no', 'discs_in_possession', 'authdiscs', 'granted_date',
       'exp_date', 'description', 'op_id', 'op_name', 'trading_name',
       'address', 'start_point', 'finish_point', 'via', 'effective_date',
       'received_date', 'end_date', 'service_type_other_details',
       'licence_status', 'registration_status', 'pub_text',
       'service_type_description', 'short_notice', 'subsidies_description',
       'subsidies_details', 'auth_description', 'tao_covered_by_area',
       'registration_code', 'service_code']

    assert all([col in otc.columns for col in cols])
