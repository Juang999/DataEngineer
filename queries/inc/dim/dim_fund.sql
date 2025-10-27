SELECT
	bk_id AS fund_id,
	bk_en_id AS entity_id,
	bk_code AS fund_code,
	bk_name AS fund_name,
	bk_cu_id AS currency_id,
	bk_ac_id AS account_id,
	bk_cc_id AS copy_carbon_id,
	bk_sb_id AS subaccount_id,
	bk_add_by AS created_by,
	bk_add_date AS created_at
FROM PUBLIC.bk_mstr bm
WHERE bk_add_date > '{created_at}'
ORDER BY bk_id ASC;