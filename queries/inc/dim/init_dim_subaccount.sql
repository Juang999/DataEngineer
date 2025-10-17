SELECT
	sb_id AS subaccount_id,
	sb_en_id AS entity_id,
	sb_code AS subaccount_code,
	sb_desc AS subaccount_desc,
	sb_add_by AS created_by,
	sb_add_date AS created_at
FROM PUBLIC.sb_mstr sm
ORDER BY sb_id ASC