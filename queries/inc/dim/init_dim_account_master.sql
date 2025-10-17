SELECT
	ac_id AS account_id,
	ac_code AS account_code,
	ac_name AS account_name,
	ac_parent AS account_parent_id,
	ac_type AS account_type,
	ac_is_sumlevel AS sum_level,
	ac_sign AS account_sign,
	CASE WHEN ac_active = 'Y' THEN TRUE ELSE FALSE END is_active,
	ac_cu_id AS currency_id,
	CASE WHEN ac_is_budget = 'Y' THEN TRUE ELSE FALSE END AS account_is_budget,
	ac_code_hirarki AS hierarchy_code,
	ac_add_date AS created_at
FROM public.ac_mstr
ORDER BY account_id ASC