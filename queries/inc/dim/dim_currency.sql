SELECT
    cu_id AS currency_id,
    cu_code AS currency_code,
    cu_name AS currency_name,
    cu_symbol AS currency_symbol,
    cu_add_date AS created_at
FROM public.cu_mstr
WHERE cu_add_date > '{created_at}'
ORDER BY cu_id ASC