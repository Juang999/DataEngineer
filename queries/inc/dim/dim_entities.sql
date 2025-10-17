SELECT
    en_id,
    en_desc,
    en_add_date
FROM public.en_mstr
WHERE en_add_date > '{created_at}'
ORDER BY en_add_date ASC