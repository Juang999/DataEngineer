SELECT
	CAST(TO_CHAR(co.casho_date, 'YYYYMMDD') AS BIGINT) AS date_id,
	co.casho_en_id AS entity_id,
	co.casho_ptnr_id AS partner_id,
	co.casho_bk_id AS fund_id,
	co.casho_code AS cashout_code,
	co.casho_remarks AS cashout_remarks,
	co.casho_amount AS cashout_amount,
	co.casho_cu_id AS currency_id,
	co.casho_exc_rate AS cashout_exc_rate,
	co.casho_amount_remains AS cashout_amount_remains,
	co.casho_amount_realization AS cashout_amount_realization,
	co.casho_req_code AS cashout_request_code,
	co.casho_req_oid AS cashout_request_oid,
	co.casho_cc_id AS copy_carbon_id,
	co.casho_is_memo AS cashout_is_memo,
	co.casho_reques_ptnr_id AS request_partner_id,
	co.casho_enduser_ptnr_id AS sales_person_id,
	co.casho_add_by AS created_by,
	co.casho_add_date AS created_at,
	cd.cashod_amount AS cashout_detail_amount,
	cd.cashod_remarks AS cashout_detail_remarks,
	co.casho_reff_code AS cashout_reference_code,
	cd.cashod_ac_id AS account_id
FROM public.casho_out co
INNER JOIN public.ptnr_mstr ptnr ON co.casho_ptnr_id = ptnr.ptnr_id
INNER JOIN public.en_mstr en ON co.casho_en_id = en.en_id
INNER JOIN public.bk_mstr bk ON co.casho_bk_id = bk.bk_id
INNER JOIN public.cu_mstr cu ON co.casho_cu_id = cu.cu_id
LEFT JOIN public.req_mstr req ON co.casho_req_oid = req.req_oid
LEFT JOIN public.cc_mstr cc ON co.casho_cc_id = cc.cc_id
LEFT JOIN public.ptnr_mstr ptnr_sold ON co.casho_reques_ptnr_id = ptnr_sold.ptnr_id
LEFT JOIN public.ptnr_mstr ptnr_sales ON co.casho_enduser_ptnr_id = ptnr_sales.ptnr_id
INNER JOIN public.cashod_detail cd ON co.casho_oid = cd.cashod_casho_oid
left join public.ac_mstr ac on ac.ac_id = cd.cashod_ac_id
WHERE EXISTS (
  SELECT 1 FROM public.tconfuserentity tcue
  WHERE tcue.userid = 1 AND tcue.user_en_id = co.casho_en_id
)
AND YEAR(co.casho_date) IN (2019, 2020, 2021, 2022, 2023, 2024, 2025)
ORDER BY co.casho_add_date ASC;