SELECT
	CAST(TO_CHAR(ap.arpay_add_date, 'YYYYMMDD') as BIGINT) as date_id,
	ap.arpay_en_id as entity_id,
	ap.arpay_bill_to as partner_id,
	ap.arpay_code as cashin_code,
	ad.arpayd_amount as cashin_detail_amount,
	ad.arpayd_ar_ref as cashin_detail_reference,
	ar.ar_eff_date::date AS cashin_effective_date,
	ap.arpay_bk_id as fund_id,
	ap.arpay_total_amount as cashin_total_amount,
	ap.arpay_cu_id as currency_id,
	ap.arpay_remarks as cashin_remarks,
	ad.arpayd_exc_rate * ad.arpayd_amount AS cashin_amount_idr,
	so.so_sales_person as cashin_sales_id,
	ap.arpay_eff_date::date AS arpay_effective_date,
	(ap.arpay_eff_date::date - ar.ar_eff_date::date) AS day,
	ap.arpay_add_by as created_by,
	ap.arpay_add_date as created_at
FROM Public.arpay_payment ap
  INNER JOIN Public.arpayd_det ad On ap.arpay_oid = ad.arpayd_arpay_oid
  INNER JOIN Public.en_mstr en On ap.arpay_en_id = en.en_id
  INNER JOIN Public.cu_mstr cu On ap.arpay_cu_id = cu.cu_id
  INNER JOIN Public.ptnr_mstr ptnr On ap.arpay_bill_to = ptnr.ptnr_id
  INNER JOIN Public.bk_mstr bk On ap.arpay_bk_id = bk.bk_id
  INNER JOIN Public.ar_mstr ar On ad.arpayd_ar_oid = ar.ar_oid
  INNER JOIN Public.arso_so aso On ar.ar_oid = aso.arso_ar_oid
  INNER JOIN Public.so_mstr so On aso.arso_so_oid = so.so_oid
  LEFT JOIN
  (
    Select p.ptnr_id,
      p.ptnr_name
    FROM Public.ptnr_mstr p
      JOIN
      (
        Select DISTINCT so_sales_person
        FROM Public.so_mstr
) s ON p.ptnr_id = s.so_sales_person
) tinvc On tinvc.ptnr_id = so.so_sales_person
WHERE year(ap.arpay_eff_date) in (2019, 2021, 2022, 2023, 2024, 2025) AND
  ap.arpay_en_id IN (
                      SELECT user_en_id
                      FROM tconfuserentity
                      WHERE userid = 1
)
order by created_at asc