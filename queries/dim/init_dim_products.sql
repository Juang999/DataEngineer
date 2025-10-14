SELECT
    pt_id,
    pt_dom_id,
    pt_en_id,
    pt_code,
    pt_desc1,
    CASE WHEN pt_pl_id IN (7, 4) THEN pt_desc1 ELSE '-' END AS pt_desc2,
    pt_add_date AS created_at,
    pt_class,
    TO_CHAR(pt_add_date, 'YYYYMMDD')::INT AS date_id,
    product_line.pl_desc AS product_line_name,
    CASE WHEN SUM(invc_qty) > 0 THEN detail_pricelist.pidd_price ELSE 0 END AS price,
    CAST(data_cost.invct_cost AS BIGINT) AS cost,
    YEAR(pt_year) AS launch_year
FROM public.pt_mstr product
LEFT JOIN public.pl_mstr product_line ON product_line.pl_id = product.pt_pl_id
LEFT JOIN public.pid_det relation_pricelist ON relation_pricelist.pid_pt_id = product.pt_id
RIGHT JOIN public.pi_mstr master_pricelist ON master_pricelist.pi_oid = relation_pricelist.pid_pi_oid AND master_pricelist.pi_id IN (103, 202, 304)
LEFT JOIN public.pidd_det detail_pricelist ON detail_pricelist.pidd_pid_oid = relation_pricelist.pid_oid AND pidd_payment_type = 9942
LEFT JOIN public.invc_mstr master_inventory ON master_inventory.invc_pt_id = product.pt_id
LEFT JOIN PUBLIC.invct_table data_cost ON data_cost.invct_pt_id = product.pt_id
GROUP BY
    product.pt_id,
    pt_dom_id,
    pt_en_id,
    pt_code,
    pt_desc1,
    pt_desc2,
    pt_add_date,
    pt_class,
    product_line_name,
    pidd_price,
    cost,
    pt_pl_id,
    product.pt_year
ORDER BY pt_add_date ASC