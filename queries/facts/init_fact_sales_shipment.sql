select
	CAST(to_char(so_date, 'YYYYMMDD') as BIGINT) as date_id,
    soship_en_id as entity_id,
    so_code,
    ptnr_mstr.ptnr_id as partner_id,
    soship_code,
    soship_date,
    soship_si_id as site_id,
    soship_is_shipment,
    soshipd_seq,
    soship_cu_id as currency_id,
    so_exc_rate,
    sod_pt_id as product_id,
    sod_cost,
    sod_taxable,
    sod_tax_inc,
    sod_sales_unit,
    (sod_sales_unit *(- 1) * soshipd_qty) as sod_sales_unit_total,
    tax_mstr.code_name as tax_name,
    soshipd_qty * - 1 as soshipd_qty,
    sod_price,
    soshipd_qty * - 1 * sod_price as sales_ttl,
    sod_disc,
    soshipd_qty * - 1 * sod_cost as total_cost,
    soshipd_qty * - 1 * sod_price * sod_disc as disc_value,
    case upper(sod_tax_inc)
        when 'N' then soshipd_qty * - 1 * sod_price
        when 'Y' then (soshipd_qty * - 1 * sod_price) * cast ((100.0 / 110.0) as numeric (26, 8))
    end as price_fp,
    case upper(sod_tax_inc)
        when 'N' then soshipd_qty * - 1 * sod_price * sod_disc
        when 'Y' then (soshipd_qty * - 1 * sod_price) * cast ((100.0 / 110.0) as numeric (26, 8)) * sod_disc
    end as disc_fp,
    case upper(sod_tax_inc)
        when 'N' then (soshipd_qty * - 1 * sod_price) -(soshipd_qty * - 1 * sod_price * sod_disc)
        when 'Y' then ((soshipd_qty * - 1 * sod_price) * cast ((100.0 / 110.0) as numeric (26, 8))) -((soshipd_qty * - 1 * sod_price) * cast ((100.0
        / 110.0) as numeric (26, 8)) * sod_disc)
    end as dpp,
    case upper(sod_tax_inc)
        when 'N' then ((soshipd_qty * - 1 * sod_price) -(soshipd_qty * - 1 * sod_price * sod_disc)) -(soshipd_qty * - 1 * sod_cost)
        when 'Y' then (((soshipd_qty * - 1 * sod_price) * cast ((100.0 / 110.0) as numeric (26, 8))) -((soshipd_qty * - 1 * sod_price) * cast ((100.0
        / 110.0) as numeric (26, 8)) * sod_disc)) -(soshipd_qty * - 1 * sod_cost)
    end as gross_profit,
    case pl_code
        when '990000000001' then case upper(sod_tax_inc)
                                    when 'N' then ((soshipd_qty * - 1 * sod_price) -(soshipd_qty * - 1 * sod_price * sod_disc)) * 0.1
                                    when 'Y' then ((((soshipd_qty * - 1 * sod_price) * cast ((100.0 / 110.0) as numeric (26, 8))) -((soshipd_qty * - 1
                                    * sod_price) * cast ((100.0 / 110.0) as numeric (26, 8)) * sod_disc))) * 0.1
                                end
    end as ppn_bayar,
    case pl_code
        when '990000000002' then case upper(sod_tax_inc)
                                    when 'N' then ((soshipd_qty * - 1 * sod_price) -(soshipd_qty * - 1 * sod_price * sod_disc)) * 0.1
                                    when 'Y' then (((soshipd_qty * - 1 * sod_price) * cast ((100.0 / 110.0) as numeric (26, 8))) -((soshipd_qty * - 1
                                    * sod_price) * cast ((100.0 / 110.0) as numeric (26, 8)) * sod_disc))
                                end
    end as ppn_bebas,
    um_mstr.code_name as um_name,
    soshipd_loc_id as location_id,
    reason_mstr.code_name as reason_name,
    ar_code,
    ar_date,
    ti_code,
    so_sales_person as sales_person_id,
    ti_date,
    pay_type.code_name as pay_type_desc,
    so_pi_id AS pricelist_id,
    case
        when ptnr_mstr.ptnr_is_ps = 'Y' then 'PS'
        else 'NON PS'
    end as ps_status,
    'exapro' as data_source,
    soship_add_date as created_at
FROM public.soship_mstr
    inner join soshipd_det on soshipd_soship_oid = soship_oid
    inner join so_mstr on so_oid = soship_so_oid
    inner join pi_mstr on so_pi_id = pi_id
    inner join sod_det on sod_oid = soshipd_sod_oid
    inner join ptnr_mstr sales_mstr on sales_mstr.ptnr_id = so_sales_person
    inner join en_mstr on en_id = soship_en_id
    inner join si_mstr on si_id = soship_si_id
    inner join ptnr_mstr on ptnr_mstr.ptnr_id = so_ptnr_id_sold
    inner join pt_mstr on pt_id = sod_pt_id
    inner join code_mstr as um_mstr on um_mstr.code_id = soshipd_um
    inner join loc_mstr on loc_id = soshipd_loc_id
    inner join cu_mstr on cu_id = so_cu_id
    inner join code_mstr as tax_mstr on tax_mstr.code_id = sod_tax_class
    inner join code_mstr pay_type on pay_type.code_id = so_pay_type
    left outer join code_mstr as reason_mstr on reason_mstr.code_id = soshipd_rea_code_id
    left outer join ars_ship on ars_soshipd_oid = soshipd_oid
    left outer join ar_mstr on ar_oid = ars_ar_oid
    left outer join tia_ar on tia_ar_oid = ar_oid
    left outer join ti_mstr on ti_oid = tia_ti_oid
    left outer join ptnrg_grp on ptnrg_grp.ptnrg_id = ptnr_mstr.ptnr_ptnrg_id
    inner join pl_mstr on pl_id = pt_pl_id
where year(soship_add_date)  in (2019, 2020, 2021, 2022, 2023, 2024, 2025) and
    pay_type.code_usr_1 <> '0' and
    left (en_desc, 3) <> 'CMD' and
    so_en_id in (
                    select user_en_id
                    from tconfuserentity
                    where userid = 1
    )
union all
SELECT
	CAST(to_char(so_date, 'YYYYMMDD') as BIGINT) as date_id,
    soship_en_id as entity_id,
    so_code,
    ptnr_mstr.ptnr_id as partner_id,
    soship_code,
    soship_date,
    soship_si_id as site_id,
    soship_is_shipment,
    soshipd_seq,
    soship_cu_id as currency_id,
    so_exc_rate,
    sod_pt_id as product_id,
    sod_cost,
    sod_taxable,
    sod_tax_inc,
    sod_sales_unit,
    (sod_sales_unit *(- 1) * soshipd_qty) as sod_sales_unit_total,
    tax_mstr.code_name as tax_name,
    soshipd_qty * - 1 as soshipd_qty,
    sod_price,
    soshipd_qty * - 1 * sod_price as sales_ttl,
    sod_disc,
    soshipd_qty * - 1 * sod_cost as total_cost,
    soshipd_qty * - 1 * sod_price * sod_disc as disc_value,
    case upper(sod_tax_inc)
        when 'N' then soshipd_qty * - 1 * sod_price
        when 'Y' then (soshipd_qty * - 1 * sod_price) * cast ((100.0 / 110.0) as numeric (26, 8))
    end as price_fp,
    case upper(sod_tax_inc)
        when 'N' then soshipd_qty * - 1 * sod_price * sod_disc
        when 'Y' then (soshipd_qty * - 1 * sod_price) * cast ((100.0 / 110.0) as numeric (26, 8)) * sod_disc
    end as disc_fp,
    case upper(sod_tax_inc)
        when 'N' then (soshipd_qty * - 1 * sod_price) -(soshipd_qty * - 1 * sod_price * sod_disc)
        when 'Y' then ((soshipd_qty * - 1 * sod_price) * cast ((100.0 / 110.0) as numeric (26, 8))) -((soshipd_qty * - 1 * sod_price) * cast ((100.0
        / 110.0) as numeric (26, 8)) * sod_disc)
    end as dpp,
    case upper(sod_tax_inc)
        when 'N' then ((soshipd_qty * - 1 * sod_price) -(soshipd_qty * - 1 * sod_price * sod_disc)) -(soshipd_qty * - 1 * sod_cost)
        when 'Y' then (((soshipd_qty * - 1 * sod_price) * cast ((100.0 / 110.0) as numeric (26, 8))) -((soshipd_qty * - 1 * sod_price) * cast ((100.0
        / 110.0) as numeric (26, 8)) * sod_disc)) -(soshipd_qty * - 1 * sod_cost)
    end as gross_profit,
    case pl_code
        when '990000000001' then case upper(sod_tax_inc)
                                    when 'N' then ((soshipd_qty * - 1 * sod_price) -(soshipd_qty * - 1 * sod_price * sod_disc)) * 0.1
                                    when 'Y' then ((((soshipd_qty * - 1 * sod_price) * cast ((100.0 / 110.0) as numeric (26, 8))) -((soshipd_qty * - 1
                                    * sod_price) * cast ((100.0 / 110.0) as numeric (26, 8)) * sod_disc))) * 0.1
                                end
    end as ppn_bayar,
    case pl_code
        when '990000000002' then case upper(sod_tax_inc)
                                    when 'N' then ((soshipd_qty * - 1 * sod_price) -(soshipd_qty * - 1 * sod_price * sod_disc)) * 0.1
                                    when 'Y' then (((soshipd_qty * - 1 * sod_price) * cast ((100.0 / 110.0) as numeric (26, 8))) -((soshipd_qty * - 1
                                    * sod_price) * cast ((100.0 / 110.0) as numeric (26, 8)) * sod_disc))
                                end
    end as ppn_bebas,
    um_mstr.code_name as um_name,
    soshipd_loc_id as location_id,
    reason_mstr.code_name as reason_name,
    null as ar_code,
    null as ar_date,
    ti_code,
    so_sales_person as sales_person_id,
    ti_date,
    pay_type.code_name as pay_type_desc,
    so_pi_id AS pricelist_id,
    case
        when ptnr_mstr.ptnr_is_ps = 'Y' then 'PS'
        else 'NON PS'
    end as ps_status,
    'exapro' as data_source,
    soship_add_date as created_at
FROM public.soship_mstr
    inner join soshipd_det on soshipd_soship_oid = soship_oid
    inner join so_mstr on so_oid = soship_so_oid
    inner join pi_mstr on so_pi_id = pi_id
    inner join sod_det on sod_oid = soshipd_sod_oid
    inner join ptnr_mstr sales_mstr on sales_mstr.ptnr_id = so_sales_person
    inner join en_mstr on en_id = soship_en_id
    inner join si_mstr on si_id = soship_si_id
    inner join ptnr_mstr on ptnr_mstr.ptnr_id = so_ptnr_id_sold
    inner join pt_mstr on pt_id = sod_pt_id
    inner join code_mstr as um_mstr on um_mstr.code_id = soshipd_um
    inner join loc_mstr on loc_id = soshipd_loc_id
    inner join cu_mstr on cu_id = so_cu_id
    inner join code_mstr as tax_mstr on tax_mstr.code_id = sod_tax_class
    inner join code_mstr pay_type on pay_type.code_id = so_pay_type
    left outer join code_mstr as reason_mstr on reason_mstr.code_id = soshipd_rea_code_id
    left outer join ars_ship on ars_soshipd_oid = soshipd_oid
    left outer join ar_mstr on ar_oid = ars_ar_oid
    left outer join tis_soship on tis_soship_oid = soship_oid
    left outer join ti_mstr on ti_oid = tis_ti_oid
    left outer join ptnrg_grp on ptnrg_grp.ptnrg_id = ptnr_mstr.ptnr_ptnrg_id
    inner join pl_mstr on pl_id = pt_pl_id
where year(soship_add_date) in (2019, 2020, 2021, 2022, 2023, 2024, 2025) and
    pay_type.code_usr_1 = '0' and
    so_en_id in (
                    select user_en_id
                    from tconfuserentity
                    where userid = 1
    )
union all
SELECT
	CAST(to_char(so_date, 'YYYYMMDD') as BIGINT) as date_id,
    soship_en_id as entity_id,
    so_code,
    ptnr_mstr.ptnr_id as partner_id,
    soship_code,
    soship_date,
    soship_si_id as site_id,
    soship_is_shipment,
    soshipd_seq,
    soship_cu_id as currency_id,
    so_exc_rate,
    sod_pt_id as product_id,
    sod_cost,
    sod_taxable,
    sod_tax_inc,
    sod_sales_unit,
    (sod_sales_unit *(- 1) * soshipd_qty) as sod_sales_unit_total,
    tax_mstr.code_name as tax_name,
    soshipd_qty * - 1 as soshipd_qty,
    sod_price,
    soshipd_qty * - 1 * sod_price as sales_ttl,
    sod_disc,
    soshipd_qty * - 1 * sod_cost as total_cost,
    soshipd_qty * - 1 * sod_price * sod_disc as disc_value,
    case upper(sod_tax_inc)
        when 'N' then soshipd_qty * - 1 * sod_price
        when 'Y' then (soshipd_qty * - 1 * sod_price) * cast ((100.0 / 110.0) as numeric (26, 8))
    end as price_fp,
    case upper(sod_tax_inc)
        when 'N' then soshipd_qty * - 1 * sod_price * sod_disc
        when 'Y' then (soshipd_qty * - 1 * sod_price) * cast ((100.0 / 110.0) as numeric (26, 8)) * sod_disc
    end as disc_fp,
    case upper(sod_tax_inc)
        when 'N' then (soshipd_qty * - 1 * sod_price) -(soshipd_qty * - 1 * sod_price * sod_disc)
        when 'Y' then ((soshipd_qty * - 1 * sod_price) * cast ((100.0 / 110.0) as numeric (26, 8))) -((soshipd_qty * - 1 * sod_price) * cast ((100.0
        / 110.0) as numeric (26, 8)) * sod_disc)
    end as dpp,
    case upper(sod_tax_inc)
        when 'N' then ((soshipd_qty * - 1 * sod_price) -(soshipd_qty * - 1 * sod_price * sod_disc)) -(soshipd_qty * - 1 * sod_cost)
        when 'Y' then (((soshipd_qty * - 1 * sod_price) * cast ((100.0 / 110.0) as numeric (26, 8))) -((soshipd_qty * - 1 * sod_price) * cast ((100.0
        / 110.0) as numeric (26, 8)) * sod_disc)) -(soshipd_qty * - 1 * sod_cost)
    end as gross_profit,
    case pl_code
        when '990000000001' then case upper(sod_tax_inc)
                                    when 'N' then ((soshipd_qty * - 1 * sod_price) -(soshipd_qty * - 1 * sod_price * sod_disc)) * 0.1
                                    when 'Y' then ((((soshipd_qty * - 1 * sod_price) * cast ((100.0 / 110.0) as numeric (26, 8))) -((soshipd_qty * - 1
                                    * sod_price) * cast ((100.0 / 110.0) as numeric (26, 8)) * sod_disc))) * 0.1
                                end
    end as ppn_bayar,
    case pl_code
        when '990000000002' then case upper(sod_tax_inc)
                                    when 'N' then ((soshipd_qty * - 1 * sod_price) -(soshipd_qty * - 1 * sod_price * sod_disc)) * 0.1
                                    when 'Y' then (((soshipd_qty * - 1 * sod_price) * cast ((100.0 / 110.0) as numeric (26, 8))) -((soshipd_qty * - 1
                                    * sod_price) * cast ((100.0 / 110.0) as numeric (26, 8)) * sod_disc))
                                end
    end as ppn_bebas,
    um_mstr.code_name as um_name,
    soshipd_loc_id as location_id,
    reason_mstr.code_name as reason_name,
    null as ar_code,
    null as ar_date,
    ti_code,
    so_sales_person as sales_person_id,
    ti_date,
    pay_type.code_name as pay_type_desc,
    so_pi_id AS pricelist_id,
    case
        when ptnr_mstr.ptnr_is_ps = 'Y' then 'PS'
        else 'NON PS'
    end as ps_status,
    'exapro' as data_source,
    soship_add_date as created_at
FROM public.soship_mstr
    inner join soshipd_det on soshipd_soship_oid = soship_oid
    inner join so_mstr on so_oid = soship_so_oid
    inner join pi_mstr on so_pi_id = pi_id
    inner join sod_det on sod_oid = soshipd_sod_oid
    inner join ptnr_mstr sales_mstr on sales_mstr.ptnr_id = so_sales_person
    inner join en_mstr on en_id = soship_en_id
    inner join si_mstr on si_id = soship_si_id
    inner join ptnr_mstr on ptnr_mstr.ptnr_id = so_ptnr_id_sold
    inner join pt_mstr on pt_id = sod_pt_id
    inner join code_mstr as um_mstr on um_mstr.code_id = soshipd_um
    inner join loc_mstr on loc_id = soshipd_loc_id
    inner join cu_mstr on cu_id = so_cu_id
    inner join code_mstr as tax_mstr on tax_mstr.code_id = sod_tax_class
    inner join code_mstr pay_type on pay_type.code_id = so_pay_type
    left outer join code_mstr as reason_mstr on reason_mstr.code_id = soshipd_rea_code_id
    left outer join tis_soship on tis_soship_oid = soship_oid
    left outer join ti_mstr on ti_oid = tis_ti_oid
    left outer join ptnrg_grp on ptnrg_grp.ptnrg_id = ptnr_mstr.ptnr_ptnrg_id
    inner join pl_mstr on pl_id = pt_pl_id
where year(soship_add_date)  in (2019, 2020, 2021, 2022, 2023, 2024, 2025) and
    pay_type.code_usr_1 <> '0' and
    left (en_desc, 3) = 'CMD' and
    so_en_id in (
                    select user_en_id
                    from tconfuserentity
                    where userid = 1
    )