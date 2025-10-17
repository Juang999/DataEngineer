SELECT
	cast(to_char(riu_date, 'YYYYMMDD') as bigint) as date_id,
    riu_en_id as entity_id,
    riu_type2 as receive_number,
    riu_date as effective_date,
    riu_remarks as remarks,
    riud_det.riud_pt_id as product_id,
    riud_det.riud_si_id as site_id,
    riud_det.riud_loc_id as location_id,
    riud_qty as qty_adjustment,
    code_name as unitmeasure,
    CAST(riud_um_conv AS INTEGER) as um_conversion,
    CAST(riud_qty_real AS INTEGER) as qty_real,
    riud_lot_serial as lot_number,
    CAST(riud_cost AS INTEGER) as cost,
    riud_det.riud_ac_id as account_id,
    riud_det.riud_sb_id as subaccount_id,
    riud_det.riud_cc_id as cost_center_id,
    riu_add_by as created_by,
    riu_add_date as created_at,
    riu_upd_by as updated_by,
    riu_upd_date as updated_at
FROM public.riu_mstr
INNER JOIN public.riud_det ON (public.riud_det.riud_riu_oid = public.riu_mstr.riu_oid)
INNER JOIN public.pt_mstr ON (public.riud_det.riud_pt_id = public.pt_mstr.pt_id)
INNER JOIN public.code_mstr ON (public.riud_det.riud_um = public.code_mstr.code_id)
INNER JOIN public.loc_mstr ON (public.riud_det.riud_loc_id = public.loc_mstr.loc_id)
INNER JOIN public.si_mstr ON (public.riud_det.riud_si_id = public.si_mstr.si_id)
INNER JOIN public.ac_mstr ON (public.riud_det.riud_ac_id = public.ac_mstr.ac_id)
INNER JOIN public.sb_mstr ON (public.riud_det.riud_sb_id = public.sb_mstr.sb_id)
INNER JOIN public.cc_mstr ON (public.riud_det.riud_cc_id = public.cc_mstr.cc_id)
inner join en_mstr on en_id = riu_en_id
where riu_date < current_date
and riu_mstr.riu_type ~~* 'P'
and riu_en_id in (
                select user_en_id
                from tconfuserentity
                where userid = 1
)
order by created_at asc