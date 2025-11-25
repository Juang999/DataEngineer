SELECT   
    public.riu_mstr.riu_en_id as entity_id,   
    TO_CHAR(public.riud_det.riud_dt, 'YYYYMMDD')::BIGINT as date_id,
    public.riud_det.riud_pt_id as product_id,
    CAST(public.riud_det.riud_qty AS INTEGER) as qty_receipt,
    code_name as um,   
    public.riud_det.riud_loc_id as location_id,   
    public.riu_mstr.riu_remarks as remarks,
    public.riu_mstr.riu_type2 as receipt_code,
    public.riud_det.riud_dt as created_at
from public.riud_det 
INNER JOIN public.riu_mstr ON (public.riud_det.riud_riu_oid = public.riu_mstr.riu_oid)   
INNER JOIN public.loc_mstr ON (public.riud_det.riud_loc_id = public.loc_mstr.loc_id)   
INNER JOIN public.pt_mstr ON (public.riud_det.riud_pt_id = public.pt_mstr.pt_id)   
INNER JOIN public.code_mstr ON (public.riud_det.riud_um = public.code_mstr.code_id)   
LEFT OUTER JOIN public.en_mstr ON (public.riu_mstr.riu_en_id = public.en_mstr.en_id)  
where riu_mstr.riu_type ~~* 'R'  and riu_en_id in (select user_en_id from tconfuserentity   where userid = 1)  
order by  public.riud_det.riud_dt ASC