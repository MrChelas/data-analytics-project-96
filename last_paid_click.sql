
with
    last_visit as (
    select 
        *,
        row_number() over(partition by s.visitor_id order by s.visit_date desc) as rn
    from sessions s
    )

select 
    la.visitor_id,
    la.visit_date, 
    la.medium,  
    l.lead_id,
    l.created_at,
    l.amount,
    l.closing_reason,
    l.status_id, rn
from last_visit la
left join leads l
    on la.visitor_id = l.visitor_id
where la.medium in ('cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social') and rn = 1
order by amount desc nulls last, visit_date, medium
limit 10;
