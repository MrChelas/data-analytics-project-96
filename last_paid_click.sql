with 
last_visit as (
select
    s.visitor_id,
    s.visit_date, 
    s.medium as UTM_medium,
    s.campaign as UTM_campaign,
    s.source as UTM_source,  
    l.lead_id,
    l.created_at,
    l.amount,
    l.closing_reason,
    l.status_id,
    row_number() over(partition by s.visitor_id order by s.visit_date desc) as rn
from sessions as s
left join leads l
    on s.visitor_id = l.visitor_id and s.visit_date <= l.created_at
where s.medium in ('cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social')
)

select 
    visitor_id,
    date(visit_date),
    UTM_source,
    UTM_medium,
    UTM_campaign,  
    lead_id,
    created_at,
    amount,
    closing_reason,
    status_id
from last_visit
where rn = 1
order by amount desc nulls last, visit_date, UTM_source, UTM_medium, UTM_campaign
limit 10;