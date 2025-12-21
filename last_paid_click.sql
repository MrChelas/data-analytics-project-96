
with 
last_visit as (
	select *,
		row_number() over(partition by s.visitor_id order by s.visit_date desc) as rn
	from sessions s
)

select 
    lv.visitor_id,
    lv.visit_date, 
    lv.medium,  
    l.lead_id,
    l.created_at,
    l.amount,
    l.closing_reason,
    l.status_id
from last_visit lv
left join leads l
	on lv.visitor_id = l.visitor_id and lv.visit_date < l.created_at
where lv.medium <> 'organic' and rn = 1
order by amount desc nulls last, visit_date, medium;