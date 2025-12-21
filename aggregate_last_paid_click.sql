with 
vk1 as (
select
date_trunc('day', campaign_date) as date_vk,
utm_source, utm_medium, utm_campaign,
sum(daily_spent) as vk_spent
from vk_ads
group by 1, 2, 3, 4
order by 1
),

ya1 as (
select
date_trunc('day', campaign_date) as date_ya,
utm_source, utm_medium, utm_campaign,
sum(daily_spent) as ya_spent
from ya_ads
group by 1, 2, 3, 4
order by 1
),

purchases as (
select
date_trunc('day', s.visit_date) as visit_date,
source, medium, campaign,
count(s.visitor_id) as visitors_count,
count(created_at) as leads_count,
count(created_at) filter(where status_id = 142) as purchases_count,
sum(coalesce(l.amount, 0)) as revenue
from (select *, row_number() over(partition by visitor_id order by visit_date desc) as rn
	  from sessions) s
left join leads l
on s.visitor_id = l.visitor_id
where s.medium in ('cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social') and rn = 1
group by 1, 2, 3, 4
)

select 
visit_date,
visitors_count,
source, medium, campaign,
coalesce(vk_spent, 0) + coalesce(ya_spent, 0) as total_cost,
leads_count, purchases_count, revenue
from purchases p
left join vk1
on p.visit_date = vk1.date_vk and p.source = vk1.utm_source and p.campaign = vk1.utm_campaign
left join ya1
on p.visit_date = ya1.date_ya and p.source = ya1.utm_source and p.campaign = ya1.utm_campaign
order by visit_date, visitors_count desc, source, medium, campaign, revenue desc nulls last
;