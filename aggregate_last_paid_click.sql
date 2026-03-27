with ads_data as (
	select
		date(campaign_date) as visit_date,
		utm_source,
		utm_medium,
		utm_campaign,
		sum(daily_spent) as total_spent
	from vk_ads
	group by date(campaign_date), utm_source, utm_medium, utm_campaign

UNION ALL

	select
		date(campaign_date) as visit_date,
		utm_source,
		utm_medium,
		utm_campaign,
		sum(daily_spent) as total_spent
	from ya_ads
	group by date(campaign_date), utm_source, utm_medium, utm_campaign
),

last_paid_click as (
select
    s.visitor_id,
    date(s.visit_date) as visit_date, 
    s.medium,
    s.campaign,
    s.source,  
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
),

agg_data as (
    select
	source as utm_source,
	medium as utm_medium,
	campaign as utm_campaign,
	visit_date,
	count(visitor_id) as visitors_count,
	count(created_at) filter(where created_at is not null) as leads_count,
	count(created_at) filter(where status_id = 142) as purchases_count, --visitor_id--
	sum(amount) filter(where status_id = 142) as revenue
    from last_paid_click
    where rn = 1
    group by utm_source, utm_medium, utm_campaign, visit_date
)

select
    a.visit_date,
    a.visitors_count,
    a.utm_source,
    a.utm_medium,
    a.utm_campaign,
    coalesce(m.total_spent, 0) as total_cost,
    a.leads_count,
    a.purchases_count, 
    a.revenue
from agg_data as a
left join ads_data as m
on a.visit_date = m.visit_date and
lower(a.utm_source) = lower(m.utm_source) and
lower(a.utm_medium) = lower(m.utm_medium) and 
lower(a.utm_campaign) = lower(m.utm_campaign)
order by 
    revenue desc nulls last, visit_date, visitors_count desc, utm_source, utm_medium, utm_campaign
limit 15;