with staged as (
    select * from {{ ref('stg_raw_jobs') }}
),

scored as (
    select
        *,
        (
            case when job_title like '%data engineer%'      then 3 else 0 end +
            case when job_title like '%analytics engineer%' then 3 else 0 end +
            case when job_title like '%etl%'                then 2 else 0 end +
            case when job_title like '%pipeline%'           then 2 else 0 end +
            case when job_title like '%senior%'             then 1 else 0 end +
            case when job_title like '%junior%'             then 1 else 0 end
        ) as ats_score,

        case
            when job_title like '%senior%' or job_title like '%lead%' then 'Senior'
            when job_title like '%junior%' or job_title like '%associate%' then 'Junior'
            else 'Mid'
        end as experience_level,

        case
            when job_title like '%data engineer%'      then 'Data Engineering'
            when job_title like '%analytics engineer%' then 'Analytics Engineering'
            when job_title like '%etl%'                then 'ETL/Pipeline'
            when job_title like '%data analyst%'       then 'Data Analytics'
            else 'Other'
        end as job_category

    from staged
    where is_ghost_job = false
)

select
    id,
    job_title,
    company_name,
    location,
    job_url,
    source_platform,
    scraped_date,
    ats_score,
    experience_level,
    job_category,
    rank() over (order by ats_score desc) as relevance_rank
from scored
order by ats_score desc