with source as (
    select * from {{ source('job_pipeline', 'raw_jobs') }}
),

cleaned as (
    select
        id,
        trim(lower(job_title))          as job_title,
        trim(company_name)              as company_name,
        trim(location)                  as location,
        job_url,
        source_platform,
        date_scraped,
        date(date_scraped)              as scraped_date,
        case
            when date_scraped < now() - interval '30 days'
            then true else false
        end                             as is_ghost_job
    from source
    where job_title is not null
    and company_name is not null
)

select * from cleaned