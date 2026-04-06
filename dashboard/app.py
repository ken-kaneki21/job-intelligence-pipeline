import streamlit as st
import psycopg2
import pandas as pd
import plotly.express as px

st.set_page_config(
    page_title="Job Intelligence Dashboard",
    page_icon="🎯",
    layout="wide"
)

def get_connection():
    return psycopg2.connect(
        host="localhost",
        port=5432,
        database="job_pipeline",
        user="saurabh",
        password="password123"
    )

@st.cache_data(ttl=300)
def load_jobs():
    conn = get_connection()
    df = pd.read_sql("""
        SELECT 
            job_title,
            company_name,
            location,
            job_url,
            source_platform,
            scraped_date,
            ats_score,
            experience_level,
            job_category,
            relevance_rank
        FROM analytics.mart_jobs
        ORDER BY relevance_rank
    """, conn)
    conn.close()
    return df

@st.cache_data(ttl=300)
def load_applications():
    conn = get_connection()
    df = pd.read_sql("""
        SELECT * FROM applications
        ORDER BY applied_date DESC
    """, conn)
    conn.close()
    return df

# ── HEADER ──
st.title("🎯 Job Intelligence Dashboard")
st.markdown("*Real-time job market intelligence powered by Airflow + PostgreSQL + dbt*")
st.divider()

# ── LOAD DATA ──
try:
    jobs_df = load_jobs()
    apps_df = load_applications()

    # ── METRICS ROW ──
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Jobs Found", len(jobs_df))
    with col2:
        st.metric("DE Roles", len(jobs_df[jobs_df['job_category'] == 'Data Engineering']))
    with col3:
        st.metric("Applications Sent", len(apps_df))
    with col4:
        avg_score = round(jobs_df['ats_score'].mean(), 1)
        st.metric("Avg ATS Score", avg_score)

    st.divider()

    # ── TWO COLUMNS ──
    left, right = st.columns([2, 1])

    with left:
        st.subheader("🏆 Top Ranked Jobs")
        top_jobs = jobs_df.head(20)[['relevance_rank', 'job_title', 'company_name', 'ats_score', 'experience_level', 'job_category']]
        st.dataframe(top_jobs, use_container_width=True, hide_index=True)

    with right:
        st.subheader("📊 Jobs by Category")
        cat_counts = jobs_df['job_category'].value_counts().reset_index()
        cat_counts.columns = ['Category', 'Count']
        fig = px.pie(cat_counts, values='Count', names='Category', hole=0.4)
        fig.update_layout(margin=dict(t=0, b=0, l=0, r=0))
        st.plotly_chart(fig, use_container_width=True)

    st.divider()

    # ── CHARTS ROW ──
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("🏢 Top Companies Hiring")
        company_counts = jobs_df['company_name'].value_counts().head(10).reset_index()
        company_counts.columns = ['Company', 'Openings']
        fig2 = px.bar(company_counts, x='Openings', y='Company', orientation='h',
                      color='Openings', color_continuous_scale='Blues')
        fig2.update_layout(margin=dict(t=0, b=0))
        st.plotly_chart(fig2, use_container_width=True)

    with col2:
        st.subheader("📈 Experience Level Distribution")
        exp_counts = jobs_df['experience_level'].value_counts().reset_index()
        exp_counts.columns = ['Level', 'Count']
        fig3 = px.bar(exp_counts, x='Level', y='Count',
                      color='Level',
                      color_discrete_map={'Junior': '#2ECC71', 'Mid': '#3498DB', 'Senior': '#E74C3C'})
        fig3.update_layout(margin=dict(t=0, b=0), showlegend=False)
        st.plotly_chart(fig3, use_container_width=True)

    st.divider()

    # ── APPLICATION TRACKER ──
    st.subheader("📋 Application Tracker")

    col1, col2 = st.columns([3, 1])
    with col2:
        st.markdown("**Add New Application**")
        with st.form("add_application"):
            new_title = st.text_input("Job Title")
            new_company = st.text_input("Company")
            new_date = st.date_input("Applied Date")
            new_status = st.selectbox("Status", ["Applied", "Screening", "Interview", "Offer", "Rejected"])
            new_notes = st.text_area("Notes", height=80)
            submitted = st.form_submit_button("Add")

            if submitted and new_title and new_company:
                conn = get_connection()
                cur = conn.cursor()
                cur.execute("""
                    INSERT INTO applications 
                        (job_title, company_name, applied_date, status, notes)
                    VALUES (%s, %s, %s, %s, %s)
                """, (new_title, new_company, new_date, new_status, new_notes))
                conn.commit()
                cur.close()
                conn.close()
                st.success("Added!")
                st.cache_data.clear()
                st.rerun()

    with col1:
        if len(apps_df) > 0:
            st.dataframe(
                apps_df[['job_title', 'company_name', 'applied_date', 'status', 'notes']],
                use_container_width=True,
                hide_index=True
            )

            # Status breakdown
            status_counts = apps_df['status'].value_counts().reset_index()
            status_counts.columns = ['Status', 'Count']
            fig4 = px.bar(status_counts, x='Status', y='Count',
                         color='Status',
                         color_discrete_map={
                             'Applied': '#3498DB',
                             'Screening': '#F39C12',
                             'Interview': '#9B59B6',
                             'Offer': '#2ECC71',
                             'Rejected': '#E74C3C'
                         })
            st.plotly_chart(fig4, use_container_width=True)
        else:
            st.info("No applications tracked yet — add your first one!")

except Exception as e:
    st.error(f"Database connection error: {e}")
    st.info("Make sure Docker is running and PostgreSQL is up.")