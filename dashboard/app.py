import streamlit as st
import psycopg2
import pandas as pd
import plotly.express as px
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'scripts'))

st.set_page_config(
    page_title="Job Intelligence",
    page_icon="🎯",
    layout="wide"
)

# ── CUSTOM CSS ─────────────────────────────────────────────
st.markdown("""
<style>
    /* Hide default sidebar */
    [data-testid="stSidebar"] { display: none; }
    
    /* Top nav bar */
    .topnav {
        position: sticky;
        top: 0;
        z-index: 999;
        background: #0f1117;
        padding: 12px 24px;
        display: flex;
        align-items: center;
        gap: 32px;
        border-bottom: 1px solid #2d2d2d;
        margin-bottom: 32px;
    }
    .topnav .brand {
        font-size: 18px;
        font-weight: 700;
        color: #fff;
        margin-right: 16px;
    }
    .topnav a {
        color: #aaa;
        text-decoration: none;
        font-size: 14px;
        font-weight: 500;
        padding: 6px 12px;
        border-radius: 6px;
        transition: all 0.2s;
    }
    .topnav a:hover {
        color: #fff;
        background: #1e1e2e;
    }
    
    /* Section anchors offset for sticky nav */
    .section-anchor {
        padding-top: 80px;
        margin-top: -80px;
    }
    
    /* Section dividers */
    .section-divider {
        margin: 48px 0 32px 0;
        border: none;
        border-top: 1px solid #2d2d2d;
    }
    
    /* Main content padding */
    .main .block-container {
        padding-top: 0 !important;
        max-width: 1200px;
    }
</style>

<div class="topnav">
    <span class="brand">🎯 Job Intelligence</span>
    <a href="#dashboard">📊 Dashboard</a>
    <a href="#resume-match">📄 Resume Match</a>
    <a href="#applications">📋 Applications</a>
    <a href="#market-trends">📈 Market Trends</a>
</div>
""", unsafe_allow_html=True)

# ── DB CONNECTION ──────────────────────────────────────────
def get_connection():
    host = os.getenv("DB_HOST", "localhost")
    return psycopg2.connect(
        host=host,
        port=5432,
        database=os.getenv("DB_NAME", "job_pipeline"),
        user=os.getenv("DB_USER", "saurabh"),
        password=os.getenv("DB_PASSWORD", "password123"),
        sslmode="require" if host.endswith(".azure.com") else "disable"
    )

# ── DATA LOADERS ───────────────────────────────────────────
@st.cache_data(ttl=300)
def load_jobs():
    conn = get_connection()
    df = pd.read_sql("""
        SELECT job_title, company_name, location, job_url,
               source_platform, scraped_date, ats_score,
               experience_level, yoe_range, job_category,
               relevance_rank
        FROM analytics.mart_jobs
        ORDER BY relevance_rank
    """, conn)
    conn.close()
    return df

@st.cache_data(ttl=300)
def load_applications():
    conn = get_connection()
    df = pd.read_sql(
        "SELECT * FROM applications ORDER BY applied_date DESC", conn
    )
    conn.close()
    return df

def load_resumes():
    conn = get_connection()
    df = pd.read_sql("""
        SELECT id, name, filename, extracted_skills, uploaded_at
        FROM resumes ORDER BY uploaded_at DESC
    """, conn)
    conn.close()
    return df

def load_matches(resume_id, min_score=0.0):
    conn = get_connection()
    df = pd.read_sql("""
        SELECT r.id as job_id, r.job_title, r.company_name,
               r.location, r.job_url, r.source_platform,
               r.date_posted, jm.match_score,
               jm.matched_keywords, jm.missing_keywords
        FROM job_matches jm
        JOIN raw_jobs r ON jm.job_id = r.id
        WHERE jm.resume_id = %s AND jm.match_score >= %s
        ORDER BY jm.match_score DESC
        LIMIT 200
    """, conn, params=(resume_id, min_score))
    conn.close()
    return df

def get_total_jobs():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM raw_jobs")
    count = cur.fetchone()[0]
    cur.close()
    conn.close()
    return count

# ══════════════════════════════════════════════════════════
# SECTION 1 — DASHBOARD
# ══════════════════════════════════════════════════════════
st.markdown('<div class="section-anchor" id="dashboard"></div>', unsafe_allow_html=True)
st.title("📊 Job Intelligence Dashboard")
st.markdown("*Real-time job market intelligence — Airflow + PostgreSQL + dbt*")

try:
    jobs_df = load_jobs()
    apps_df = load_applications()

    st.subheader("🔍 Search & Filter")
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        keyword_search = st.text_input("🔎 Search Job Title", placeholder="e.g. data engineer...")
    with col2:
        location_search = st.text_input("📍 Location", placeholder="e.g. Bangalore...")
    with col3:
        yoe_options = ["All", "0-1", "1-2", "2-3", "3-4+"]
        selected_yoe = st.selectbox("📅 Experience (YOE)", yoe_options)
    with col4:
        categories = ["All"] + sorted(jobs_df['job_category'].dropna().unique().tolist())
        selected_category = st.selectbox("💼 Category", categories)

    filtered = jobs_df.copy()
    if keyword_search:
        filtered = filtered[filtered['job_title'].str.contains(keyword_search, case=False, na=False)]
    if location_search:
        filtered = filtered[filtered['location'].str.contains(location_search, case=False, na=False)]
    if selected_yoe != "All":
        filtered = filtered[filtered['yoe_range'] == selected_yoe]
    if selected_category != "All":
        filtered = filtered[filtered['job_category'] == selected_category]

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Jobs Found", len(filtered))
    c2.metric("DE Roles", len(filtered[filtered['job_category'] == 'Data Engineering']))
    c3.metric("Applications Sent", len(apps_df))
    c4.metric("Avg ATS Score", round(filtered['ats_score'].mean(), 1) if len(filtered) > 0 else 0)

    left, right = st.columns([3, 1])
    with left:
        st.subheader("🏆 Top Ranked Jobs")
        for _, row in filtered.head(20).iterrows():
            with st.expander(f"#{int(row['relevance_rank'])} — **{row['job_title']}** @ {row['company_name']} | {row['location']}"):
                c1, c2, c3 = st.columns(3)
                c1.write(f"**Category:** {row['job_category']}")
                c2.write(f"**Level:** {row['experience_level']}")
                c3.write(f"**YOE:** {row['yoe_range']} yrs")
                if row['job_url']:
                    st.markdown(f"[🔗 Apply Here]({row['job_url']})")

    with right:
        st.subheader("📊 Jobs by Category")
        cat_counts = filtered['job_category'].value_counts().reset_index()
        cat_counts.columns = ['Category', 'Count']
        fig = px.pie(cat_counts, values='Count', names='Category', hole=0.4)
        fig.update_layout(margin=dict(t=0, b=0, l=0, r=0))
        st.plotly_chart(fig, use_container_width=True)

    c1, c2 = st.columns(2)
    with c1:
        st.subheader("🏢 Top Companies Hiring")
        co = filtered['company_name'].value_counts().head(10).reset_index()
        co.columns = ['Company', 'Openings']
        fig2 = px.bar(co, x='Openings', y='Company', orientation='h', color='Openings', color_continuous_scale='Blues')
        fig2.update_layout(margin=dict(t=0, b=0))
        st.plotly_chart(fig2, use_container_width=True)

    with c2:
        st.subheader("📅 Jobs by YOE Range")
        yoe_counts = filtered['yoe_range'].value_counts().reset_index()
        yoe_counts.columns = ['YOE', 'Count']
        yoe_order = ['0-1', '1-2', '2-3', '3-4+']
        yoe_counts['YOE'] = pd.Categorical(yoe_counts['YOE'], categories=yoe_order, ordered=True)
        yoe_counts = yoe_counts.sort_values('YOE')
        fig3 = px.bar(yoe_counts, x='YOE', y='Count', color='YOE',
            color_discrete_map={'0-1': '#2ECC71', '1-2': '#3498DB', '2-3': '#F39C12', '3-4+': '#E74C3C'})
        fig3.update_layout(margin=dict(t=0, b=0), showlegend=False)
        st.plotly_chart(fig3, use_container_width=True)

except Exception as e:
    st.error(f"Dashboard error: {e}")
    import traceback; st.code(traceback.format_exc())

# ══════════════════════════════════════════════════════════
# SECTION 2 — RESUME MATCH
# ══════════════════════════════════════════════════════════
st.markdown('<hr class="section-divider">', unsafe_allow_html=True)
st.markdown('<div class="section-anchor" id="resume-match"></div>', unsafe_allow_html=True)
st.title("📄 Resume Match Engine")
st.markdown("*Upload your resume — get matched against live job listings from Naukri, LinkedIn, Indeed and Internshala*")

try:
    from resume_matcher import extract_text_from_pdf, extract_skills, save_resume, score_jobs_for_resume

    col1, col2 = st.columns([1, 1])
    with col1:
        st.subheader("Upload Resume")
        resume_name = st.text_input("Resume Label", placeholder="e.g. Saurabh DE Resume")
        uploaded_file = st.file_uploader("Upload PDF", type=["pdf"])

        if uploaded_file and resume_name:
            if st.button("🚀 Upload & Match", type="primary"):
                with st.spinner("Extracting text from resume..."):
                    pdf_bytes = uploaded_file.read()
                    raw_text = extract_text_from_pdf(pdf_bytes)

                if not raw_text:
                    st.error("Could not extract text. Try a different PDF.")
                else:
                    skills = extract_skills(raw_text)
                    st.success(f"Found {len(skills)} skills!")
                    st.write("**Skills found:**", ", ".join(skills))
                    resume_id = save_resume(resume_name, uploaded_file.name, raw_text, skills)

                    try:
                        total = get_total_jobs()
                        st.info(f"🔍 Matching against {total} jobs in database...")
                    except Exception:
                        st.info("🔍 Matching against job database...")

                    with st.spinner("🎯 Scoring all jobs against your resume... (~2 mins)"):
                        scored = score_jobs_for_resume(resume_id)
                        st.success(f"Scored {scored} jobs!")
                        st.cache_data.clear()
                        st.rerun()

    with col2:
        st.subheader("Your Resumes")
        try:
            resumes_df = load_resumes()
            if len(resumes_df) > 0:
                for _, row in resumes_df.iterrows():
                    with st.expander(f"📄 {row['name']} — {str(row['uploaded_at'])[:10]}"):
                        st.write("**Skills:**", row['extracted_skills'])
            else:
                st.info("No resumes uploaded yet.")
        except Exception:
            st.info("Upload your first resume!")

    st.divider()
    st.subheader("🎯 Your Job Matches")

    resumes_df = load_resumes()
    if len(resumes_df) == 0:
        st.info("Upload a resume to see matches!")
    else:
        resume_options = {f"{r['name']} ({str(r['uploaded_at'])[:10]})": r['id'] for _, r in resumes_df.iterrows()}
        selected_resume = st.selectbox("Select Resume", list(resume_options.keys()))
        resume_id = resume_options[selected_resume]

        col1, col2, col3 = st.columns(3)
        with col1:
            min_score = st.slider("Min Match Score", 0.0, 10.0, 4.0, 0.5)
        with col2:
            platform_filter = st.selectbox("Platform", ["All", "JSearch", "Internshala", "Naukri", "Instahyre"])
        with col3:
            location_filter = st.text_input("Filter Location", placeholder="e.g. Bangalore...", key="rm_loc")

        matches_df = load_matches(resume_id, min_score)
        if platform_filter != "All":
            matches_df = matches_df[matches_df['source_platform'] == platform_filter]
        if location_filter:
            matches_df = matches_df[matches_df['location'].str.contains(location_filter, case=False, na=False)]

        if len(matches_df) == 0:
            st.info("No matches found. Try lowering the score filter.")
        else:
            st.write(f"**{len(matches_df)} jobs** match your criteria")
            c1, c2, c3 = st.columns(3)
            c1.metric("🔥 Excellent (8+)", len(matches_df[matches_df['match_score'] >= 8]))
            c2.metric("👍 Good (6-8)", len(matches_df[(matches_df['match_score'] >= 6) & (matches_df['match_score'] < 8)]))
            c3.metric("📋 Fair (4-6)", len(matches_df[(matches_df['match_score'] >= 4) & (matches_df['match_score'] < 6)]))
            st.divider()

            for idx, job in matches_df.iterrows():
                score = float(job['match_score'])
                color = "🟢" if score >= 7 else "🟡" if score >= 4 else "🔴"
                fit = "🔥 Excellent Fit" if score >= 7 else "👍 Good Fit" if score >= 5 else "📋 Fair Fit"
                with st.expander(f"{color} **{job['job_title']}** @ {job['company_name']} — Match: {score}/10 | {fit}"):
                    c1, c2 = st.columns(2)
                    with c1:
                        st.write(f"📍 **Location:** {job['location']}")
                        st.write(f"🌐 **Platform:** {job['source_platform']}")
                        if job.get('date_posted'):
                            st.write(f"📅 **Posted:** {str(job['date_posted'])[:10]}")
                        if job['job_url']:
                            st.markdown(f"[🔗 Apply Here]({job['job_url']})")
                        q = f"{job['job_title']} interview questions".replace(' ', '+')
                        st.markdown(f"[🎯 Interview Questions](https://www.google.com/search?q={q})")
                        company_slug = job['company_name'].lower().replace(' ', '-').replace(',', '').replace('.', '').replace("'", '')
                        st.markdown(f"[⭐ Glassdoor Reviews](https://www.glassdoor.co.in/Reviews/{company_slug}-reviews.htm)")
                        li = job['company_name'].replace(' ', '%20')
                        st.markdown(f"[💼 LinkedIn](https://www.linkedin.com/company/{li})")
                    with c2:
                        st.write(f"✅ **Matched:** {job['matched_keywords'] or 'N/A'}")
                        st.write(f"❌ **Missing:** {job['missing_keywords'] or 'None — great fit!'}")
                        st.progress(score / 10)
                        fit_color = "#2ECC71" if score >= 7 else "#F39C12" if score >= 5 else "#E74C3C"
                        st.markdown(f"**Fit Level:** <span style='color:{fit_color}'>{fit}</span>", unsafe_allow_html=True)
                    if st.button("📝 Track Application", key=f"track_{idx}"):
                        conn = get_connection()
                        cur = conn.cursor()
                        cur.execute("INSERT INTO applications (job_title, company_name, applied_date, status) VALUES (%s, %s, CURRENT_DATE, 'Applied')",
                            (job['job_title'], job['company_name']))
                        conn.commit(); cur.close(); conn.close()
                        st.success("Added to tracker!")
                        st.cache_data.clear()

except Exception as e:
    st.error(f"Resume Match error: {e}")
    import traceback; st.code(traceback.format_exc())

# ══════════════════════════════════════════════════════════
# SECTION 3 — APPLICATIONS
# ══════════════════════════════════════════════════════════
st.markdown('<hr class="section-divider">', unsafe_allow_html=True)
st.markdown('<div class="section-anchor" id="applications"></div>', unsafe_allow_html=True)
st.title("📋 Application Tracker")

try:
    apps_df = load_applications()
    col1, col2 = st.columns([3, 1])

    with col2:
        st.subheader("Add Application")
        with st.form("add_app"):
            new_title = st.text_input("Job Title")
            new_company = st.text_input("Company")
            new_date = st.date_input("Applied Date")
            new_status = st.selectbox("Status", ["Applied", "Screening", "Interview", "Offer", "Rejected"])
            new_notes = st.text_area("Notes", height=80)
            submitted = st.form_submit_button("Add")
            if submitted and new_title and new_company:
                conn = get_connection()
                cur = conn.cursor()
                cur.execute("INSERT INTO applications (job_title, company_name, applied_date, status, notes) VALUES (%s, %s, %s, %s, %s)",
                    (new_title, new_company, new_date, new_status, new_notes))
                conn.commit(); cur.close(); conn.close()
                st.success("Added!")
                st.cache_data.clear()
                st.rerun()

    with col1:
        if len(apps_df) > 0:
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Total Applied", len(apps_df))
            c2.metric("Interviews", len(apps_df[apps_df['status'] == 'Interview']))
            c3.metric("Offers", len(apps_df[apps_df['status'] == 'Offer']))
            responded = len(apps_df[apps_df['status'] != 'Applied'])
            c4.metric("Response Rate", f"{round(responded / len(apps_df) * 100)}%")

            status_filter = st.multiselect("Filter by Status", apps_df['status'].unique().tolist(), default=apps_df['status'].unique().tolist())
            filtered_apps = apps_df[apps_df['status'].isin(status_filter)]
            st.dataframe(filtered_apps[['job_title', 'company_name', 'applied_date', 'status', 'notes']], use_container_width=True, hide_index=True)

            status_counts = apps_df['status'].value_counts().reset_index()
            status_counts.columns = ['Status', 'Count']
            fig = px.bar(status_counts, x='Status', y='Count', color='Status',
                color_discrete_map={'Applied': '#3498DB', 'Screening': '#F39C12', 'Interview': '#9B59B6', 'Offer': '#2ECC71', 'Rejected': '#E74C3C'})
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No applications yet!")

except Exception as e:
    st.error(f"Applications error: {e}")

# ══════════════════════════════════════════════════════════
# SECTION 4 — MARKET TRENDS
# ══════════════════════════════════════════════════════════
st.markdown('<hr class="section-divider">', unsafe_allow_html=True)
st.markdown('<div class="section-anchor" id="market-trends"></div>', unsafe_allow_html=True)
st.title("📈 India Job Market Trends")
st.markdown("*Based on live job data from Naukri, LinkedIn, Indeed & Internshala*")

try:
    skill_keywords = [
        'python', 'sql', 'airflow', 'dbt', 'spark', 'kafka',
        'snowflake', 'bigquery', 'databricks', 'aws', 'azure',
        'gcp', 'docker', 'kubernetes', 'postgresql', 'mysql',
        'mongodb', 'tableau', 'power bi', 'pandas', 'pyspark',
        'git', 'etl'
    ]

    conn = get_connection()
    total_df = pd.read_sql("SELECT COUNT(*) as c FROM raw_jobs", conn)
    total = int(total_df['c'].iloc[0])
    conn.close()

    skill_counts = []
    for skill in skill_keywords:
        conn = get_connection()
        result = pd.read_sql(f"""
            SELECT COUNT(*) as count FROM raw_jobs
            WHERE LOWER(job_description) LIKE '%{skill}%'
            OR LOWER(job_title) LIKE '%{skill}%'
        """, conn)
        conn.close()
        skill_counts.append({'Skill': skill.title(), 'Jobs': int(result['count'].iloc[0])})

    skill_df = pd.DataFrame(skill_counts)
    skill_df = skill_df[skill_df['Jobs'] > 0].sort_values('Jobs', ascending=False)
    skill_df['Percentage'] = (skill_df['Jobs'] / total * 100).round(1)

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Jobs Tracked", total)
    c2.metric("Top Skill", skill_df.iloc[0]['Skill'])
    c3.metric("Top Skill Demand", f"{skill_df.iloc[0]['Percentage']}%")
    c4.metric("Skills Tracked", len(skill_df))

    st.divider()
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("🔥 Most In-Demand Skills")
        fig = px.bar(skill_df.head(15), x='Percentage', y='Skill', orientation='h',
            color='Percentage', color_continuous_scale='Viridis', text='Percentage')
        fig.update_traces(texttemplate='%{text}%', textposition='outside')
        fig.update_layout(margin=dict(t=0, b=0), showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("🏢 Top Hiring Companies")
        conn = get_connection()
        company_df = pd.read_sql("""
            SELECT company_name, COUNT(*) as openings FROM raw_jobs
            WHERE company_name IS NOT NULL AND company_name != ''
            GROUP BY company_name ORDER BY openings DESC LIMIT 15
        """, conn)
        conn.close()
        fig2 = px.bar(company_df, x='openings', y='company_name', orientation='h',
            color='openings', color_continuous_scale='Blues')
        fig2.update_layout(margin=dict(t=0, b=0))
        st.plotly_chart(fig2, use_container_width=True)

    st.divider()
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("📍 Jobs by City")
        conn = get_connection()
        city_df = pd.read_sql("""
            SELECT
                CASE
                    WHEN LOWER(location) LIKE '%bangalore%' OR LOWER(location) LIKE '%bengaluru%' THEN 'Bangalore'
                    WHEN LOWER(location) LIKE '%hyderabad%' THEN 'Hyderabad'
                    WHEN LOWER(location) LIKE '%mumbai%' THEN 'Mumbai'
                    WHEN LOWER(location) LIKE '%pune%' THEN 'Pune'
                    WHEN LOWER(location) LIKE '%delhi%' OR LOWER(location) LIKE '%noida%' OR LOWER(location) LIKE '%gurgaon%' THEN 'Delhi NCR'
                    WHEN LOWER(location) LIKE '%remote%' THEN 'Remote'
                    ELSE 'Other'
                END as city,
                COUNT(*) as jobs
            FROM raw_jobs GROUP BY city ORDER BY jobs DESC
        """, conn)
        conn.close()
        fig3 = px.pie(city_df, values='jobs', names='city', hole=0.4,
            color_discrete_sequence=px.colors.qualitative.Set3)
        st.plotly_chart(fig3, use_container_width=True)

    with col2:
        st.subheader("🌐 Jobs by Platform")
        conn = get_connection()
        platform_df = pd.read_sql("""
            SELECT source_platform, COUNT(*) as jobs FROM raw_jobs
            GROUP BY source_platform ORDER BY jobs DESC
        """, conn)
        conn.close()
        fig4 = px.bar(platform_df, x='source_platform', y='jobs',
            color='source_platform', color_discrete_sequence=px.colors.qualitative.Pastel)
        fig4.update_layout(showlegend=False)
        st.plotly_chart(fig4, use_container_width=True)

    st.divider()
    st.subheader("💡 Key Insights")
    top3 = skill_df.head(3)['Skill'].tolist()
    bangalore_jobs = city_df[city_df['city'] == 'Bangalore']['jobs'].sum()
    bangalore_pct = round(bangalore_jobs / total * 100)
    st.info(f"🔥 **Top 3 skills in demand:** {', '.join(top3)} — master these to maximize your chances.")
    st.info(f"📍 **Bangalore dominates** with {bangalore_pct}% of all tracked openings.")
    st.info(f"🎯 **{skill_df.iloc[0]['Skill']}** appears in {skill_df.iloc[0]['Percentage']}% of job descriptions — it's the #1 skill to have.")

except Exception as e:
    st.error(f"Market Trends error: {e}")
    import traceback; st.code(traceback.format_exc())