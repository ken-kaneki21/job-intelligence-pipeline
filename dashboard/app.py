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

# ── DB CONNECTION ──────────────────────────────────────────
def get_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=5432,
        database=os.getenv("DB_NAME", "job_pipeline"),
        user=os.getenv("DB_USER", "saurabh"),
        password=os.getenv("DB_PASSWORD", "password123"),
        sslmode="require" if os.getenv("DB_HOST", "").endswith(".azure.com") else "disable"
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
        "SELECT * FROM applications ORDER BY applied_date DESC",
        conn
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
        SELECT 
            r.id as job_id,
            r.job_title, r.company_name, r.location,
            r.job_url, r.source_platform, r.date_posted,
            jm.match_score, jm.interview_chance,
            jm.matched_keywords, jm.missing_keywords
        FROM job_matches jm
        JOIN raw_jobs r ON jm.job_id = r.id
        WHERE jm.resume_id = %s
        AND jm.match_score >= %s
        ORDER BY jm.match_score DESC
        LIMIT 200
    """, conn, params=(resume_id, min_score))
    conn.close()
    return df

# ── SIDEBAR ────────────────────────────────────────────────
st.sidebar.title("🎯 Job Intelligence")
st.sidebar.markdown("*Powered by Airflow + dbt + PostgreSQL*")
st.sidebar.divider()

page = st.sidebar.radio(
    "Navigate",
    ["📊 Dashboard", "📄 Resume Match", "📋 Applications"]
)

# ══════════════════════════════════════════════════════════
# PAGE 1 — DASHBOARD
# ══════════════════════════════════════════════════════════
if page == "📊 Dashboard":
    st.title("🎯 Job Intelligence Dashboard")
    st.markdown(
        "*Real-time job market intelligence — "
        "Airflow + PostgreSQL + dbt*"
    )
    

    try:
        jobs_df = load_jobs()
        apps_df = load_applications()

        # Filters
        st.subheader("🔍 Search & Filter")
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            keyword_search = st.text_input(
                "🔎 Search Job Title",
                placeholder="e.g. data engineer..."
            )
        with col2:
            location_search = st.text_input(
                "📍 Location",
                placeholder="e.g. Bangalore..."
            )
        with col3:
            yoe_options = ["All", "0-1", "1-2", "2-3", "3-4+"]
            selected_yoe = st.selectbox("📅 Experience (YOE)", yoe_options)
        with col4:
            categories = ["All"] + sorted(
                jobs_df['job_category'].dropna().unique().tolist()
            )
            selected_category = st.selectbox("💼 Category", categories)

        # Apply filters
        filtered = jobs_df.copy()
        if keyword_search:
            filtered = filtered[
                filtered['job_title'].str.contains(
                    keyword_search, case=False, na=False
                )
            ]
        if location_search:
            filtered = filtered[
                filtered['location'].str.contains(
                    location_search, case=False, na=False
                )
            ]
        if selected_yoe != "All":
            filtered = filtered[filtered['yoe_range'] == selected_yoe]
        if selected_category != "All":
            filtered = filtered[filtered['job_category'] == selected_category]

        

        # Metrics
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Jobs Found", len(filtered))
        c2.metric("DE Roles", len(
            filtered[filtered['job_category'] == 'Data Engineering']
        ))
        c3.metric("Applications Sent", len(apps_df))
        c4.metric("Avg ATS Score",
            round(filtered['ats_score'].mean(), 1)
            if len(filtered) > 0 else 0
        )

        
        left, right = st.columns([3, 1])

        with left:
            st.subheader("🏆 Top Ranked Jobs")
            for _, row in filtered.head(20).iterrows():
                with st.expander(
                    f"#{int(row['relevance_rank'])} — "
                    f"**{row['job_title']}** @ "
                    f"{row['company_name']} | {row['location']}"
                ):
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
            fig = px.pie(
                cat_counts, values='Count', names='Category', hole=0.4
            )
            fig.update_layout(margin=dict(t=0, b=0, l=0, r=0))
            st.plotly_chart(fig, use_container_width=True)

        

        c1, c2 = st.columns(2)
        with c1:
            st.subheader("🏢 Top Companies Hiring")
            co = filtered['company_name'].value_counts().head(10).reset_index()
            co.columns = ['Company', 'Openings']
            fig2 = px.bar(
                co, x='Openings', y='Company', orientation='h',
                color='Openings', color_continuous_scale='Blues'
            )
            fig2.update_layout(margin=dict(t=0, b=0))
            st.plotly_chart(fig2, use_container_width=True)

        with c2:
            st.subheader("📅 Jobs by YOE Range")
            yoe_counts = filtered['yoe_range'].value_counts().reset_index()
            yoe_counts.columns = ['YOE', 'Count']
            yoe_order = ['0-1', '1-2', '2-3', '3-4+']
            yoe_counts['YOE'] = pd.Categorical(
                yoe_counts['YOE'], categories=yoe_order, ordered=True
            )
            yoe_counts = yoe_counts.sort_values('YOE')
            fig3 = px.bar(
                yoe_counts, x='YOE', y='Count', color='YOE',
                color_discrete_map={
                    '0-1': '#2ECC71', '1-2': '#3498DB',
                    '2-3': '#F39C12', '3-4+': '#E74C3C'
                }
            )
            fig3.update_layout(margin=dict(t=0, b=0), showlegend=False)
            st.plotly_chart(fig3, use_container_width=True)

    except Exception as e:
        st.error(f"Error: {e}")
        import traceback
        st.code(traceback.format_exc())

# ══════════════════════════════════════════════════════════
# PAGE 2 — RESUME MATCH
# ══════════════════════════════════════════════════════════
elif page == "📄 Resume Match":
    st.title("📄 Resume Match Engine")
    st.markdown(
        "*Upload your resume — we scrape jobs tailored to "
        "YOUR profile and score every one*"
    )
    

    from resume_matcher import (
        extract_text_from_pdf, extract_skills,
        save_resume, score_jobs_for_resume
    )

    col1, col2 = st.columns([1, 1])

    with col1:
        st.subheader("Upload Resume")
        resume_name = st.text_input(
            "Resume Label",
            placeholder="e.g. Saurabh DE Resume"
        )
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

                    resume_id = save_resume(
                        resume_name, uploaded_file.name,
                        raw_text, skills
                    )

                    with st.spinner(
                        "🔍 Scraping jobs tailored to your resume..."
                    ):
                        from scraper import scrape_for_resume
                        scraped = scrape_for_resume(
                            resume_id, user_id=resume_id
                        )
                        st.success(f"Found {scraped} new jobs for your profile!")

                    with st.spinner("🎯 Scoring all jobs..."):
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
                    with st.expander(
                        f"📄 {row['name']} — "
                        f"{str(row['uploaded_at'])[:10]}"
                    ):
                        st.write("**Skills:**", row['extracted_skills'])
            else:
                st.info("No resumes uploaded yet.")
        except:
            st.info("Upload your first resume!")

    

    # ── JOB MATCHES ──
    st.subheader("🎯 Your Job Matches")

    try:
        resumes_df = load_resumes()
        if len(resumes_df) == 0:
            st.info("Upload a resume to see matches!")
        else:
            # Resume selector
            resume_options = {
                f"{r['name']} ({str(r['uploaded_at'])[:10]})": r['id']
                for _, r in resumes_df.iterrows()
            }
            selected_resume = st.selectbox(
                "Select Resume", list(resume_options.keys())
            )
            resume_id = resume_options[selected_resume]

            # Filters
            col1, col2, col3 = st.columns(3)
            with col1:
                min_score = st.slider(
                    "Min Match Score", 0.0, 10.0, 4.0, 0.5
                )
            with col2:
                platform_filter = st.selectbox(
                    "Platform",
                    ["All", "JSearch", "Internshala", "Naukri"]
                )
            with col3:
                location_filter = st.text_input(
                    "Filter Location",
                    placeholder="e.g. Bangalore..."
                )

            matches_df = load_matches(resume_id, min_score)

            # Apply filters
            if platform_filter != "All":
                matches_df = matches_df[
                    matches_df['source_platform'] == platform_filter
                ]
            if location_filter:
                matches_df = matches_df[
                    matches_df['location'].str.contains(
                        location_filter, case=False, na=False
                    )
                ]

            if len(matches_df) == 0:
                st.info("No matches found. Try lowering the score filter.")
            else:
                st.write(
                    f"**{len(matches_df)} jobs** match your criteria"
                )

                # Score distribution
                c1, c2, c3 = st.columns(3)
                c1.metric("Excellent (8+)",
                    len(matches_df[matches_df['match_score'] >= 8])
                )
                c2.metric("Good (6-8)",
                    len(matches_df[
                        (matches_df['match_score'] >= 6) &
                        (matches_df['match_score'] < 8)
                    ])
                )
                c3.metric("Fair (4-6)",
                    len(matches_df[
                        (matches_df['match_score'] >= 4) &
                        (matches_df['match_score'] < 6)
                    ])
                )

                

                for idx, job in matches_df.iterrows():
                    score = float(job['match_score'])
                    chance = float(job.get('interview_chance') or 0)
                    color = (
                        "🟢" if score >= 7
                        else "🟡" if score >= 4
                        else "🔴"
                    )

                    with st.expander(
                        f"{color} **{job['job_title']}** @ "
                        f"{job['company_name']} — "
                        f"Match: {score}/10 | "
                        f"Interview Chance: {chance}%"
                    ):
                        c1, c2 = st.columns(2)

                        with c1:
                            st.write(f"📍 **Location:** {job['location']}")
                            st.write(f"🌐 **Platform:** {job['source_platform']}")
                            if job.get('date_posted'):
                                st.write(
                                    f"📅 **Posted:** "
                                    f"{str(job['date_posted'])[:10]}"
                                )
                            if job['job_url']:
                                st.markdown(
                                    f"[🔗 Apply Here]({job['job_url']})"
                                )

                            # Interview questions
                            q = (
                                f"{job['company_name']} "
                                f"{job['job_title']} "
                                f"interview questions"
                            ).replace(' ', '+')
                            st.markdown(
                                f"[🎯 Interview Questions]"
                                f"(https://www.google.com/search?q={q})"
                            )

                            # Glassdoor
                            g = (
                                f"{job['company_name']} reviews"
                            ).replace(' ', '+')
                            st.markdown(
                                f"[⭐ Company Reviews]"
                                f"(https://www.google.com/search?q={g}"
                                f"+site:glassdoor.com)"
                            )

                        with c2:
                            st.write(
                                f"✅ **Matched:** "
                                f"{job['matched_keywords'] or 'N/A'}"
                            )
                            st.write(
                                f"❌ **Missing:** "
                                f"{job['missing_keywords'] or 'None'}"
                            )
                            st.progress(score / 10)
                            st.write(f"🎲 **Interview Chance:** {chance}%")

                        # Action buttons
                        b1, b2 = st.columns(2)
                        with b1:
                            if st.button(
                                "📝 Track Application",
                                key=f"track_{idx}"
                            ):
                                conn = get_connection()
                                cur = conn.cursor()
                                cur.execute("""
                                    INSERT INTO applications
                                        (job_title, company_name,
                                         applied_date, status)
                                    VALUES (%s, %s, CURRENT_DATE, 'Applied')
                                """, (job['job_title'], job['company_name']))
                                conn.commit()
                                cur.close()
                                conn.close()
                                st.success("Added to tracker!")
                                st.cache_data.clear()

    except Exception as e:
        st.error(f"Error: {e}")
        import traceback
        st.code(traceback.format_exc())

# ══════════════════════════════════════════════════════════
# PAGE 3 — APPLICATIONS
# ══════════════════════════════════════════════════════════
elif page == "📋 Applications":
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
                new_status = st.selectbox(
                    "Status",
                    ["Applied", "Screening", "Interview",
                     "Offer", "Rejected"]
                )
                new_notes = st.text_area("Notes", height=80)
                submitted = st.form_submit_button("Add")

                if submitted and new_title and new_company:
                    conn = get_connection()
                    cur = conn.cursor()
                    cur.execute("""
                        INSERT INTO applications
                            (job_title, company_name,
                             applied_date, status, notes)
                        VALUES (%s, %s, %s, %s, %s)
                    """, (
                        new_title, new_company,
                        new_date, new_status, new_notes
                    ))
                    conn.commit()
                    cur.close()
                    conn.close()
                    st.success("Added!")
                    st.cache_data.clear()
                    st.rerun()

        with col1:
            if len(apps_df) > 0:
                c1, c2, c3, c4 = st.columns(4)
                c1.metric("Total Applied", len(apps_df))
                c2.metric("Interviews",
                    len(apps_df[apps_df['status'] == 'Interview'])
                )
                c3.metric("Offers",
                    len(apps_df[apps_df['status'] == 'Offer'])
                )
                responded = len(apps_df[apps_df['status'] != 'Applied'])
                c4.metric("Response Rate",
                    f"{round(responded / len(apps_df) * 100)}%"
                )

                

                status_filter = st.multiselect(
                    "Filter by Status",
                    apps_df['status'].unique().tolist(),
                    default=apps_df['status'].unique().tolist()
                )

                filtered_apps = apps_df[
                    apps_df['status'].isin(status_filter)
                ]

                st.dataframe(
                    filtered_apps[[
                        'job_title', 'company_name',
                        'applied_date', 'status', 'notes'
                    ]],
                    use_container_width=True,
                    hide_index=True
                )

                status_counts = apps_df['status'].value_counts().reset_index()
                status_counts.columns = ['Status', 'Count']
                fig = px.bar(
                    status_counts, x='Status', y='Count',
                    color='Status',
                    color_discrete_map={
                        'Applied': '#3498DB',
                        'Screening': '#F39C12',
                        'Interview': '#9B59B6',
                        'Offer': '#2ECC71',
                        'Rejected': '#E74C3C'
                    }
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No applications yet!")

    except Exception as e:
        st.error(f"Error: {e}")