import os
import psycopg2
import PyPDF2
from io import BytesIO
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

# ── DB CONNECTION ──────────────────────────────────────────
def get_db_connection():
    return psycopg2.connect(
        host="localhost",
        port=5432,
        database="job_pipeline",
        user="saurabh",
        password="password123"
    )

# ── EXTRACT TEXT FROM PDF ──────────────────────────────────
def extract_text_from_pdf(pdf_bytes):
    try:
        reader = PyPDF2.PdfReader(BytesIO(pdf_bytes))
        text = ""
        for page in reader.pages:
            text += page.extract_text() or ""
        return text.strip()
    except Exception as e:
        print(f"PDF extraction error: {e}")
        return ""

# ── EXTRACT SKILLS FROM TEXT ───────────────────────────────
def extract_skills(text):
    skill_keywords = [
        # Data Engineering
        "airflow", "dbt", "spark", "kafka", "hadoop", "hive",
        "snowflake", "bigquery", "redshift", "databricks",
        "postgresql", "mysql", "mongodb", "cassandra",
        "etl", "elt", "data pipeline", "data warehouse",
        "medallion architecture", "data modeling", "schema design",
        "docker", "kubernetes", "terraform", "git",
        # Programming
        "python", "sql", "java", "scala", "bash", "javascript",
        "pandas", "numpy", "pyspark",
        # Cloud
        "aws", "azure", "gcp", "google cloud",
        "s3", "lambda", "ec2", "rds",
        # BI & Analytics
        "power bi", "tableau", "looker", "metabase",
        "excel", "dax", "power query",
        # ML/AI
        "machine learning", "deep learning", "scikit-learn",
        "tensorflow", "pytorch", "langchain", "llm",
        "nlp", "computer vision", "mlflow",
        "openai", "hugging face", "vector database",
        # Concepts
        "data quality", "data governance", "data lineage",
        "ci/cd", "agile", "scrum", "rest api", "fastapi",
        "a/b testing", "statistics", "regression",
        "feature engineering", "data cleaning",
    ]
    text_lower = text.lower()
    return [s for s in skill_keywords if s in text_lower]

def infer_skills_from_title(job_title):
    """
    Infer expected skills from job title
    when no description is available
    """
    title_lower = job_title.lower()
    
    inferred = []
    
    if any(x in title_lower for x in ["data engineer", "etl", "pipeline"]):
        inferred.extend([
            "python", "sql", "etl", "airflow", "dbt",
            "postgresql", "spark", "data pipeline"
        ])
    if any(x in title_lower for x in ["analytics engineer", "analytics"]):
        inferred.extend([
            "sql", "python", "dbt", "data modeling",
            "power bi", "tableau", "analytics"
        ])
    if any(x in title_lower for x in ["data analyst", "business analyst"]):
        inferred.extend([
            "sql", "python", "excel", "power bi",
            "tableau", "statistics", "a/b testing"
        ])
    if any(x in title_lower for x in ["data scientist", "ml", "machine learning"]):
        inferred.extend([
            "python", "machine learning", "scikit-learn",
            "tensorflow", "pytorch", "statistics",
            "feature engineering", "regression"
        ])
    if any(x in title_lower for x in ["ai engineer", "llm", "genai"]):
        inferred.extend([
            "python", "langchain", "llm", "openai",
            "vector database", "fastapi", "rest api"
        ])
    if any(x in title_lower for x in ["software engineer", "backend", "developer"]):
        inferred.extend([
            "python", "sql", "git", "rest api",
            "docker", "aws", "ci/cd"
        ])
    if "snowflake" in title_lower:
        inferred.append("snowflake")
    if "databricks" in title_lower:
        inferred.append("databricks")
    if "spark" in title_lower:
        inferred.append("spark")
    if "kafka" in title_lower:
        inferred.append("kafka")
    if "aws" in title_lower:
        inferred.append("aws")
    if "azure" in title_lower:
        inferred.append("azure")
    if "gcp" in title_lower or "google cloud" in title_lower:
        inferred.append("gcp")

    return list(set(inferred))

# ── SAVE RESUME TO DB ──────────────────────────────────────
def save_resume(name, filename, raw_text, skills):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO resumes (name, filename, raw_text, extracted_skills)
        VALUES (%s, %s, %s, %s)
        RETURNING id
    """, (name, filename, raw_text, ", ".join(skills)))
    resume_id = cur.fetchone()[0]
    conn.commit()
    cur.close()
    conn.close()
    return resume_id

# ── COMPUTE MATCH SCORE ────────────────────────────────────
def compute_match_score(resume_text, job_title, job_text):
    if not resume_text.strip():
        return 0.0, [], []

    # If no description — infer skills from title
    title_skills = infer_skills_from_title(job_title)
    
    # Build job text from description + inferred title skills
    enriched_job_text = f"{job_title} {job_title} {job_text} {' '.join(title_skills)}"

    # TF-IDF similarity
    try:
        vectorizer = TfidfVectorizer(stop_words='english', max_features=500)
        tfidf_matrix = vectorizer.fit_transform(
            [resume_text, enriched_job_text]
        )
        similarity = cosine_similarity(
            tfidf_matrix[0:1], tfidf_matrix[1:2]
        )[0][0]
        tfidf_score = float(round(similarity * 10, 2))
    except:
        tfidf_score = 0.0

    # Keyword overlap using inferred + extracted skills
    resume_skills = set(extract_skills(resume_text))
    job_skills = set(extract_skills(enriched_job_text))
    
    # Add inferred skills to job skills
    job_skills.update(title_skills)

    if job_skills:
        matched = resume_skills.intersection(job_skills)
        missing = job_skills.difference(resume_skills)
        overlap_score = (len(matched) / len(job_skills)) * 10
    else:
        matched = set()
        missing = set()
        overlap_score = tfidf_score

    if job_skills:
        final_score = float(round(
            (tfidf_score * 0.4) + (overlap_score * 0.6), 1
        ))
    else:
        final_score = float(round(tfidf_score, 1))

    return float(min(final_score, 10.0)), list(matched), list(missing)

# ── SCORE ALL JOBS FOR A RESUME ────────────────────────────
def score_jobs_for_resume(resume_id, user_id=1):
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute(
        "SELECT raw_text FROM resumes WHERE id = %s", (resume_id,)
    )
    result = cur.fetchone()
    if not result:
        print(f"Resume {resume_id} not found")
        return 0

    resume_text = result[0]

    cur.execute("""
        SELECT r.id, r.job_title, r.job_description
        FROM raw_jobs r
        WHERE r.id NOT IN (
            SELECT job_id FROM job_matches WHERE resume_id = %s
        )
        LIMIT 1000
    """, (resume_id,))

    jobs = cur.fetchall()
    cur.close()
    conn.close()

    print(f"Scoring {len(jobs)} jobs for resume {resume_id}...")

    scored = 0
    for job_id, title, description in jobs:
        try:
            conn2 = get_db_connection()
            cur2 = conn2.cursor()

            # Double the title weight — fixes missing skills issue
            full_job_text = (
                f"{title or ''} {title or ''} {description or ''}"
            )

            score, matched, missing = compute_match_score(
                resume_text, title or "", full_job_text
            )

            interview_chance = float(min(round(score * 9.5, 1), 95.0))

            cur2.execute("""
                INSERT INTO job_matches
                    (job_id, resume_id, match_score,
                     matched_keywords, missing_keywords,
                     interview_chance)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
            """, (
                job_id,
                resume_id,
                score,
                ", ".join(matched[:10]),
                ", ".join(missing[:10]),
                interview_chance
            ))
            conn2.commit()
            cur2.close()
            conn2.close()
            scored += 1

            if scored % 100 == 0:
                print(f"Progress: {scored}...")

        except Exception as e:
            print(f"Error {job_id}: {e}")
            try:
                conn2.rollback()
                conn2.close()
            except:
                pass
            continue

    print(f"Scored {scored} jobs for resume {resume_id}")
    return scored

# ── GET ALL RESUMES ────────────────────────────────────────
def get_all_resumes():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT id, name, filename, extracted_skills, uploaded_at
        FROM resumes ORDER BY uploaded_at DESC
    """)
    columns = [desc[0] for desc in cur.description]
    results = [dict(zip(columns, row)) for row in cur.fetchall()]
    cur.close()
    conn.close()
    return results