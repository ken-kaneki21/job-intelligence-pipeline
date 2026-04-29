#!/bin/bash
pip install streamlit psycopg2-binary pandas plotly scikit-learn PyPDF2 requests beautifulsoup4 python-dotenv numpy
python -m streamlit run dashboard/app.py --server.port 8000 --server.address 0.0.0.0 --server.headless true
