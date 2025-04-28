import streamlit as st


# --- Google Drive Folder IDs ---
# https://drive.google.com/drive/folders/1CZcBJFFhuzrbzIArDOwc07xPELqDnikf
PDF_BACKLOG_FOLDER_ID = "1993TlUkd9_4XqWCutyY5oNTpmBdnxefc"
PDF_LIVE_FOLDER_ID = "1-vyQQp30mKzudkTOk7YJLmmVDirBOIpg"
PDF_DELETED_FOLDER_ID = "1FYUFxenYC6nWomzgv6j1O4394Zv6Bs5F"

# --- Google Sheets ID ---
LIBRARY_CATALOG_ID = "16F5tRIvuHncofRuXCsQ20A7utZWRuEgA2bvj4nQQjek"

# --- Environment Variable Path ---
ENV_PATH = "/Users/drew_wilkins/Drews_Files/Drew/Python/Localcode/.env"

# --- Config Qdrant ---
QDRANT_URL = st.secrets["QDRANT_URL"]
QDRANT_API_KEY = st.secrets["QDRANT_API_KEY"]
# QDRANT_URL = st.secrets["QDRANT_URL"]
# QDRANT_API_KEY = st.secrets["QDRANT_API_KEY"]
QDRANT_PATH = "/Users/drew_wilkins/Drews_Files/Drew/Python/Localcode/Drews_Tools/qdrant_ASK_lib_tools/qdrant_db"

# --- LangChain + RAG specific CONFIG dict ---
CONFIG = {
    "splitter_type": "CharacterTextSplitter",
    "chunk_size": 2000,
    "chunk_overlap": 200,
    "length_function": len,
    "separators": ["}"],
    "qdrant_collection_name": "ASK_vectorstore",
    "embedding_model": "text-embedding-ada-002",
    "embedding_dims": 1536,
    "vector_name": "text-dense",
    "sparse_vector_name": "None",
    "sparse_embedding": "None",
    "search_type": "mmr",
    "k": 5,
    'fetch_k': 20,
    'lambda_mult': .7,
    "score_threshold": 0.5,
    "generation_model": "gpt-3.5-turbo-16k",
    "temperature": 0.7,
}