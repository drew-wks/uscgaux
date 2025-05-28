import os
from dotenv import load_dotenv


# --- Google Drive Folder IDs ---
# https://drive.google.com/drive/folders/1CZcBJFFhuzrbzIArDOwc07xPELqDnikf

GOOGLE_CONFIG = {
    "PDF_TAGGING": "1993TlUkd9_4XqWCutyY5oNTpmBdnxefc",  # a PDF Folder
    "PDF_LIVE": "1-vyQQp30mKzudkTOk7YJLmmVDirBOIpg",  # a PDF Folder
    "PDF_DELETED": "1FYUFxenYC6nWomzgv6j1O4394Zv6Bs5F",  # a PDF Folder
    "LIBRARY_UNIFIED": "1glNjmKYloO0u6tCd5qy3z5ee8I-MkqwUxWsWEvLMBkM",  # a Google Sheet
    "LIBRARY_ARCHIVE": "1vgLY8qyqPOVMTvzX9VF8mWYdmONgIyx5m0L4uSJRmFs",  # a Google Sheet
    "ADMIN_EVENT_LOG": "1MYxGVdMqd3DkRYD0CtQuVFSmHn6TT_p1CDPJiMS2Nww",  # a Google Sheet
    "LIBRARY_CATALOG_ID": "16F5tRIvuHncofRuXCsQ20A7utZWRuEgA2bvj4nQQjek",  # a Google Sheet
}


# --- Config Qdrant here because don't import streamlit in library_utils---
def set_env_vars():
    for key, value in GOOGLE_CONFIG.items():
        os.environ[key] = value
    os.environ["QDRANT_PATH"] = "/Users/drew_wilkins/Drews_Files/Drew/Python/Localcode/Drews_Tools/qdrant_ASK_lib_tools/qdrant_db"
    load_dotenv("/Users/drew_wilkins/Drews_Files/Drew/Python/Localcode/.env")  # needed for local testing
    try:
        import streamlit as st
        os.environ["QDRANT_URL"] = st.secrets["QDRANT_URL"]
        os.environ["QDRANT_API_KEY"] = st.secrets["QDRANT_API_KEY"]
    except ModuleNotFoundError:
        print("Streamlit not available — skipping st.secrets.")
    except Exception as e:
        print(f"Could not load Streamlit secrets: {e}")




RAG_CONFIG = {
    "splitter_type": "CharacterTextSplitter",
    "chunk_size": 2000,
    "chunk_overlap": 200,
    "length_function": len,
    "separators": ["}"],
    "qdrant_location": "cloud",
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