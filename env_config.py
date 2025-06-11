import os, logging, json
from dotenv import load_dotenv, dotenv_values


logging.basicConfig(
    level=logging.INFO,  # or DEBUG
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)


# --- Google Drive Folder IDs and Google Sheet File IDs ---
# https://drive.google.com/drive/folders/1CZcBJFFhuzrbzIArDOwc07xPELqDnikf

GCP_CONFIG = {
    "PDF_TAGGING": "1993TlUkd9_4XqWCutyY5oNTpmBdnxefc",  # a PDF Folder
    "PDF_LIVE": "1-vyQQp30mKzudkTOk7YJLmmVDirBOIpg",  # a PDF Folder
    "PDF_ARCHIVE": "1FYUFxenYC6nWomzgv6j1O4394Zv6Bs5F",  # a PDF Folder
    "LIBRARY_UNIFIED": "1glNjmKYloO0u6tCd5qy3z5ee8I-MkqwUxWsWEvLMBkM",  # a Google Sheet
    "LIBRARY_ARCHIVE": "1vgLY8qyqPOVMTvzX9VF8mWYdmONgIyx5m0L4uSJRmFs",  # a Google Sheet
    "EVENT_LOG": "1MYxGVdMqd3DkRYD0CtQuVFSmHn6TT_p1CDPJiMS2Nww",  # a Google Sheet
    "LIBRARY_CATALOG_ID": "16F5tRIvuHncofRuXCsQ20A7utZWRuEgA2bvj4nQQjek",  # a Google Sheet
}

"""
--- STREAMLIT RUNTIME SWITCHES ---

Purpose:
This module reads environment switches that help your app distinguish between running in Streamlit (e.g., UI mode) vs. local scripts (e.g., testing, CLI).

✅ Define these in your `.env` file for local development.
🚫 DO NOT include these in your Streamlit Community Cloud secrets — they are inferred there.

--- Switches ---

1. RUN_CONTEXT
    Set this to:
    - "streamlit" → uses st.secrets and decorators (DEFAULT)
    - "cli"       → uses .env and skips Streamlit features


2. FORCE_USER_AUTH
    - true → Requires login. Defaults to True when in streamlit
    - false → Bypasses login for local dev or test flows.
"""


def set_env_vars():
    """
    THIS IS THE LEGACY FUNCTION
    Loads environment variables and/or secrets into environmental variables for runtime.
    THis app does not rely on passing variables as secrets at runtime
    Always attempts to load the .env file first to capture overrides like RUN_CONTEXT.
    """
    
    # Get env file if it exists
    ENV_FILE = "/Users/drew_wilkins/Drews_Files/Drew/Python/Localcode/.env"
    if os.path.exists(ENV_FILE):
        load_dotenv(dotenv_path=ENV_FILE)
        logging.info(f"Loaded local .env file from {ENV_FILE}")
    else:
        logging.info(f"No local .env file found at {ENV_FILE}, assuming Streamlit Cloud")

    # Set context values by reading values from env or else setting defaults that assume Streamlit
    run_context = os.getenv("RUN_CONTEXT", "streamlit").lower()
    force_user_auth = os.getenv("FORCE_USER_AUTH", "true").lower() == "true"

    logging.info(f"Running in context: {run_context.upper()}")
    logging.info(f"Force user authentication: {force_user_auth}")


    # If context is set to Streamlit, stores Streamlit secrets into os.environ. Yes, it will do this on every reload, but it's very quick
    if run_context == "streamlit":
        # First remove eixsting keys from os.environ that came from .env, if present
        # We will re establish the context values again below
        env_keys = dotenv_values(ENV_FILE).keys()
        for key in env_keys:
            os.environ.pop(key, None)
        # Store Streamlit secrets into os.environ
        try:
            import streamlit as st
            for key, value in st.secrets.items():
                if isinstance(value, dict):
                    os.environ[key.upper() + "_JSON_STRING"] = json.dumps(value)
                    logging.warning("Dicts found and converted to JSON strings")
                else:
                    os.environ[key] = str(value)
        except Exception as e:
            logging.warning(f"Failed to load Streamlit secrets: {e}")

    # Store the balance of values into os.environ explicitly, regardless of streamlit context
    for key, value in GCP_CONFIG.items():
        os.environ[key] = value
    os.environ["RUN_CONTEXT"] = run_context
    os.environ["FORCE_USER_AUTH"] = str(force_user_auth).lower()
    os.environ["QDRANT_PATH"] = "/Users/drew_wilkins/Drews_Files/Drew/Python/Localcode/Drews_Tools/qdrant_ASK_lib_tools/qdrant_db"



def env_config():
    """
    THIS IS THE NEW FUNCTION
    Respects RUN_CONTEXT and FORCE_USER_AUTH from .env.
    Loads app config from .env and/or Streamlit secrets.
    Returns a normalized config dictionary.
    
    Usage:  Import and call it in your main code...
    from env_config import env_config
    config = env_config()
    
    access varialbes like this:
    library_id = config["LIBRARY_UNIFIED"]
    """

    config = {}

    # Load .env if present
    ENV_FILE = "/Users/drew_wilkins/Drews_Files/Drew/Python/Localcode/.env"
    if os.path.exists(ENV_FILE):
        load_dotenv(ENV_FILE)
        logging.info(f"Found a local .env file at {ENV_FILE}")

    # Read declared context (don't assume based on .env presence)
    run_context = os.getenv("RUN_CONTEXT", "streamlit").lower()
    force_user_auth = os.getenv("FORCE_USER_AUTH", "true").lower() == "true"

    # Start loading variables into config dict
    config["run_context"] = run_context
    config["force_user_auth"] = force_user_auth
    logging.info(f"Running in context: {run_context.upper()}")
    logging.info(f"Force user authentication: {force_user_auth}")

    config.update(GCP_CONFIG)
    config["QDRANT_PATH"] = "/Users/drew_wilkins/Drews_Files/Drew/Python/Localcode/Drews_Tools/qdrant_ASK_lib_tools/qdrant_db"

    # Optionally load Streamlit secrets if run_context is set to that
    if run_context == "streamlit":
        try:
            import streamlit as st
            for key, value in st.secrets.items():
                config[key.lower()] = value  # Lowercase for consistency
            logging.info(f"Set balance of env values from Streamlit secrets")
        except Exception as e:
            config["streamlit_error"] = str(e)
            logging.warning(f"Failed to load Streamlit secrets: {e}")
    # Otherwise CLI/test mode — bring in all extra .env values
    else:
        if os.path.exists(ENV_FILE):
            for key, value in dotenv_values(ENV_FILE).items():
                if key not in config:
                    config[key] = value
            logging.info(f"Set balance of env values from.env file at {ENV_FILE}")
   
    return config




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


def rag_config(key: str):
    """
    Retrieve a required configuration value from RAG_CONFIG.

    Args:
        key (str): The configuration key to retrieve.

    Returns:
        Any: The value associated with the key in RAG_CONFIG.

    Raises:
        ValueError: If the key is missing or the value is None.
        KeyError: If the key is not present in RAG_CONFIG.
    """
    if key not in RAG_CONFIG:
        raise KeyError(f"Missing required config key '{key}' in RAG_CONFIG")

    value = RAG_CONFIG[key]
    if value is None:
        raise ValueError(f"Config key '{key}' is set to None in RAG_CONFIG")

    return value
