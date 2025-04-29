import logging
import os  # needed for local testing
from dotenv import load_dotenv
import streamlit as st

# utilities
import google_utils as goo_utils
import library_utils as lib_utils
from admin_config import *

# Qdrant and embeddings
from qdrant_client import QdrantClient
from langchain_qdrant import QdrantVectorStore
from langchain_openai import OpenAIEmbeddings
from langchain.text_splitter import RecursiveCharacterTextSplitter


load_dotenv(ENV_PATH)  # needed for local testing

# Config LangSmith observability
# LANGCHAIN_API_KEY = os.environ["LANGCHAIN_API_KEY"]
# os.environ["LANGCHAIN_TRACING_V2"] = "false"
# os.environ["LANGCHAIN_PROJECT"] = "ASK_main_upsert_notebook"

# st.secrets pulls from ~/.streamlit when run locally


# Config langchain_openai
OPENAI_API_KEY = st.secrets["OPENAI_API_KEY_ASK"] # for langchain_openai.OpenAIEmbeddings
os.environ["OPENAI_API_KEY"] = st.secrets["OPENAI_API_KEY_ASK"] # for openai client in cloud environment


# --- DRY RUN MODE ---
DRY_RUN = True  # True will NOT actually upload to Qdrant or move files. Only simulate.


# --- SETUP LOGGING ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


# --- Chunking function ---
def chunk(docs_pages):
    """
    Turns a list of full-page Document objects ("docs_pages")
    into a list of smaller Document objects ("docs_chunks").
    """
    text_splitter = RecursiveCharacterTextSplitter(
        chunk_size=CONFIG["chunk_size"],
        chunk_overlap=CONFIG["chunk_overlap"],
        length_function=CONFIG["length_function"],
        separators=CONFIG["separators"],
    )
    return text_splitter.split_documents(docs_pages)


# --- Main ingestion pipeline ---
def main():
    logging.info("Starting Qdrant ingestion from Google Drive and Google Sheets.")

    # 1. Connect to Google APIs
    drive_client, sheets_client = goo_utils.get_gcp_clients()

    # 2. Fetch PDFs list from Google Drive Backlog folder
    pdfs_df = goo_utils.fetch_pdfs(drive_client, PDF_BACKLOG_FOLDER_ID)
    if pdfs_df.empty:
        logging.warning("No PDFs found in Google Drive folder.")
        return

    # 3. Fetch library catalog
    catalog_df = goo_utils.fetch_sheet_as_dataframe(sheets_client, LIBRARY_CATALOG_ID)
    if catalog_df.empty:
        logging.error("Failed to fetch library catalog.")
        return

    # 4. Connect to Qdrant
    qdrant_client = QdrantClient(
        url=QDRANT_URL,  # for cloud
        api_key=QDRANT_API_KEY,  # for cloud
        prefer_grpc=True,
        # path=QDRANT_PATH,  # for local
    )

    # 5. Confirm connection to Qdrant
    try:
        # Detect if client is cloud or local
        qdrant_type = lib_utils.which_qdrant(qdrant_client)

        # List existing collections
        collections = lib_utils.list_collections(qdrant_client)

        logging.info(f"Successfully connected to {qdrant_type.upper()} Qdrant instance at {QDRANT_URL}")
        logging.info(f"Available collections: {collections}")
        logging.info(f"Selecting collection: {CONFIG['qdrant_collection_name']} on {qdrant_type.upper()}")

    except Exception as e:
        logging.error(f"Failed to connect to Qdrant: {e}")
        return

    # 6. Initialize a LangChain vectorstore object
    qdrant = QdrantVectorStore(client=qdrant_client,
        collection_name=CONFIG["qdrant_collection_name"],
        # embedding here is LC interface to the embedding model
        embedding=OpenAIEmbeddings(
            model=CONFIG["embedding_model"]
        ),
        validate_collection_config=True  # Skip validation
    )

    total_pdfs = len(pdfs_df)
    total_success = 0
    total_failures = 0

    for idx, row in pdfs_df.iterrows():
        pdf_name = row["Name"]
        file_id = row["ID"]
        logging.info(f"Processing PDF file: '{pdf_name}' with ID {file_id}")

        try:
            # Download PDF
            pdf_bytes = goo_utils.download_pdf_from_drive(drive_client, file_id)
            if pdf_bytes is None:
                logging.warning(f"Skipping {pdf_name}: unable to download.")
                total_failures += 1
                continue

            # Compute UUID
            pdf_id = lib_utils.compute_pdf_id(pdf_bytes)
            if not pdf_id:
                logging.warning(f"Skipping {pdf_name}: unable to compute PDF ID.")
                total_failures += 1
                continue

            # Check if already exists
            if lib_utils.is_pdf_id_in_qdrant(qdrant_client, CONFIG, pdf_id):
                logging.info(f"Skipping {pdf_name}: already exists in Qdrant.")
                continue

            # Get Metadata
            planned_metadata = lib_utils.get_planned_metadata_for_single_record(catalog_df, pdf_id)
            if planned_metadata is None:
                logging.warning(f"Skipping {pdf_name}: metadata missing.")
                total_failures += 1
                continue

            # --- Metadata QA Printout ---
            print("\nMetadata check for", pdf_name)
            print(f"{'Field':<25}{'Type':<15}{'Value'}")
            print("-" * 70)
            for key, value in planned_metadata.items():
                print(f"{key:<25}{type(value).__name__:<15}{str(value)[:50]}{'...' if len(str(value)) > 50 else ''}")
            print("\n")

            # Extract full page documents and attach metadata
            docs_pages = lib_utils.pdf_to_Docs_via_pypdf(pdf_bytes, planned_validated_metadata=planned_metadata)

            # --- Chunk full pages into smaller chunks
            docs_chunks = chunk(docs_pages)

            # Add to Qdrant
            if DRY_RUN:
                logging.info(f"[Dry Run] Would have added {len(docs_chunks)} chunks for {pdf_name} to Qdrant.")
            else:
                qdrant.add_documents(docs_chunks)
                logging.info(f"Successfully ingested {pdf_name}.")

            # Move PDF to live folder
            if DRY_RUN:
                logging.info(f"[Dry Run] Would have moved {pdf_name} to Live folder.")
            else:
                moved = goo_utils.move_file_between_folders(drive_client, file_id, PDF_LIVE_FOLDER_ID)
                if moved:
                    logging.info(f"Successfully moved {pdf_name} to live folder.")
                else:
                    logging.warning(f"Failed to move {pdf_name} to live folder.")

            total_success += 1

        except Exception as e:
            logging.error(f"Failed processing {pdf_name}: {e}")
            total_failures += 1

    # Summary
    logging.info(f"Finished processing. Success: {total_success}, Failures: {total_failures}, Total: {total_pdfs}")


if __name__ == "__main__":
    main()