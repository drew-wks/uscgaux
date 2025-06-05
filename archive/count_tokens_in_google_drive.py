# python3 count_tokens_in_google_drive.py
#
# Requirements (add to requirements.txt if you don’t already have them):
#   pdfminer.six     — robust text extraction from PDFs
#   tiktoken         — OpenAI‑compatible tokenizer
#
# This script assumes the following helpers already exist, exactly as in
# your other admin utilities:
#   goo_utils.init_auth()
#   goo_utils.get_gcp_clients()         → returns (drive_client, sheets_client)
#   goo_utils.list_pdfs_in_drive_folder(drive_client, folder_id)  # → DataFrame with columns ID, Name
#   goo_utils.download_file_bytes(drive_client, file_id)          # → bytes
#
# If you don’t yet have download_file_bytes, it’s only ~15 lines using
# googleapiclient.http.MediaIoBaseDownload – see comment in code.

import io
import logging
from typing import Tuple

import pandas as pd
import pdfminer.high_level
import tiktoken
from dotenv import load_dotenv

from env_config import *
import gcp_utils as goo_utils

# ── Logging & env ────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")
load_dotenv(ENV_PATH)

# ── Helpers ─────────────────────────────────────────────────────────────────
ENCODING = tiktoken.get_encoding("cl100k_base")  # matches GPT‑3.5/4 family

def extract_text_from_pdf(pdf_bytes: bytes) -> str:
    """Return all text found in a PDF (best‑effort)."""
    with io.BytesIO(pdf_bytes) as fh:
        return pdfminer.high_level.extract_text(fh) or ""

def count_tokens(text: str) -> int:
    """Fast token count using the chosen encoding."""
    return len(ENCODING.encode(text))

def tokens_for_drive_folder(drive_client, folder_id: str) -> Tuple[int, pd.DataFrame]:
    """Download every PDF in a Drive folder and return total + per‑file DataFrame."""
    pdfs_df = goo_utils.list_pdfs_in_drive_folder(drive_client, folder_id)
    total = 0
    per_file_rows = []

    for _, row in pdfs_df.iterrows():
        file_id, name = row["ID"], row["Name"]
        try:
            fh = goo_utils.fetch_pdf_from_drive(drive_client, file_id)
            if fh is None:
                raise RuntimeError("Download failed")
            text = pdfminer.high_level.extract_text(fh)
            n_tokens = count_tokens(text)
            total += n_tokens
            per_file_rows.append({"file_name": name, "file_id": file_id, "tokens": n_tokens})
            logging.info(f"{name}: {n_tokens:,} tokens")
        except Exception as e:
            logging.error(f"❌ {name}: {e}")

    return total, pd.DataFrame(per_file_rows)


def main():
    logging.info("🔎  Counting tokens in Drive PDFs …")

    goo_utils.init_auth()
    drive_client, _ = goo_utils.get_gcp_clients()

    backlog_total, backlog_df = tokens_for_drive_folder(drive_client, PDF_TAGGING_FOLDER)
    live_total,    live_df    = tokens_for_drive_folder(drive_client, PDF_LIVE_FOLDER_ID)

    grand_total = backlog_total + live_total

    logging.info(f"📂 Backlog folder: {backlog_total:,} tokens")
    logging.info(f"📂 Live    folder: {live_total:,} tokens")
    logging.info(f"🏁 Grand total:    {grand_total:,} tokens")

    (pd.concat([backlog_df.assign(folder="Backlog"),
                live_df.assign(folder="Live")], ignore_index=True)
        .to_csv("pdf_token_counts.csv", index=False))
    logging.info("📝  Detailed breakdown written to pdf_token_counts.csv")

if __name__ == "__main__":
    main()
