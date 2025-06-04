
"""
Module: log_writer.py

Provides a reusable logging function for all agents to append entries to EVENT_LOG.
"""

import os, logging
from datetime import datetime, timezone
from dotenv import load_dotenv
from app_config import set_env_vars
from google_utils import get_cached_sheets_client, get_cached_drive_client


logging.basicConfig(level=logging.INFO)


set_env_vars()  # needed for local testing  


def log_event(action, pdf_id, filename, extra_columns=None):
    """
    Append a single row to the ADMIN_EVENT_LOG tab.

    Args:
        action (str): Action type (e.g., 'promoted_to_live')
        pdf_id (str): PDF identifier
        filename (str): Name of the file
        extra_columns: Optional list of extra values.
    """
    event = {
        "action": action,
        "pdf_id": pdf_id,
        "pdf_file_name": filename,
        "extra": extra_columns
    }

    log_events([event]) # type: ignore
    return event


def log_events(events):
    """
    Append multiple rows to the ADMIN_EVENT_LOG tab and return the timestamped events.

    Args:
        events: List of dictionaries in the form:
            {
                "action": str,
                "pdf_id": str,
                "pdf_file_name": str,
                "extra": optional list of additional values
            }

    Returns:
        List[dict]: Timestamped event dictionaries.
    """
    logged_events = []

    try:
        sheets_client = get_cached_sheets_client()
        ws = sheets_client.open_by_key(os.environ["EVENT_LOG"]).worksheet("Sheet1")

        rows = []
        for e in events:
            timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            event = {
                "timestamp": timestamp,
                "action": e["action"],
                "pdf_id": e["pdf_id"],
                "pdf_file_name": e["pdf_file_name"]
            }
            row_values = [timestamp, e["action"], e["pdf_id"], e["pdf_file_name"]]

            if e.get("extra"):
                event["extra"] = e["extra"]
                row_values.extend(e["extra"])

            rows.append(row_values)
            logged_events.append(event)

        ws.append_rows(rows, value_input_option="USER_ENTERED")

        log_url = f"https://docs.google.com/spreadsheets/d/{os.environ['EVENT_LOG']}"
        logging.info(f"📄 Admin event log written to: {log_url}")

    except Exception as e:
        logging.error(f"Failed to append log batch: {e}")

    return logged_events



def print_log_link():
    log_sheet_id = os.environ["EVENT_LOG"]
    log_url = f"https://docs.google.com/spreadsheets/d/{log_sheet_id}"
    logging.info(f"📄 Admin event log written to: {log_url}")

