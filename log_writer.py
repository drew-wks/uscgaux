
"""
Module: log_writer.py

Provides a reusable logging function for all agents to append entries to EVENT_LOG.
"""

import os, logging
from datetime import datetime, timezone
from dotenv import load_dotenv
from app_config import set_env_vars
set_env_vars() 

logging.basicConfig(level=logging.INFO)



def log_event(action, pdf_id, filename, sheets_client, event_log_id=None, extra_columns=None):
    """
    Append a single row to the ADMIN_EVENT_LOG tab.

    Args:
        action (str): Action type (e.g., 'promoted_to_live')
        pdf_id (str): PDF identifier
        filename (str): Name of the file
        sheets_client: Authenticated Google Sheets client
        event_log_id (str): Optional ID of the spreadsheet. Falls back to env var.
        extra_columns: Optional list of extra values.
        
    Returns:
        dict: Logged event with timestamp
    """

    event = {
        "action": action,
        "pdf_id": pdf_id,
        "pdf_file_name": filename,
        "extra": extra_columns
    }

    log_events([event], sheets_client, event_log_id) # type: ignore
    return event


def log_events(events, sheets_client, event_log_id=None):
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
        sheets_client: Authenticated Google Sheets client
        event_log_id (str): Optional ID of the spreadsheet. Falls back to env var.

    Returns:
        List[dict]: Timestamped event dictionaries.
    """
    logged_events = []

    try:
        spreadsheet_id = event_log_id or os.environ["EVENT_LOG"]
        ws = sheets_client.open_by_key(spreadsheet_id).worksheet("Sheet1")

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
    log_sheet_id = os.environ.get("EVENT_LOG", "missing_env_var")
    log_url = f"https://docs.google.com/spreadsheets/d/{log_sheet_id}"
    logging.info(f"📄 Admin event log written to: {log_url}")

