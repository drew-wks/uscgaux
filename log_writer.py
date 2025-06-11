
"""
Module: log_writer.py

Provides a reusable logging function for all agents to append entries to EVENT_LOG.
"""

import logging
from datetime import datetime, timezone
from typing import Optional, Dict, List, Any
from gspread.client import Client as SheetsClient
from env_config import env_config

config = env_config()

logging.basicConfig(level=logging.INFO)



def log_event(sheets_client: SheetsClient, action: str, pdf_id: str, filename: str, event_log_id: Optional[str] = None, extra_columns: Optional[List[Any]] = None):
    """
    Append a single row to the ADMIN_EVENT_LOG tab.

    Args:
        sheets_client: Authenticated Google Sheets client
        action (str): Action type (e.g., 'promoted_to_live')
        pdf_id (str): PDF identifier
        filename (str): Name of the file
        event_log_id (str): Optional ID of the spreadsheet. Falls back to env var.
        extra_columns: Optional list of extra values.
        
    Returns:
        dict: Logged event with timestamp
    """

    event = {
        "action": action,
        "pdf_id": pdf_id,
        "pdf_file_name": filename,
        "extra_columns": extra_columns
    }

    log_events(sheets_client, [event], event_log_id) # type: ignore
    return event


def log_events(
    sheets_client: SheetsClient,
    events: List[Dict[str, Any]],
    event_log_id: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Append multiple rows to the ADMIN_EVENT_LOG tab and return the timestamped events.

    Args:
        sheets_client: Authenticated Google Sheets client
        events: List of dictionaries in the form:
            {
                "action": str,
                "pdf_id": str,
                "pdf_file_name": str,
                "extra_columns": Optional[List[Any]]
            }
        event_log_id (str): Optional ID of the spreadsheet. Falls back to env var.

    Returns:
        List[dict]: Timestamped event dictionaries.
    """
    logged_events = []

    try:
        spreadsheet_id = event_log_id or config["EVENT_LOG"]
        ws = sheets_client.open_by_key(spreadsheet_id).worksheet("Sheet1")
        headers = ws.row_values(1)

        rows = []
        for e in events:
            timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            event = {
                "timestamp": timestamp,
                "action": e["action"],
                "pdf_id": e["pdf_id"],
                "pdf_file_name": e["pdf_file_name"]
            }

            row_data = {
                "timestamp": timestamp,
                "action": e["action"],
                "pdf_id": e["pdf_id"],
                "pdf_file_name": e["pdf_file_name"]
            }

            if e.get("extra_columns"):
                event["extra_columns"] = e["extra_columns"]
                for idx, extra_val in enumerate(e["extra_columns"]):
                    key = f"extra_{idx+1}"
                    row_data[key] = extra_val

            # Ensure row is aligned with the headers
            row_values = [row_data.get(col, "") for col in headers]
            rows.append(row_values)
            logged_events.append(event)

        ws.append_rows(rows, value_input_option="USER_ENTERED")  # type: ignore

        log_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}"
        logging.info(f"Admin event log written to: {log_url}")

    except Exception as e:
        logging.error(f"Failed to append log batch: {e}")

    return logged_events




def print_log_link():
    log_sheet_id = config["EVENT_LOG"]
    log_url = f"https://docs.google.com/spreadsheets/d/{log_sheet_id}"
    logging.info(f"📄 Admin event log written to: {log_url}")

