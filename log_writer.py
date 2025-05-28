
"""
Module: log_writer.py

Provides a reusable logging function for all agents to append entries to ADMIN_EVENT_LOG.
"""

import os, logging
from datetime import datetime, timezone
from dotenv import load_dotenv
from admin_config import set_env_vars
from google_utils import get_gcp_clients


set_env_vars()  # needed for local testing  


def log_admin_event(action, pdf_id, filename, extra_columns=None):
    """
    Append a single row to the ADMIN_EVENT_LOG tab.

    Args:
        action (str): Action type (e.g., 'promoted_to_live')
        pdf_id (str): PDF identifier
        filename (str): Name of the file
        extra_columns: Optional list of extra values (e.g., timestamp).
    """
    
    drive_client, sheets_client = get_gcp_clients()
    
    event = {
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "action": action,
        "pdf_id": pdf_id,
        "pdf_file_name": filename
    }
    
    try:
        ws = sheets_client.open_by_key(os.environ["ADMIN_EVENT_LOG"]).worksheet("Sheet1")
        row_values = list(event.values())
        if extra_columns:
            row_values.extend(extra_columns)
        ws.append_row(row_values, value_input_option="USER_ENTERED")
        logging.info(f"Appended row to log {os.environ['ADMIN_EVENT_LOG']}")
    except Exception as e:
        logging.error(f"Failed to append row to log: {e}")

