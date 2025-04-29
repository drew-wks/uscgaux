import streamlit as st
from datetime import datetime, timedelta

def show_metadata_form():
    """Display the PDF metadata input form and return metadata dictionary."""

    # === Document Type
    doc_type_options = {
        0: "ALAUX / ALCOAST",
        1: "Coast Guard Directive (e.g., CI, CIM, CCN, etc)",
        2: "L2 Message",
        9: "Other",
    }
    doc_type_selection = st.radio(
        "This is a:",
        options=list(doc_type_options.keys()),
        format_func=lambda x: doc_type_options[x],
        index=0
    )

    # === Scope
    scope = st.selectbox(
        "Scope",
        options=["national", "area", "district", "region", "division", "sector", "flotilla", "station", "other"],
        index=0
    )

    # === Other fields
    title = st.text_input("Title")
    organization = st.text_input("Organization")
    unit = st.text_input("Unit")

    # === Issue Date
    today_iso = datetime.now().strftime("%Y-%m-%dT00:00:00Z")
    issue_date = st.text_input("Issue Date", value=today_iso)

    # --- Calculate expiration date default
    try:
        parsed_issue = datetime.strptime(issue_date, "%Y-%m-%dT%H:%M:%SZ")
    except ValueError:
        parsed_issue = datetime.now()

    if doc_type_selection in (0, 2):
        expiration_date_default = (parsed_issue + timedelta(days=365)).strftime("%Y-%m-%dT00:00:00Z")
    elif doc_type_selection == 1:
        expiration_date_default = (parsed_issue + timedelta(days=3650)).strftime("%Y-%m-%dT00:00:00Z")
    elif doc_type_selection == 9:
        expiration_date_default = "2099-12-31T00:00:00Z"
    else:
        expiration_date_default = "2099-12-31T00:00:00Z"

    expiration_date = st.text_input("Expiration Date", value=expiration_date_default)

    # === Auxiliary Specific and Public Release
    aux_specific = st.radio(
        "Auxiliary Specific?",
        options=[True, False],
        index=0
    )

    public_release = st.radio(
        "Public Release?",
        options=[True, False],
        index=0
    )

    # --- Build metadata dictionary
    metadata = {
        "title": title,
        "organization": organization,
        "scope": scope,
        "unit": unit,
        "issue_date": issue_date,
        "expiration_date": expiration_date,
        "aux_specific": aux_specific,
        "public_release": public_release,
    }

    return metadata