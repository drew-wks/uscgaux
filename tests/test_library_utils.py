import io
import pandas as pd
from pypdf import PdfWriter

import utils.library_utils as library_utils


def test_compute_pdf_id():
    buf = io.BytesIO()
    writer = PdfWriter()
    writer.add_blank_page(width=100, height=100)
    writer.write(buf)
    buf.seek(0)
    pdf_id = library_utils.compute_pdf_id(buf)
    assert isinstance(pdf_id, str)


def test_validate_core_metadata_format():
    df = pd.DataFrame({
        'pdf_id': ['1'],
        'pdf_file_name': ['a'],
        'gcp_file_id': ['g'],
        'link': ['l']
    })
    missing = library_utils.validate_core_metadata_format(df)
    assert missing == set()


def test_fetch_rows_by_status():
    df = pd.DataFrame({'status': ['new', 'archived', 'delete']})
    result = library_utils.fetch_rows_by_status(df, ['delete'])
    assert len(result) == 1


def test_change_status_in_df():
    df = pd.DataFrame({'status': ['old', 'old', 'keep']})
    updated = library_utils.change_status_in_df(df, 'old', 'new')
    assert list(updated['status']) == ['new', 'new', 'keep']
