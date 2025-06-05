# Allows you to access some scripts in this repo as a module in other projects
# Add this to the requestor's requirements.txt: git+https://github.com/drew-wks/uscgaux.git@main#egg=uscgaux

from setuptools import setup


setup(
    name="uscgaux",
    version="0.1.0",
    py_modules=["gcp_utils"],
    install_requires=[
        # dependencies for gcp_utils.py
        "pandas",
        "gspread>=5.8.3",
        "google-api-python-client",
        "google-auth",
        "streamlit_authenticator"
        # add dependencies for other modules here
    ],
    author="Drew Wilkins",
    url="https://github.com/drew-wks/uscgaux",
)
