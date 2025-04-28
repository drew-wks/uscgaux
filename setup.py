# Allows you to access some scripts in this repo as a module in other projects

from setuptools import setup

setup(
    name="uscgaux",
    version="0.1.0",
    py_modules=["google_utils"],       # we only have that one module
    install_requires=[
        "google-api-python-client",
        "oauth2client",
        "pandas",
        # …any other libs google_utils needs
    ],
    author="Drew Wilkins",
    url="https://github.com/drew-wks/uscgaux",
)
