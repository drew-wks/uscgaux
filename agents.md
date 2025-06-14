## Setup
Before running any code or tests, run the following:

pip install -r requirements.txt
pip install -r dev/requirements-dev.txt

If you are in the workspace you may need to address this as:
pip install -r /workspace/uscgaux/requirements-dev.txt


## CODEX Dev Environment Tips
Do NOT attempt to run any command which requires open network communication.  Your Dev environment has no network access and is sandboxed. No harm will come from trying but you will waste your effort.

Do not use `status_map.csv` if found. It may be outdated.


Do NOT Run `install.sh` this script, if it exists. This script will be executed during environement setup for you during your environment setup prior to you arriving.  If you make changes that require new dependencies or services (like postgres etc...) to be installed, you must edit this file to include the dependencies and/or service installation and startup.

The 'install.sh' references dependencies gathered here: `requirements.txt` and `requirements-dev.txt`. 

Note that the effects will not take place until the next task session. 

# Run pyright 
After editing a function
Before submitting a PR
Before running tests

# Run st.testing.v1.AppTest
After editing streamlit_app.py
After I ask you to test the Streamlit app

# Network access
Whenever requests are blocked due to network access restrictions, include the list of requests made your report