# USCG Aux Project

This repository contains utilities for USCG Auxiliary management.

## Setup

1. **Create a virtual environment** (recommended):

   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   ```

2. **Install dependencies**:

   ```bash
   pip install -r requirements.txt
   ```

### Automated Setup

You can run `setup.sh` to automatically create the virtual environment and install packages:

```bash
./setup.sh
```

This script will create `.venv` in the repository root and install all packages from `requirements.txt`.
