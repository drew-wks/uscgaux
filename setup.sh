#!/bin/bash
# Codex-compatible setup script

echo "Installing main dependencies..."
pip install -r requirements.txt

echo "Installing dev/test dependencies..."
pip install -r requirements-dev.txt
