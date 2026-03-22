#!/bin/bash

set -e  # exit on error

# 1. Check if venv exists
if [ ! -d "venv" ]; then
    echo "Error: virtual environment not found. Run setup first."
    exit 1
fi

# 2. Activate venv
echo "Activating virtual environment..."
source venv/bin/activate

# 3. Run script
echo "Running automation.py..."
python automation.py

echo "Done."