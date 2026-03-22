#!/bin/bash

set -e  # exit on error

echo "=== Starting project setup ==="

# 1. Create virtual environment if it doesn't exist
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
else
    echo "Virtual environment already exists."
fi

# 2. Activate virtual environment
echo "Activating virtual environment..."
source venv/bin/activate

# 3. Upgrade pip (optional but recommended)
pip install --upgrade pip

# 4. Install Python dependencies
if [ -f "requirements.txt" ]; then
    echo "Installing dependencies..."
    pip install -r requirements.txt
else
    echo "requirements.txt not found, skipping..."
fi

# 5. Ensure sqlite3 is installed
if ! command -v sqlite3 &> /dev/null; then
    echo "sqlite3 not found. Installing..."

    if [ -x "$(command -v apt-get)" ]; then
        sudo apt-get update
        sudo apt-get install -y sqlite3
    elif [ -x "$(command -v yum)" ]; then
        sudo yum install -y sqlite
    elif [ -x "$(command -v brew)" ]; then
        brew install sqlite
    else
        echo "Package manager not supported. Please install sqlite3 manually."
        exit 1
    fi
else
    echo "sqlite3 already installed."
fi

# 6. Create db directory if not exists
mkdir -p db

# 7. Initialize database with schema
DB_PATH="db/annotation.db"

if [ ! -f "$DB_PATH" ]; then
    echo "Creating database and applying schema..."
    sqlite3 "$DB_PATH" < schema.sql
else
    echo "Database already exists. Skipping schema load."
fi

# 8. Run Python scripts
echo "Running data insertion scripts..."
python insertUser.py
python insertVideo.py

echo "=== Setup complete ==="