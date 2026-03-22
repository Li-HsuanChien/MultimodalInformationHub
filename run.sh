
set -e  # exit on error

# 1. Check if venv exists
if [ ! -d "venv" ]; then
    echo "Error: virtual environment not found. Run setup first."
    exit 1
fi

echo "Activating virtual environment..."
source venv/bin/activate

echo "Running fetch_and_convert_files.py..."
python msDriveCommunication.py

echo "Running automation.py..."
python automation.py

echo "Done."