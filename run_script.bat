@echo off
REM Check if Python is installed
python --version
IF %ERRORLEVEL% NEQ 0 (
    echo Python is not installed. Please install Python and try again.
    exit /b 1
)

REM Create a virtual environment
python -m venv env

REM Activate the virtual environment
call env\Scripts\activate

REM Install the required dependencies
pip install --upgrade pip
pip install -r requirements.txt

REM Run the Python script
python AMBR.py

REM Deactivate the virtual environment
call env\Scripts\deactivate

echo Script execution completed.
pause
