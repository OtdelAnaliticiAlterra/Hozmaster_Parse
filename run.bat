chcp 65001

call "%~dp0.venv\Scripts\activate"

pip install -r "%~dp0requirements.txt"

py "%~dp0Hozmaster_Parse.py"