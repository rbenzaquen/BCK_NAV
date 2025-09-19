# Desde la carpeta donde están api.py y script.py
#source /home/benzarod/blck/gspread-env/bin/activate
#nohup uvicorn api:app --host 0.0.0.0 --port 8000 --reload > uvicorn.log 2>&1 &
#echo $! > uvicorn.pid
systemctl status blck-api.service
