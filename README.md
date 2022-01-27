# Foobar


## Requirements
- docker 
- python 3


## Usage
it would be better if you can create a venv for the project.

Navigate to the project folder

```bash
docker-compose up -d
```

```bash
pip -r install requirements.txt
```
```bash
python3 populate_database.py
```
```bash
python3 flask_app.py
```
this would pull then run the 3 needed containers(mongodb, spark master, spark worker), 
create a database and collection on mongodb and populate it with the Excel data, 
then run jupyter and flask servers.

open a browser and navigate to ```172.0.0.1:5000```

click each question to get the corresponding answer.

The questions without hyperlink are still WIP.