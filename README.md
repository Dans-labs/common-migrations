# Common Dataverse Migrations
DANS migration pipelines from any input format to Dataverse data repository

## Authors
Developed by DANS R&D group in the context of external projects:
- Eko Indarto (DCCD project)
- Vic Ding (ODISSEI project)
- Slava Tykhonov (CLARIAH project)

## Installation 

Install poetry using brew: brew install poetry

Install dependencies: poetry install

Go inside of ./core directory and then run the migration
```
poetry run python main.py 
```

## Export
Exports the lock file to other formats:
poetry export -f requirements.txt --output requirements.txt
