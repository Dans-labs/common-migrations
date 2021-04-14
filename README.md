# Common Dataverse Migrations
DANS migration pipelines from any input format to Dataverse data repository

# Developed by DANS R&D group in the context of external projects:
- Eko Indarto (DCCD project)
- Vic Ding (ODISSEI project)
- Slava Tykhonov (CLARIAH project)

# Installation 

Install poetry using brew: brew install poetry

Install dependencies: poetry install

Run main.py: Goto dccd directory and then
poetry run python main.py 


Exports the lock file to other formats:
poetry export -f requirements.txt --output requirements.txt
