# Migration core for Dataverse data repository
1. Install all dependencies from requirements.txt

2. Copy and edit config.py
```
cp ./config_sample.py ./config.py
```
3. Copy your token from Dataverse and put here in config.py:

DATAVERSE_API_TOKEN = 'your_token_here'

Run migration process:
```
python main.py
```

Check deposited metadata in your Dataverse!

