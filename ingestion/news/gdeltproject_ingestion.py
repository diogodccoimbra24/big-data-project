
import requests
import json
from pathlib import Path

#To solve the problem with the directory
PROJECT_ROOT = Path(__file__).resolve().parents[2]

url = "https://api.gdeltproject.org/api/v2/doc/doc"
params = {
    "query": "(bitcoin OR btc)",
    "mode": "timelinetone",
    "startdatetime": "20250101000000",
    "enddatetime": "20251231235959",
    "format": "json",
}

response = requests.get(url, params=params)

if response.status_code == 200:
    data = response.json()
    news = data

    # If response is good a json file is created in data/raw/news
    output_dir = PROJECT_ROOT / "data" / "raw" / "news"
    output_dir.mkdir(parents=True, exist_ok=True)

    created_file_path = output_dir / "news365.json"

    with open (created_file_path, "w") as f:
        json.dump(news, f, indent=2)

else:
    print("Error:", response.status_code, response.text)