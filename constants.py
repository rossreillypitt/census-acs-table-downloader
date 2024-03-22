import os
import dotenv
import json

dotenv.load_dotenv()

API_KEY = os.environ.get("API_KEY")
VARIABLES = json.load(open("variables.json"))

