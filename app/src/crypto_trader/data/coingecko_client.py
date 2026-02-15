import os
from dotenv import load_dotenv
from coingecko_sdk import Coingecko

load_dotenv()  # loads .env if present

def make_client() -> Coingecko:
    return Coingecko(
        pro_api_key=os.environ.get("COINGECKO_PRO_API_KEY"),
        environment="pro",
        max_retries=3,
    )

client = make_client()
