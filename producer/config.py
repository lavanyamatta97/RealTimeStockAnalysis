import logging # replacement for print  
import os # to be able to access API keys and secrets in .env file 

from dotenv import load_dotenv # same as os to be able to access .env variables

load_dotenv() #call the function

#configure logging module
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

#complete the config
logger = logging.getLogger(__name__)

BASEURL = "alpha-vantage.p.rapidapi.com"
url = f"https://{BASEURL}/query" # we use f to make it readable
api_key = os.getenv("API_KEY")


headers = {
	"x-rapidapi-key": api_key,
	"x-rapidapi-host": BASEURL,
	#"Content-Type": "application/json"
}