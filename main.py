import sys
sys.path.insert(0, 'lib')

# import scraping script
import src.scraping.set_website as s_set_website
import src.scraping.yahoo as s_yahoo

# import processing script
import src.process.set_website as p_set_website
import src.process.yahoo as p_yahoo

# import other utils
from flask import Flask, request

app = Flask(__name__)

@app.route("/")
def index():
    return "This web app is created for scraping"

@app.route("/daily")
def run():
    print("Start daily scraping task")

    # Parameters
    chrome_driver_dir = "tools/chromedriver"
    download_dir = ""
    firebase_credential_path = "credential/dbsweb-secret.json"
    bucket = "dbsweb-f2346.appspot.com"

    # Get metadata
    print("Getting SET data")
    s_set_website.run(chrome_driver_dir=chrome_driver_dir, download_dir=download_dir,
                      firebase_credential_path=firebase_credential_path, bucket=bucket)
    print("Finishing scrape SET data")
    print("Processing SET data")
    p_set_website.run(firebase_credential_path=firebase_credential_path, bucket=bucket)
    print("Finishing process SET data")

    # Get Yahoo price data
    print("Getting YAHOO data")
    s_yahoo.run(firebase_credential_path=firebase_credential_path, bucket=bucket)
    print("Finishing scrape YAHOO data")
    print("Processing YAHOO data")
    p_yahoo.run(firebase_credential_path=firebase_credential_path, bucket=bucket)
    print("Finishing process YAHOO data")

    # Factsheet data
    print("Getting SET data")
    s_set_website.run_factsheet(chrome_driver_dir=chrome_driver_dir, download_dir=download_dir,
                      firebase_credential_path=firebase_credential_path, bucket=bucket)
    print("Finishing scrape SET data")
    print("Processing SET data")
    p_set_website.run_factsheet(firebase_credential_path=firebase_credential_path, bucket=bucket)
    print("Finishing process SET data")
    
    return "Finish daily task"

if __name__ == "__main__":
	app.run(host='0.0.0.0', port=8080)