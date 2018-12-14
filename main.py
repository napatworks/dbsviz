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
    # Parameters
    chrome_driver_dir = "tools/chromedriver"
    download_dir = ""
    firebase_credential_path = "credential/dbsweb-secret.json"
    bucket = "dbsweb-f2346.appspot.com"

    # Get metadata
    s_set_website.run(chrome_driver_dir=chrome_driver_dir, download_dir=download_dir,
                      firebase_credential_path=firebase_credential_path, bucket=bucket)
    p_set_website.run(firebase_credential_path=firebase_credential_path, bucket=bucket)

    # Get Yahoo price data
    s_yahoo.run(firebase_credential_path=firebase_credential_path, bucket=bucket)
    p_yahoo.run(firebase_credential_path=firebase_credential_path, bucket=bucket)
    return "Finish daily task"

if __name__ == "__main__":
	app.run(host='0.0.0.0')