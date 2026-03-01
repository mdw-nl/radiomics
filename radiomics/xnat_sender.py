import os
import pydicom
from pydicom import dcmread
import requests
from requests.auth import HTTPBasicAuth
import logging
import time
from consumer import Consumer
from config_handler import Config
import json
import zipfile

from global_var import XNAT_URL, XNAT_PASSWORD, XNAT_USERNAME

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger()

"""This class is made to send dicom data to a XNAT server. Important thing to note XNAT filters data on Patient ID and Patient's name, 
which means that if the data received has the same patient name and the same patient id then it sorts it into the same data package."""

class SendDICOM:
    def __init__(self):
        self.xnat_url = XNAT_URL
        username = XNAT_USERNAME
        password = XNAT_PASSWORD
        self.auth = HTTPBasicAuth(username, password)
    
    def checking_connectivity(self):
        """Ckecks the connection to xnat"""
        logging.info("Checking connectivity")
        connectivity = requests.get(self.xnat_url, auth=self.auth)
        logging.info(connectivity.status_code)
        return connectivity.status_code
    
    def is_session_ready(self, url):
        """Checks if the project url is ready"""
        response = requests.get(url, auth=self.auth)
        return response.status_code == 200

    
    def upload_to_xnat(self, csv_content, metadata, filename):
        """Upload CSV directly to XNAT"""
        try:
            project, subject, experiment = metadata["project"], metadata["subject"], metadata["experiment"]
            check_url = f"{self.xnat_url}/data/projects/{project}/subjects/{subject}/experiments/{experiment}"
            logging.info(check_url)
            
            while not self.is_session_ready(check_url):
                logging.info("DICOM session not archived yet; waiting...")
                time.sleep(5)

            upload_url = f"{check_url}/resources/json/files/{filename}"

            response = requests.put(
                upload_url,
                data=csv_content.encode('utf-8'), 
                auth=self.auth,
                headers={'Content-Type': 'text/csv'}
            )

            if response.status_code in [200, 201]:
                logging.info(f"Uploaded {filename} successfully.")
            else:
                logging.error(f"Failed to upload {filename}. Status {response.status_code}: {response.text}")

        except Exception as e:
            logging.error("An error occurred while uploading to XNAT.", exc_info=True)

        