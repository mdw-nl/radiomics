import logging
import time

import requests
from global_var import XNAT_PASSWORD, XNAT_URL, XNAT_USERNAME
from requests.auth import HTTPBasicAuth

"""This class is made to send dicom data to a XNAT server. Important thing to note XNAT filters data on Patient ID and Patient's name,
which means that if the data received has the same patient name and the same patient id then it sorts it into the same data package."""

logger = logging.getLogger(__name__)


class SendDICOM:
    def __init__(self):
        self.xnat_url = XNAT_URL
        username = XNAT_USERNAME
        password = XNAT_PASSWORD
        self.auth = HTTPBasicAuth(username, password)

    def checking_connectivity(self):
        """Ckecks the connection to xnat"""
        logger.info("Checking connectivity")
        connectivity = requests.get(self.xnat_url, auth=self.auth)
        logger.info("%s", connectivity.status_code)
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
            logger.info("%s", check_url)

            while not self.is_session_ready(check_url):
                logger.info("DICOM session not archived yet; waiting...")
                time.sleep(5)

            upload_url = f"{check_url}/resources/json/files/{filename}"

            response = requests.put(
                upload_url, data=csv_content.encode("utf-8"), auth=self.auth, headers={"Content-Type": "text/csv"}
            )

            if response.status_code in [200, 201]:
                logger.info("Uploaded %s successfully to XNAT.", filename)
            else:
                logger.error("Failed to upload %s. Status %s: %s", filename, response.status_code, response.text)

        except Exception:
            logger.exception("An error occurred while uploading to XNAT.")
