import os

NUMBER_ATTEMPTS = 5
RETRY_DELAY_IN_SECONDS = 10
QUERY_UID = "SELECT file_path FROM public.dicom_insert WHERE study_instance_uid = %s LIMIT 1;"
DELETE_END = os.getenv("DELETE_END", "false").lower() in ("true", "1", "yes")
UPLOAD_DESTINATION = os.getenv("UPLOAD_DESTINATION", "gdp")
INSERT_QUERY_DICOM_META = """
    INSERT INTO calculation_status (
        study_uid,status,timestamp
    ) VALUES (%s, %s, %s)
"""

XNAT_URL = "http://xnat-nginx:80"
XNAT_USERNAME = "admin"
XNAT_PASSWORD = "admin"

QUERY_PATIENT = """SELECT * FROM public.dicom_insert WHERE patient_id = %s;"""


