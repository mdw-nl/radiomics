import radiomics
from platipy.dicom.io.rtstruct_to_nifti import convert_rtstruct
import os
import csv
import logging
import pydicom
import sys
from config_handler import Config
from consumer import Consumer
from PostgresInterface import PostgresInterface
from global_var import QUERY_UID
from xnat_sender import SendDICOM
import shutil
import time

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger()


def connect_db():
    """Function to connect to the postgress database"""
    postgres_config = Config("postgres")
    if postgres_config is None:
        raise Exception("Postgres config is None")
    config_dict_db = postgres_config.config
    host, port, user, pwd, db = config_dict_db["host"], config_dict_db["port"], \
        config_dict_db["username"], config_dict_db["password"], config_dict_db["db"]
    db = PostgresInterface(host=host, database=db, user=user, password=pwd, port=port)
    db.connect()
    logging.info("Connected to the database")
    return db

class radiomics_class:
    def __init__(self):        
        self.nifti_output_folder = "niftidata"
        self.image_file = 'image.nii'
        self.settings = "/radiomics_settings/Params.yaml"
        self.db = connect_db()
        self.xnat_sender = SendDICOM()
    
    def get_folder_from_db(self, study_uid):
        """Function to get the patient data based on study uid and the query defined in global variables"""
        result = self.db.fetch_one(QUERY_UID, params=(study_uid,))
        if result:
            file_path = result[0]
            return os.path.dirname(os.path.dirname(file_path))
        else:
            logging.error(f"No folder found in DB for study UID: {study_uid}")
            return None    
    
    def convert_DCM(self, data_folder, retries=3, delay=10):
        """Function to convert DCM data to Nifti"""
        logging.info("Converting dicom to nifti...")
        try:
            for root, dirs, files in os.walk(data_folder):
                for file in files:
                    file_path = os.path.join(root, file)
                    try:
                        ds = pydicom.dcmread(file_path, stop_before_pixels=True, force=True)
                    except Exception:
                        logging.debug(f"Skipping unreadable file: {file_path}")
                        continue

                    # Skip files without Modality tag
                    if not hasattr(ds, 'Modality'):
                        logging.debug(f"Skipping file without Modality tag: {file_path}")
                        continue

                    if ds.Modality == "RTSTRUCT":
                        self.rtstruct_path = file_path
                        logging.info(f"RTstruct found at {self.rtstruct_path}")
                    elif ds.Modality == "CT" and self.ct_folder is None:
                        self.ct_folder = root
                        logging.info(f"CT folder found at: {self.ct_folder}")

                # Stop walking once both are found
                if self.rtstruct_path and self.ct_folder:
                    break

            if not self.rtstruct_path or not os.path.isfile(self.rtstruct_path):
                logging.error(f"RTstruct was not found in: {data_folder}")
                sys.exit(1)
            if not self.ct_folder:
                logging.error(f"CT folder was not found in: {data_folder}")
                sys.exit(1)

        except Exception as e:
            logging.error(f"An error occurred finding the RTstruct: {e}", exc_info=True)
        
        try:   
            if not os.path.exists(self.nifti_output_folder):
                os.makedirs(self.nifti_output_folder)
            
            # Try to get the data multiple times cause sometimes the data is not yet complete in postgress and the conversion fails
            for attempt in range(retries):
                    try:
                        convert_rtstruct(
                            dcm_img=self.ct_folder,
                            dcm_rt_file=self.rtstruct_path,
                            output_img=self.image_file,
                            prefix='Mask_',
                            output_dir=self.nifti_output_folder
                        )
                        logging.info("Conversion to nifti complete.")
                        return 
                    except Exception as e:
                        logging.warning(f"Conversion attempt {attempt + 1} failed: {e}")
                        if attempt < retries - 1:
                            logging.info(f"Retrying in {delay} seconds...")
                            time.sleep(delay)
                        else:
                            raise
            
        except Exception as e:
            logging.error(f"An error occurred during the conversion to nifti in the convert_DCM method: {e}", exc_info=True)
        
    def get_results(self):
        """Applies the pyradiomics package to the nifti data to get the radiomics results"""    
        logging.info("Calculating the radiomics features...")
        
        try:
            image_path = os.path.join(self.nifti_output_folder, f"{self.image_file}.gz")
            extractor = radiomics.featureextractor.RadiomicsFeatureExtractor(self.settings)    
            self.result_dict = {}
            
            for file in os.listdir("niftidata"):
                file_path = os.path.join("niftidata", file)
                
                if file_path == image_path:
                    continue
                    
                result = extractor.execute(image_path, file_path)
                self.result_dict[file] = result
                
            logging.info("All the radiomics results are completed")
            
        except Exception as e:
            logging.error(f"An error occurred trying to get the radiomics results in the get_result method: {e}", exc_info=True)

    def get_csv_and_metadata(self):
        """Build CSV and metadata in memory."""
        # Read RTSTRUCT for patient info
        ds_rtstruct = pydicom.dcmread(self.rtstruct_path, stop_before_pixels=True)
        patient_id = ds_rtstruct.PatientID

        # Read a CT file to get BodyPartExamined (treatment site is added to CT files)
        ct_file = os.path.join(self.ct_folder, os.listdir(self.ct_folder)[0])
        ds_ct = pydicom.dcmread(ct_file, stop_before_pixels=True)

        # Build CSV in memory
        output = csv.StringIO()
        fieldnames = list(next(iter(self.result_dict.values())).keys())
        writer = csv.DictWriter(output, fieldnames=['id'] + fieldnames)
        writer.writeheader()
        for key, od in self.result_dict.items():
            row = {'id': key}
            row.update(od)
            writer.writerow(row)
        csv_content = output.getvalue()

        # Build metadata dict
        study_uid_label = str(ds_rtstruct.StudyInstanceUID).replace(".", "_")
        metadata = {
            "project": str(ds_ct.get("BodyPartExamined", "UNKNOWN")),
            "subject": str(ds_rtstruct.PatientID),     # try PatientID instead
            "experiment": study_uid_label
        }
        logging.info(f"PatientName: {ds_rtstruct.PatientName}, PatientID: {ds_rtstruct.PatientID}")

        filename = f"radiomics_results_{patient_id}.csv"
        return csv_content, metadata, filename

    def run(self, ch, method, properties, body, executor):
        ch.basic_ack(delivery_tag=method.delivery_tag)

        # Reset per-run state
        self.rtstruct_path = None
        self.ct_folder = None
        self.result_dict = {}
        if os.path.exists(self.nifti_output_folder):
            shutil.rmtree(self.nifti_output_folder)
            logging.info("Cleared niftidata folder from previous run.")

        study_uid = body.decode("utf-8").strip()
        logging.info(f"Received study UID: {study_uid}")

        data_folder = self.get_folder_from_db(study_uid)
        if not data_folder:
            logging.error(f"Could not resolve folder for study UID: {study_uid}")
            return

        try:
            self.convert_DCM(data_folder)
            self.get_results()
            csv_content, metadata, filename = self.get_csv_and_metadata()
            self.xnat_sender.upload_to_xnat(csv_content, metadata, filename)
            logging.info("Radiomics results sent to XNAT successfully.")

        except Exception as e:
            logging.error(f"An error occurred in the run method: {e}", exc_info=True)

if __name__ == '__main__':
    rabbitMQ_config = Config("radiomics")
    cons = Consumer(rmq_config=rabbitMQ_config)
    cons.open_connection_rmq()
    cons.create_channel()
    cons.channel.queue_declare(
        queue=rabbitMQ_config.config["queue_name"],
        durable=True
    )

    engine = radiomics_class()
    cons.start_consumer(callback=engine.run)