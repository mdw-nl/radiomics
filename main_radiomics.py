import radiomics
from platipy.dicom.io.rtstruct_to_nifti import convert_rtstruct
import os
import csv
import logging
import pydicom
import sys
from config_handler import Config
from consumer import Consumer
import json


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger()

class radiomics_class:
    def __init__(self):        
        self.nifti_output_folder = "niftidata"
        self.image_file = 'image.nii'
        self.settings = "params.yaml"
        
    def convert_DCM(self):
        """This method makes from the dcm data nifti data. rtdose and rtplan can be in the folder but will not be used."""
        
        # Find the rtsttruct in the dicomdata folder this file will be used to create the nifti data.
        try:
            for file in os.listdir(self.dcm_data):
                file_path = os.path.join(self.dcm_data, file)
                ds = pydicom.dcmread(file_path, stop_before_pixels=True)
                
                if ds.Modality == "RTSTRUCT":
                    self.rtstruct_path = file_path
                    
                    logging.info(f"RTstruct has been found and is at {self.rtstruct_path}")
                    break
        
            if not os.path.isfile(self.rtstruct_path):
                logging.error(f"RTstruct was not found in: {self.dcm_data}")
                sys.exit(1)
                
        except Exception as e:
            logging.error(f"An error occurred finding the RTstruct: {e}", exc_info=True)
        
        # Create the nifti data            
        try:   
            if not os.path.exists(self.nifti_output_folder):
                os.makedirs(self.nifti_output_folder)
                
            convert_rtstruct(
                dcm_img = self.dcm_data,
                dcm_rt_file = self.rtstruct_path,
                output_img = self.image_file,     
                prefix = 'Mask_',              
                output_dir = self.nifti_output_folder
            )
            
            logging.info(f"Conversion to nifti complete, all files saved to {self.nifti_output_folder}.")
            
        except Exception as e:
            logging.error(f"An error occurred during the conversion to nifti in the convert_DCM method: {e}", exc_info=True)
        
    def get_results(self):
        """Applies the pyradiomics package to the nifti data to get the radiomics results"""    
    
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
        
    def save_results(self):
        """This saves the radiomics results into a csv file. It uses the ID of the patient in the title of the CSV file"""
        
        try:              
            if not os.path.exists(self.result_folder):
                os.makedirs(self.result_folder)
            
            ds = pydicom.dcmread(self.rtstruct_path, stop_before_pixels=True)
            id = ds.PatientID
            
            result_path = os.path.join(self.result_folder, f"radiomics_results_{id}")
            
            with open(result_path, 'w', newline='') as csvfile:
                fieldnames = list(next(iter(self.result_dict.values())).keys())
                
                writer = csv.DictWriter(csvfile, fieldnames=['id'] + fieldnames)
                writer.writeheader()

                for key, od in self.result_dict.items():
                    row = {'id': key}
                    row.update(od)
                    writer.writerow(row)
            
            logging.info(f"All the results are results are saved to {result_path}")
            
        except Exception as e:
            logging.error(f"An error occurred trying to save the results in the save_results method: {e}", exc_info=True)
    
    def run(self, ch, method, properties, body, executor):
        """This runs the whole folder from a message to RabbitMQ"""
        try:
            message_data = json.loads(body.decode("utf-8"))
            dicom_folder = message_data.get('input_folder_path')
            output_folder = message_data.get('output_folder_path')
            
            self.dcm_data = dicom_folder
            self.result_folder = output_folder
            
            self.convert_DCM()
            self.get_results()
            self.save_results()

            logging.info("Pipeline completed successfully.")

        except Exception as e:
            logging.error(f"An error occurred in the run method: {e}", exc_info=True)
        
if __name__ == '__main__':
    rabbitMQ_config = Config("rabbitMQ")
    cons = Consumer(rmq_config=rabbitMQ_config)
    cons.open_connection_rmq()
    cons.send_message("messages")
    engine = radiomics_class()
    cons.start_consumer(callback=engine.run)
