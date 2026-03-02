import radiomics
from platipy.dicom.io.rtstruct_to_nifti import convert_rtstruct
import os
import csv
import logging
import pydicom
import sys
import shutil
import time

logger = logging.getLogger(__name__)


class RadiomicsCalculator:
    def __init__(self, nifti_output_folder="niftidata", image_file="image.nii",
                 settings="/radiomics_settings/Params.yaml"):
        self.nifti_output_folder = nifti_output_folder
        self.image_file = image_file
        self.settings = settings
        self.rtstruct_path = None
        self.ct_folder = None
        self.result_dict = {}

    def reset(self):
        """Reset state between runs."""
        self.rtstruct_path = None
        self.ct_folder = None
        self.result_dict = {}
        if os.path.exists(self.nifti_output_folder):
            shutil.rmtree(self.nifti_output_folder)
            logging.info("Cleared niftidata folder from previous run.")

    def find_dicom_files(self, data_folder):
        """Walk data folder and find RTSTRUCT and CT folder."""
        logging.info("Searching for DICOM files...")
        try:
            for root, dirs, files in os.walk(data_folder):
                for file in files:
                    file_path = os.path.join(root, file)
                    try:
                        ds = pydicom.dcmread(file_path, stop_before_pixels=True, force=True)
                    except Exception:
                        logging.debug(f"Skipping unreadable file: {file_path}")
                        continue

                    if not hasattr(ds, 'Modality'):
                        logging.debug(f"Skipping file without Modality tag: {file_path}")
                        continue

                    if ds.Modality == "RTSTRUCT":
                        self.rtstruct_path = file_path
                        logging.info(f"RTstruct found at {self.rtstruct_path}")
                    elif ds.Modality == "CT" and self.ct_folder is None:
                        self.ct_folder = root
                        logging.info(f"CT folder found at: {self.ct_folder}")

                if self.rtstruct_path and self.ct_folder:
                    break

            if not self.rtstruct_path or not os.path.isfile(self.rtstruct_path):
                logging.error(f"RTstruct was not found in: {data_folder}")
                sys.exit(1)
            if not self.ct_folder:
                logging.error(f"CT folder was not found in: {data_folder}")
                sys.exit(1)

        except Exception as e:
            logging.error(f"An error occurred finding DICOM files: {e}", exc_info=True)

    def convert_to_nifti(self, retries=3, delay=10):
        """Convert DICOM CT and RTSTRUCT to NIfTI format."""
        logging.info("Converting DICOM to NIfTI...")
        try:
            if not os.path.exists(self.nifti_output_folder):
                os.makedirs(self.nifti_output_folder)

            for attempt in range(retries):
                try:
                    convert_rtstruct(
                        dcm_img=self.ct_folder,
                        dcm_rt_file=self.rtstruct_path,
                        output_img=self.image_file,
                        prefix='Mask_',
                        output_dir=self.nifti_output_folder
                    )
                    logging.info("Conversion to NIfTI complete.")
                    return
                except Exception as e:
                    logging.warning(f"Conversion attempt {attempt + 1} failed: {e}")
                    if attempt < retries - 1:
                        logging.info(f"Retrying in {delay} seconds...")
                        time.sleep(delay)
                    else:
                        raise

        except Exception as e:
            logging.error(f"An error occurred during NIfTI conversion: {e}", exc_info=True)

    def calculate_features(self):
        """Run pyradiomics feature extraction on NIfTI data."""
        logging.info("Calculating radiomics features...")
        try:
            image_path = os.path.join(self.nifti_output_folder, f"{self.image_file}.gz")
            extractor = radiomics.featureextractor.RadiomicsFeatureExtractor(self.settings)
            self.result_dict = {}

            for file in os.listdir(self.nifti_output_folder):
                file_path = os.path.join(self.nifti_output_folder, file)
                if file_path == image_path:
                    continue
                result = extractor.execute(image_path, file_path)
                self.result_dict[file] = result

            logging.info("All radiomics features calculated.")

        except Exception as e:
            logging.error(f"An error occurred calculating radiomics features: {e}", exc_info=True)

    def get_csv_and_metadata(self):
        """Build CSV and metadata in memory."""
        ds_rtstruct = pydicom.dcmread(self.rtstruct_path, stop_before_pixels=True)
        patient_id = ds_rtstruct.PatientID

        ct_file = os.path.join(self.ct_folder, os.listdir(self.ct_folder)[0])
        ds_ct = pydicom.dcmread(ct_file, stop_before_pixels=True)

        output = csv.StringIO()
        fieldnames = list(next(iter(self.result_dict.values())).keys())
        writer = csv.DictWriter(output, fieldnames=['id'] + fieldnames)
        writer.writeheader()
        for key, od in self.result_dict.items():
            row = {'id': key}
            row.update(od)
            writer.writerow(row)
        csv_content = output.getvalue()

        study_uid_label = str(ds_rtstruct.StudyInstanceUID).replace(".", "_")
        metadata = {
            "project": str(ds_ct.get("BodyPartExamined", "UNKNOWN")),
            "subject": str(ds_rtstruct.PatientID),
            "experiment": study_uid_label
        }
        logging.info(f"PatientName: {ds_rtstruct.PatientName}, PatientID: {ds_rtstruct.PatientID}")

        filename = f"radiomics_results_{patient_id}.csv"
        return csv_content, metadata, filename

    def run(self, data_folder):
        """Run the full radiomics pipeline for a given data folder."""
        self.reset()
        self.find_dicom_files(data_folder)
        self.convert_to_nifti()
        self.calculate_features()
        return self.get_csv_and_metadata()