import csv
import io
import logging
import os
import shutil
import sys
import time
from pathlib import Path

import pydicom
from platipy.dicom.io.rtstruct_to_nifti import convert_rtstruct

import radiomics
import radiomics.featureextractor

logger = logging.getLogger(__name__)


class RadiomicsCalculator:
    def __init__(
        self, nifti_output_folder="niftidata", image_file="image.nii", settings="/radiomics_settings/Params.yaml"
    ):
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
        if Path(self.nifti_output_folder).exists():
            shutil.rmtree(self.nifti_output_folder)
            logger.info("Cleared niftidata folder from previous run.")

    def find_dicom_files(self, data_folder):
        """Walk data folder and find RTSTRUCT and CT folder."""
        logger.info("Searching for DICOM files...")
        try:
            for root, _dirs, files in os.walk(data_folder):
                for file in files:
                    file_path = Path(root) / file
                    try:
                        ds = pydicom.dcmread(file_path, stop_before_pixels=True, force=True)
                    except Exception:
                        logger.debug("Skipping unreadable file: %s", file_path)
                        continue

                    if not hasattr(ds, "Modality"):
                        logger.debug("Skipping file without Modality tag: %s", file_path)
                        continue

                    if ds.Modality == "RTSTRUCT":
                        self.rtstruct_path = file_path
                        logger.info("RTstruct found at %s", self.rtstruct_path)
                    elif ds.Modality == "CT" and self.ct_folder is None:
                        self.ct_folder = root
                        logger.info("CT folder found at: %s", self.ct_folder)

                if self.rtstruct_path and self.ct_folder:
                    break

            if not self.rtstruct_path or not Path(self.rtstruct_path).is_file():
                logger.error("RTstruct was not found in: %s", data_folder)
                sys.exit(1)
            if not self.ct_folder:
                logger.error("CT folder was not found in: %s", data_folder)
                sys.exit(1)

        except Exception:
            logger.exception("An error occurred finding DICOM files.")

    def convert_to_nifti(self, retries=3, delay=10):
        """Convert DICOM CT and RTSTRUCT to NIfTI format."""
        logger.info("Converting DICOM to NIfTI...")
        try:
            nifti_path = Path(self.nifti_output_folder)
            if not nifti_path.exists():
                nifti_path.mkdir(parents=True)

            for attempt in range(retries):
                try:
                    convert_rtstruct(
                        dcm_img=self.ct_folder,
                        dcm_rt_file=self.rtstruct_path,
                        output_img=self.image_file,
                        prefix="Mask_",
                        output_dir=self.nifti_output_folder,
                    )
                    logger.info("Conversion to NIfTI complete.")
                    return
                except Exception as e:
                    logger.warning("Conversion attempt %s failed: %s", attempt + 1, e)
                    if attempt < retries - 1:
                        logger.info("Retrying in %s seconds...", delay)
                        time.sleep(delay)
                    else:
                        raise

        except Exception:
            logger.exception("An error occurred during NIfTI conversion.")

    def list_roi_masks(self):
        """Return all NIfTI mask files in the output folder (everything except the image)."""
        image_path = Path(self.nifti_output_folder) / f"{self.image_file}.gz"
        return [p for p in Path(self.nifti_output_folder).iterdir() if p.is_file() and p != image_path]

    def calculate_single_roi(self, mask_path):
        """Extract radiomics features for a single ROI mask. Returns the features dict."""
        image_path = Path(self.nifti_output_folder) / f"{self.image_file}.gz"
        logger.info("Calculating features for ROI: %s", Path(mask_path).name)
        extractor = radiomics.featureextractor.RadiomicsFeatureExtractor(self.settings)
        return extractor.execute(str(image_path), str(mask_path))

    def calculate_features(self):
        """Run pyradiomics feature extraction on NIfTI data."""
        logger.info("Calculating radiomics features...")
        try:
            self.result_dict = {}
            for mask_path in self.list_roi_masks():
                self.result_dict[mask_path.name] = self.calculate_single_roi(mask_path)
            logger.info("All radiomics features calculated.")

        except Exception:
            logger.exception("An error occurred calculating radiomics features.")

    def get_csv_and_metadata(self):
        """Build CSV and metadata in memory."""
        ds_rtstruct = pydicom.dcmread(self.rtstruct_path, stop_before_pixels=True)
        patient_id = ds_rtstruct.PatientID

        ct_file = next(Path(self.ct_folder).iterdir())
        ds_ct = pydicom.dcmread(ct_file, stop_before_pixels=True)

        output = io.StringIO()
        fieldnames = list(next(iter(self.result_dict.values())).keys())
        writer = csv.DictWriter(output, fieldnames=["id", *fieldnames])
        writer.writeheader()
        for key, od in self.result_dict.items():
            row = {"id": key}
            row.update(od)
            writer.writerow(row)
        csv_content = output.getvalue()

        study_uid_label = str(ds_rtstruct.StudyInstanceUID).replace(".", "_")
        metadata = {
            "project": str(ds_ct.get("BodyPartExamined", "UNKNOWN")),
            "subject": str(ds_rtstruct.PatientID),
            "experiment": study_uid_label,
        }
        logger.info("PatientName: %s, PatientID: %s", ds_rtstruct.PatientName, ds_rtstruct.PatientID)

        filename = f"radiomics_results_{patient_id}.csv"
        return csv_content, metadata, filename

    def run(self, data_folder):
        """Run the full radiomics pipeline for a given data folder."""
        self.reset()
        self.find_dicom_files(data_folder)
        self.convert_to_nifti()
        self.calculate_features()
        return self.get_csv_and_metadata()
