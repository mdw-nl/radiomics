"""Standalone integration runner for the radiomics pipeline.

Runs RadiomicsCalculator directly against a DICOM folder — no RabbitMQ,
PostgreSQL, or XNAT needed.

Usage:
    uv run --group test python run_integration.py --dicom-folder /path/to/dicom
    uv run --group test python run_integration.py --dicom-folder /path/to/dicom --output-dir ./results
    uv run --group test python run_integration.py --help

The DICOM folder must contain at least one file with Modality == "RTSTRUCT"
and at least one file with Modality == "CT" (subdirectories are fine).
"""

import argparse
import logging
import os
import sys
import tempfile
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).parent
RADIOMICS_SRC = PROJECT_ROOT / "radiomics"
PARAMS_DEFAULT = PROJECT_ROOT / "radiomics_settings" / "Params.yaml"

sys.path.insert(0, str(RADIOMICS_SRC))

from radiomics_calculator import RadiomicsCalculator  # noqa: E402


def parse_args():
    parser = argparse.ArgumentParser(
        description="Run the radiomics pipeline standalone (no RabbitMQ/DB/XNAT needed).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--dicom-folder",
        default=os.environ.get("RADIOMICS_TEST_DICOM_FOLDER"),
        help="Path to DICOM test data folder. Can also be set via RADIOMICS_TEST_DICOM_FOLDER.",
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Directory to write results CSV (default: current directory).",
    )
    parser.add_argument(
        "--params-yaml",
        default=str(PARAMS_DEFAULT),
        help=f"Path to PyRadiomics Params.yaml (default: {PARAMS_DEFAULT})",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    if not args.dicom_folder:
        logger.error("No DICOM folder specified. Use --dicom-folder or set RADIOMICS_TEST_DICOM_FOLDER.")
        sys.exit(1)

    dicom_folder = Path(args.dicom_folder)
    if not dicom_folder.is_dir():
        logger.error("DICOM folder does not exist: %s", dicom_folder)
        sys.exit(1)

    params_yaml = Path(args.params_yaml)
    if not params_yaml.is_file():
        logger.error("Params.yaml not found: %s", params_yaml)
        sys.exit(1)

    output_dir = Path(args.output_dir) if args.output_dir else Path.cwd()
    output_dir.mkdir(parents=True, exist_ok=True)

    logger.info("Starting radiomics pipeline...")
    logger.info("  DICOM folder : %s", dicom_folder)
    logger.info("  Params YAML  : %s", params_yaml)
    logger.info("  Output dir   : %s", output_dir)

    with tempfile.TemporaryDirectory(prefix="radiomics_nifti_") as nifti_tmp:
        calc = RadiomicsCalculator(
            nifti_output_folder=nifti_tmp,
            image_file="image.nii",
            settings=str(params_yaml),
        )
        csv_content, metadata, filename = calc.run(str(dicom_folder))

    out_path = output_dir / filename
    out_path.write_text(csv_content)

    row_count = csv_content.count("\n") - 1
    logger.info("Pipeline completed successfully.")
    logger.info("  Patient ID   : %s", metadata["subject"])
    logger.info("  Study UID    : %s", metadata["experiment"])
    logger.info("  Body part    : %s", metadata["project"])
    logger.info("  Output file  : %s", out_path)
    logger.info("  CSV rows     : %d", row_count)


if __name__ == "__main__":
    main()
