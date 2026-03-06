import logging
import os
import shutil
import sys
import tempfile
from pathlib import Path

import pydicom
import pytest

logger = logging.getLogger(__name__)

RADIOMICS_SRC = Path(__file__).parent.parent / "radiomics"
PARAMS_DEFAULT = Path(__file__).parent.parent / "radiomics_settings" / "Params.yaml"

_state: dict = {"calc": None, "nifti_tmpdir": None}
_roi_mask_names: list = []


def _setup_sys_path():
    project_root = str(RADIOMICS_SRC.parent)
    while project_root in sys.path:
        sys.path.remove(project_root)
    for key in list(sys.modules):
        if key == "radiomics" or key.startswith("radiomics."):
            del sys.modules[key]
    if str(RADIOMICS_SRC) not in sys.path:
        sys.path.insert(0, str(RADIOMICS_SRC))


def pytest_addoption(parser):
    parser.addoption(
        "--dicom-folder",
        action="store",
        default=None,
        help="Path to DICOM test data folder (must contain RTSTRUCT + CT files)",
    )
    parser.addoption(
        "--output-dir",
        action="store",
        default=None,
        help="Directory to write CSV output for inspection (optional)",
    )
    parser.addoption(
        "--params-yaml",
        action="store",
        default=str(PARAMS_DEFAULT),
        help=f"Path to PyRadiomics Params.yaml settings file (default: {PARAMS_DEFAULT})",
    )
    parser.addoption("--pg-host", action="store", default=None, help="PostgreSQL host (or PG_HOST env var)")
    parser.addoption("--pg-port", action="store", default=None, help="PostgreSQL port (or PG_PORT env var)")
    parser.addoption("--pg-user", action="store", default=None, help="PostgreSQL user (or PG_USER env var)")
    parser.addoption("--pg-password", action="store", default=None, help="PostgreSQL password (or PG_PASSWORD env var)")
    parser.addoption("--pg-database", action="store", default=None, help="PostgreSQL database (or PG_DATABASE env var)")


def pytest_sessionstart(session):
    dicom_folder = session.config.getoption("--dicom-folder", default=None) or os.environ.get(
        "RADIOMICS_TEST_DICOM_FOLDER"
    )
    if not dicom_folder or not Path(dicom_folder).is_dir():
        return

    params_yaml = session.config.getoption("--params-yaml")

    _setup_sys_path()
    from radiomics_calculator import RadiomicsCalculator  # noqa: PLC0415

    _state["nifti_tmpdir"] = tempfile.mkdtemp(prefix="niftidata_")
    calc = RadiomicsCalculator(
        nifti_output_folder=_state["nifti_tmpdir"],
        image_file="image.nii",
        settings=params_yaml,
    )
    calc.reset()
    calc.find_dicom_files(dicom_folder)
    calc.convert_to_nifti()
    _state["calc"] = calc
    _roi_mask_names.extend(p.name for p in calc.list_roi_masks())
    logger.info("ROI masks ready for testing: %s", _roi_mask_names)


def pytest_sessionfinish(session, exitstatus):
    nifti_tmpdir = _state["nifti_tmpdir"]
    if nifti_tmpdir and Path(nifti_tmpdir).exists():
        shutil.rmtree(nifti_tmpdir, ignore_errors=True)


def pytest_generate_tests(metafunc):
    if "roi_result" in metafunc.fixturenames:
        metafunc.parametrize("roi_result", _roi_mask_names, indirect=True)


@pytest.fixture(scope="session", autouse=True)
def add_src_to_path():
    _setup_sys_path()


@pytest.fixture(scope="session")
def output_dir(request, tmp_path_factory):
    requested = request.config.getoption("--output-dir")
    if requested:
        out = Path(requested)
        out.mkdir(parents=True, exist_ok=True)
        return out
    return tmp_path_factory.mktemp("radiomics_output")


@pytest.fixture(scope="session")
def pipeline_result(output_dir):
    calc = _state["calc"]
    if calc is None:
        pytest.skip("No DICOM folder provided. Use --dicom-folder or set RADIOMICS_TEST_DICOM_FOLDER.")
    calc.calculate_features()
    csv_content, metadata, filename = calc.get_csv_and_metadata()
    return csv_content, metadata, filename, output_dir


@pytest.fixture(scope="session")
def postgres_interface(request):
    from PostgresInterface import PostgresInterface  # noqa: PLC0415

    def opt(name, env):
        return request.config.getoption(name) or os.environ.get(env)

    host = opt("--pg-host", "PG_HOST")
    if not host:
        pytest.skip("No PostgreSQL host provided. Use --pg-host or set PG_HOST.")

    pg = PostgresInterface(
        host=host,
        port=int(opt("--pg-port", "PG_PORT") or 5432),
        database=opt("--pg-database", "PG_DATABASE") or "postgres",
        user=opt("--pg-user", "PG_USER") or "postgres",
        password=opt("--pg-password", "PG_PASSWORD") or "postgres",
    )
    pg.connect()
    yield pg
    pg.disconnect()


@pytest.fixture
def roi_result(request):
    calc = _state["calc"]
    if calc is None:
        pytest.skip("No DICOM folder provided. Use --dicom-folder or set RADIOMICS_TEST_DICOM_FOLDER.")

    mask_name = request.param
    mask_path = Path(calc.nifti_output_folder) / mask_name
    features = calc.calculate_single_roi(mask_path)

    ds_rtstruct = pydicom.dcmread(calc.rtstruct_path, stop_before_pixels=True)
    ct_file = next(Path(calc.ct_folder).iterdir())
    ds_ct = pydicom.dcmread(ct_file, stop_before_pixels=True)
    metadata = {
        "project": str(ds_ct.get("BodyPartExamined", "UNKNOWN")),
        "subject": str(ds_rtstruct.PatientID),
        "experiment": str(ds_rtstruct.StudyInstanceUID).replace(".", "_"),
    }
    return mask_name, features, metadata
