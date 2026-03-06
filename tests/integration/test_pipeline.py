"""Integration tests for the radiomics pipeline.

Runs RadiomicsCalculator.run() directly against real DICOM data —
no RabbitMQ, PostgreSQL, or XNAT required.

Required DICOM folder structure (subdirectories are fine, tree is walked):
    test_data/
    ├── CT/
    │   ├── CT.*.dcm   (Modality == "CT", should include BodyPartExamined tag)
    │   └── ...
    └── RTSTRUCT/
        └── RS.*.dcm   (Modality == "RTSTRUCT", must reference the CT series)

Supply via CLI:
    pytest tests/integration/ -v -s --dicom-folder /path/to/dicom

Or via environment variable:
    RADIOMICS_TEST_DICOM_FOLDER=/path/to/dicom pytest tests/integration/ -v -s

Per-ROI tests (TestPerROIResult) run one ROI at a time as features are computed,
so results appear incrementally rather than waiting for the full pipeline to finish.
"""

import csv
import io
import logging
from datetime import datetime

import pytest

logger = logging.getLogger(__name__)

EXPECTED_METADATA_KEYS = {"project", "subject", "experiment"}
MINIMUM_FEATURE_COLUMNS = 10


@pytest.mark.integration
class TestRadiomicsPipeline:
    def test_returns_three_values(self, pipeline_result):
        csv_content, metadata, filename, _ = pipeline_result
        assert csv_content is not None
        assert metadata is not None
        assert filename is not None

    def test_csv_is_non_empty(self, pipeline_result):
        csv_content, _, _, _ = pipeline_result
        assert len(csv_content.strip()) > 0, "CSV content is empty"

    def test_csv_has_header_and_data_rows(self, pipeline_result):
        csv_content, _, _, _ = pipeline_result
        rows = list(csv.DictReader(io.StringIO(csv_content)))
        assert len(rows) >= 1, "CSV has no data rows"

    def test_csv_has_id_column(self, pipeline_result):
        csv_content, _, _, _ = pipeline_result
        reader = csv.DictReader(io.StringIO(csv_content))
        assert "id" in (reader.fieldnames or []), f"'id' column missing. Got: {reader.fieldnames}"

    def test_csv_has_minimum_feature_columns(self, pipeline_result):
        csv_content, _, _, _ = pipeline_result
        reader = csv.DictReader(io.StringIO(csv_content))
        feature_cols = [f for f in (reader.fieldnames or []) if f != "id"]
        assert len(feature_cols) >= MINIMUM_FEATURE_COLUMNS, (
            f"Expected at least {MINIMUM_FEATURE_COLUMNS} feature columns, got {len(feature_cols)}"
        )

    def test_csv_feature_values_are_numeric(self, pipeline_result):
        csv_content, _, _, _ = pipeline_result
        reader = csv.DictReader(io.StringIO(csv_content))
        for row in reader:
            for key, val in row.items():
                if key == "id" or key.startswith("diagnostics_"):
                    continue
                try:
                    float(val)
                except (ValueError, TypeError):
                    pytest.fail(f"Non-numeric value in feature column '{key}': {val!r}")

    def test_metadata_has_required_keys(self, pipeline_result):
        _, metadata, _, _ = pipeline_result
        missing = EXPECTED_METADATA_KEYS - set(metadata.keys())
        assert not missing, f"Metadata missing keys: {missing}"

    def test_metadata_values_are_non_empty(self, pipeline_result):
        _, metadata, _, _ = pipeline_result
        for key in EXPECTED_METADATA_KEYS:
            assert metadata.get(key), f"Metadata key '{key}' is empty or missing"

    def test_metadata_experiment_has_no_dots(self, pipeline_result):
        _, metadata, _, _ = pipeline_result
        assert "." not in metadata["experiment"], (
            "experiment (StudyInstanceUID) should have dots replaced with underscores"
        )

    def test_filename_is_csv(self, pipeline_result):
        _, _, filename, _ = pipeline_result
        assert filename.endswith(".csv"), f"Filename does not end with .csv: {filename}"

    def test_filename_contains_patient_id(self, pipeline_result):
        _, metadata, filename, _ = pipeline_result
        patient_id = metadata["subject"]
        assert patient_id in filename, f"Filename '{filename}' does not contain patient ID '{patient_id}'"

    def test_output_csv_saved_to_disk(self, pipeline_result):
        csv_content, _, filename, output_dir = pipeline_result
        out_path = output_dir / filename
        out_path.write_text(csv_content)
        assert out_path.exists()
        assert out_path.stat().st_size > 0
        logger.info("CSV saved to: %s", out_path)


@pytest.mark.integration
class TestPerROIResult:
    """Per-ROI tests that run one at a time as each ROI's features are computed."""

    def test_roi_returns_features(self, roi_result):
        roi_name, features, _ = roi_result
        assert features, f"No features returned for ROI: {roi_name}"

    def test_roi_has_minimum_feature_count(self, roi_result):
        roi_name, features, _ = roi_result
        feature_keys = [k for k in features if not k.startswith("diagnostics_")]
        assert len(feature_keys) >= MINIMUM_FEATURE_COLUMNS, (
            f"ROI '{roi_name}': expected at least {MINIMUM_FEATURE_COLUMNS} features, got {len(feature_keys)}"
        )

    def test_roi_feature_values_are_numeric(self, roi_result):
        roi_name, features, _ = roi_result
        for key, val in features.items():
            if key.startswith("diagnostics_"):
                continue
            try:
                float(val)
            except (ValueError, TypeError):
                pytest.fail(f"ROI '{roi_name}': non-numeric value in feature '{key}': {val!r}")

    def test_roi_metadata_has_required_keys(self, roi_result):
        _, _, metadata = roi_result
        missing = EXPECTED_METADATA_KEYS - set(metadata.keys())
        assert not missing, f"Metadata missing keys: {missing}"

    def test_roi_metadata_values_are_non_empty(self, roi_result):
        _, _, metadata = roi_result
        for key in EXPECTED_METADATA_KEYS:
            assert metadata.get(key), f"Metadata key '{key}' is empty or missing"

    def test_roi_metadata_experiment_has_no_dots(self, roi_result):
        _, _, metadata = roi_result
        assert "." not in metadata["experiment"], (
            "experiment (StudyInstanceUID) should have dots replaced with underscores"
        )


@pytest.mark.integration
class TestPostgresOutput:
    """Verify that pipeline results can be written to and read back from PostgreSQL.

    Requires a running PostgreSQL instance. Supply connection details via CLI flags
    or environment variables:

        just integration-test /path/to/dicom --pg-host localhost
        PG_HOST=localhost pytest tests/integration/ -v -s --dicom-folder /path/to/dicom
    """

    TABLE = "calculation_status"

    def _ensure_table(self, pg):
        pg.create_table(self.TABLE, {"study_uid": "TEXT", "status": "TEXT", "timestamp": "TIMESTAMPTZ"})

    def test_can_connect(self, postgres_interface):
        result = postgres_interface.fetch_one("SELECT 1")
        assert result == (1,), "Basic connectivity check failed"

    def test_table_can_be_created(self, postgres_interface):
        self._ensure_table(postgres_interface)
        assert postgres_interface.check_table_exists(self.TABLE)

    def test_can_insert_pipeline_result(self, postgres_interface, pipeline_result):
        _, metadata, _, _ = pipeline_result
        self._ensure_table(postgres_interface)
        postgres_interface.insert(
            self.TABLE,
            {"study_uid": metadata["experiment"], "status": "completed", "timestamp": datetime.now()},
        )
        row = postgres_interface.fetch_one(
            f"SELECT study_uid, status FROM {self.TABLE} WHERE study_uid = %s",
            (metadata["experiment"],),
        )
        assert row is not None, "Inserted row not found"
        assert row[0] == metadata["experiment"]
        assert row[1] == "completed"

    def test_cleanup(self, postgres_interface, pipeline_result):
        _, metadata, _, _ = pipeline_result
        postgres_interface.delete(self.TABLE, {"study_uid": metadata["experiment"]})
        row = postgres_interface.fetch_one(
            f"SELECT 1 FROM {self.TABLE} WHERE study_uid = %s",
            (metadata["experiment"],),
        )
        assert row is None, "Row was not deleted"
