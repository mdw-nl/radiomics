# Radiomics Pipeline

Extracts radiomics features from DICOM data (CT + RTSTRUCT) using [PyRadiomics](https://pyradiomics.readthedocs.io/en/latest/) and writes the results to a CSV file. Designed to run as a standalone tool or as a RabbitMQ consumer in the DIGIONE pipeline.

---

## Prerequisites

### just

[`just`](https://github.com/casey/just) is a command runner used to simplify common tasks in this project. Install it with:

```bash
# macOS
brew install just

# Linux (via cargo)
cargo install just

# Linux (via pre-built binary)
curl --proto '=https' --tlsv1.2 -sSf https://just.systems/install.sh | bash -s -- --to ~/.local/bin
```

Run `just` (no arguments) to see all available commands.

### uv

[`uv`](https://github.com/astral-sh/uv) is the Python package manager used in this project. Install it with:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

---

## Getting Started

### 1. Install dependencies

```bash
just install
```

For running the pipeline or tests locally (includes PyRadiomics and platipy):

```bash
just install-test
```

### 2. Run the pipeline on a DICOM folder

```bash
just run-pipeline /path/to/dicom
```

Results are written to `./radiomics_results/` by default. To specify a custom output directory:

```bash
just run-pipeline /path/to/dicom ./my-output
```

## DICOM folder structure

The pipeline walks the folder recursively. It expects:

```
/path/to/dicom/
├── CT/
│   └── CT.*.dcm        (Modality == "CT", ideally includes BodyPartExamined)
└── RTSTRUCT/
    └── RS.*.dcm         (Modality == "RTSTRUCT", must reference the CT series)
```

Subdirectories are fine — the walker finds the first CT folder and RTSTRUCT file it encounters.

---

## Output

The pipeline produces a single CSV file per patient:

```
radiomics_results_<PatientID>.csv
```

Each row corresponds to one ROI (structure) from the RTSTRUCT file. Columns are:

| Column                  | Description                                  |
| ----------------------- | -------------------------------------------- |
| `id`                    | Mask filename (e.g. `Mask_BrainStem.nii.gz`) |
| `diagnostics_*`         | PyRadiomics diagnostic fields                |
| `original_shape_*`      | Shape features                               |
| `original_firstorder_*` | First-order statistics                       |
| `original_glcm_*`       | Grey Level Co-occurrence Matrix features     |
| `original_glrlm_*`      | Grey Level Run Length Matrix features        |
| `original_glszm_*`      | Grey Level Size Zone Matrix features         |
| `original_gldm_*`       | Grey Level Dependence Matrix features        |

---

## Configuring feature extraction

Feature extraction is controlled by `radiomics_settings/Params.yaml`. The defaults extract shape, first-order, GLCM (SumAverage disabled), GLRLM, GLSZM, and GLDM features on the original image with:

- Bin width: 25
- Interpolator: BSpline
- Resampling: disabled

See the [PyRadiomics documentation](https://pyradiomics.readthedocs.io/en/latest/customization.html) for the full list of available settings.

> **Note:** Large ROIs (e.g. whole-body outlines) can significantly increase computation time due to voxel-wise feature calculations.

---

## Running integration tests

Tests use real DICOM data — no RabbitMQ, PostgreSQL, or XNAT required.

```bash
# Run all integration tests against a DICOM folder
just integration-test /path/to/dicom

# Write CSV output to a specific directory for inspection
just integration-test /path/to/dicom ./results
```

The test suite runs two classes:

- **`TestRadiomicsPipeline`** — validates the aggregate CSV (structure, content, metadata, filename)
- **`TestPerROIResult`** — validates each ROI individually as its features are computed, so results appear incrementally

You can also set the folder via environment variable:

```bash
RADIOMICS_TEST_DICOM_FOLDER=/path/to/dicom uv run --group test pytest tests/integration/ -v -s
```

---

## Pipeline integration (DIGIONE / RabbitMQ)

When running inside the DIGIONE pipeline, the service listens on RabbitMQ for messages containing a DICOM folder path, computes features, and uploads results to XNAT.

```bash
# Start all pipeline services
just up

# View logs
just logs

# Stop services
just down
```

---

## Python version note

This project requires **Python 3.10** (3.10–3.11). PyRadiomics does not support Python 3.12+.
