This repository provides a Python-based tool for extracting radiomics features from a folder of DICOM files using PyRadiomics, and saving the results into a CSV file. It is also integrated with RabbitMQ for spipeline integration.

Important requirements for running pyradiomics:
-Python 3.9 (Python 3.7–3.9 are supported; Python 3.10+ is not supported by PyRadiomics)
-numpy < 2.0.0 (PyRadiomics is not compatible with numpy ≥ 2.0.0)

Important Notes:
-Large ROIs (e.g., whole-body outlines) can cause very long computation times due to voxel-wise feature calculations.

Radiomics extraction is done using default PyRadiomics parameters; adjust the settings in the params.yaml file to get the desired settings. For more information about this go to this 
webpage: https://pyradiomics.readthedocs.io/en/latest/index.html or go to the Github repository.

If you want to use this script as seperate script so not in the DIGIONE pipeline then self.settings = "/app/radiomics_settings/Params.yaml" should be changed to self.settings = "Params.yaml"