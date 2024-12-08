**APE-HE NeXus File to JSON Document Converter**

**Description:**

This script is designed to automate the tasks of _extracting_ metadata from NeXus (.nxs) files generated by the Advanced Photoelectric Effect - High Energy (APE-HE) experiment, and the _mapping_ of extracted metadata to a predifined JSON schema to generate a structured JSON document. The NeXus file is parsed using the Python library **H5py**, which allows to efficiently navigate and manipulate HDF5-based files like NeXus.
The current script is particularly processing a single Nexus file from APE-HE experiments, converting the extracted metadata into a structured JSON format ready for storage.

**Inputs:**

- Path to the Schema File (.json):
A JSON schema that defines the desired structure of the output document.
- Path to the NeXus File (.nxs):
A NeXus file containing experimental metadata and data.
- Name of the Output File (.json):
The desired name of the output file where the metadata document will be saved.

**Output:**

A structured JSON file containing the extracted metadata aligned with the schema.

**How to execute the script:**

`python NexusMapping_cmdline.py <path_to_schema.json> <path_to_NeXus_file.nxs> <output_document.json>`

