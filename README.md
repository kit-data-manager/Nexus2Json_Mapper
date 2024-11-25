**Nexus File to JSON Document Converter**

**Description:**

This script is designed to process metadata from Nexus (.nxs) files and generate a JSON document based on a predefined schema. It automates the tasks of metadata extraction, structuring, and mapping, ensuring compatibility of of the required formats. The Nexus file is parsed using the Python library H5py, which enables efficient navigation and manipulation of HDF5-based files like Nexus.
The script is particularly suited for processing single Nexus files generated from advanced Photoelectric – High Energy (APE-HE) experiments, converting raw metadata of an experiment into a structured JSON format ready for storage.

**Inputs:**

Path to the Schema File (.json):
A JSON schema that defines the desired structure of the output document.
Path to the Nexus File (.nxs):
A Nexus file containing experimental metadata. Nexus files are built on the HDF5 format, offering a hierarchical structure to store multidimensional datasets efficiently.
Name of the Output File (.json):
The desired name of the output file where the structured metadata will be saved.

**Output:**

A structured JSON file containing the processed metadata aligned with the schema.

**How to execute the script:**

python script_name.py <path_to_schema.json> <path_to_nexus_file.nxs> <output_document.json>
