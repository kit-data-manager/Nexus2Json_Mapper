import h5py
import hyperspy.api as hs
import pandas as pd
import numpy as np
import json
import re
import argparse

### Defined functions ###
def extract_metadata(obj, group=''):
    """
    Recursive function to travel all over the nexus file tree and extract all the metadata as a dictionary.
    Inputs: obj: h5py object
           group: path to a directory (string)
    Output: metadata: dictionary
    """
    # Recursive function to extract metadata from the Nexus file.
    metadata = {}
    if isinstance(obj, h5py.Group):
        for key in obj.keys():
            full_directory = f"{group}/{key.strip()}" if group else key
            metadata.update(extract_metadata(obj[key], full_directory))
    elif isinstance(obj, h5py.Dataset):
        if isinstance(obj[()], np.ndarray):
            metadata[group] = obj[()]
        else:
            metadata[group] = obj[()].decode('utf-8')
    return metadata

def extract_keys_from_myDict(myDict, parent_key=None):
    """
    This function extracts the directory path from a schema that specifies where the metadata will be stored.
    Inputs: myDict: JSON file schema: dictionary
            parent_key: list
    Output: keys_path: list
    """
    # Extracts keys from a schema.
    keys_list = []
    keys_path = []
    avoidFields = ['value', 'unit', 'min_value', 'max_value', 'average_value']
    for key, value in myDict.items():
        new_key = [key] if parent_key is None else parent_key + [key]
        if isinstance(value, dict):
            for el in extract_keys_from_myDict(value, new_key):
                if el[-1] not in avoidFields:
                    keys_list.append(el)
                else:
                    keys_list.append(el[:-1])
                    break
        elif isinstance(value, list):
            for i, item in enumerate(value):
                if isinstance(item, dict):
                    for el in extract_keys_from_myDict(item, new_key):
                        if el[-1] not in avoidFields:
                            keys_list.append(el)
                        else:
                            keys_list.append(el[:-1])
                            break
                else:
                    keys_list.append(new_key)
        else:
            keys_list.append(new_key)

        keys_tuple = [tuple(item) for item in keys_list]
        for item in [tuple(item) for item in keys_list]:
            if item not in keys_path:
                keys_path.append(item)
    return keys_path

def set_nested_value(d, key_path, value):
    """
    This function allows to create nested dictionary from the previous dictionary.
    Inputs: d: dict
            key_path: tuple
            value: obj
    Output: nested dictionary
    """
    # Creates nested dictionaries from keys.
    for part in key_path[:-1]:
        if part not in d:
            d[part] = {}
        d = d[part]
    d[key_path[-1]] = value

def create_ape_he_document(myDict, mappingDict):
    """
    This function creates a JSON file which is a document based on a schema (Nexus).
    Inputs: myDict: JSON file schema: dictionary
            mappingDict: JSON file metadata: dictionary
    Outputs: myDict: document: Dictionary
    """
    # Creates the JSON document based on the schema.
    mappingDict_keys = extract_keys_from_myDict(mappingDict, parent_key=None)
    for key_path in mappingDict_keys:
        a_ref = mappingDict
        b_ref = myDict
        try:
            for key in key_path[:-1]:
                a_ref = a_ref[key]
                if key not in b_ref:
                    b_ref[key] = {}
                b_ref = b_ref[key]
            last_key = key_path[-1]
            if last_key in a_ref:
                if b_ref[last_key] == "" or b_ref[last_key] == -9999:
                    b_ref[last_key] = a_ref[last_key]
                elif isinstance(b_ref[last_key], dict) and 'value' in list(b_ref[last_key].keys()) and isinstance(a_ref[last_key], float):
                    b_ref[last_key]['value'] = a_ref[last_key]
                elif isinstance(b_ref[last_key], dict) and 'value' in list(b_ref[last_key].keys()) and isinstance(a_ref[last_key], str):
                    b_ref[last_key]['value'] = float(a_ref[last_key])
                elif isinstance(b_ref[last_key], dict) and 'value' in list(b_ref[last_key].keys()) and isinstance(a_ref[last_key], np.ndarray):
                    b_ref[last_key]['value'] = a_ref[last_key][-1]
                elif isinstance(b_ref[last_key], dict) and 'min_value' in list(b_ref[last_key].keys()) and a_ref[last_key].size > 0:
                    b_ref[last_key]['min_value'] = np.min(a_ref[last_key])
                    b_ref[last_key]['max_value'] = np.max(a_ref[last_key])
                    b_ref[last_key]['average_value'] = (np.min(a_ref[last_key]) + np.max(a_ref[last_key])) / 2.
                elif last_key == 'gas_flux':
                    gas_flux_dict = {}
                    gas_flux_list = []
                    for el in a_ref[last_key]:
                        gas_flux_dict['value'] = el[0]
                        gas_flux_dict['unit'] = 'ml/min'
                        gas_flux_dict['gas_name'] = el[1]
                        gas_flux_list.append(gas_flux_dict)
                        gas_flux_dict = {}
                    b_ref[last_key] = gas_flux_list
        except KeyError:
            continue
    return myDict

def main():

    parser = argparse.ArgumentParser(
        description="Process a Nexus file and generate a JSON document based on a given schema.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    
    parser.add_argument("ape_he_schema", type=str, help="Path to the JSON schema file. This file defines the structure of the output document.")
    parser.add_argument("nexus_file", type=str,  help="Path to the Nexus (.nxs) file. This file contains metadata to be extracted.")
    parser.add_argument("document_name", type=str, help="Name of the output JSON file. This is the final document generated by the script.")
    
    args = parser.parse_args()

    # Load the schema
    with open(args.ape_he_schema, 'r') as f:
        ape_he_schema = json.load(f)
    
    # Initialize metadata
    all_metadata = {}
    with h5py.File(args.nexus_file, 'r') as f:
        try:
            all_metadata.update(extract_metadata(f))
        except Exception as e:
            print(f"Error reading Nexus file: {e}")

    keys_path_ape_he = extract_keys_from_myDict(ape_he_schema)
    metadata = {tuple(key.split('/')): value for key, value in all_metadata.items()}
    
    myDict_parsed = {}
    equivalencies = {('entry', 'sample', 'transformations', 'phi(x)'): ('entry', 'sample', 'transformations', 'phi'),
                     ('entry', 'sample', 'transformations', 'theta(z)'): ('entry', 'sample', 'transformations', 'theta')}
    transformed_gasFlux = {('entry', 'sample', 'gas_flux'): []}
    gasFlux_pattern = re.compile('gas_flux')

    for key, value in metadata.items():
        if len(gasFlux_pattern.findall(key[-1])) > 0:
            gas_name = key[-1].split('_')[-1]
            transformed_gasFlux[('entry', 'sample', 'gas_flux')].append((value, gas_name))
    metadata[('entry', 'sample', 'gas_flux')] = transformed_gasFlux[('entry', 'sample', 'gas_flux')]

    keys_metadata = list(metadata.keys())
    for key in keys_path_ape_he:
        matched_key = equivalencies.get(key, key)
        if matched_key in keys_metadata and matched_key in metadata:
            value = metadata[matched_key]
            set_nested_value(myDict_parsed, key, value)

    myDoku = create_ape_he_document(ape_he_schema, myDict_parsed)

    with open(args.document_name, 'w') as json_file:
        json.dump(myDoku, json_file, indent=4)

    print(f"{args.document_name} has been created!")

if __name__ == "__main__":
    main()
