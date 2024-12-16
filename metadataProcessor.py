import re
import numpy as np
import logging
from pathlib import Path

class MetadataProcessor:

    avoidFields = ['value', 'unit', 'min_value', 'max_value', 'average_value']

    @staticmethod
    def extract_keys_from_myDict(myDict, parent_key=None): 
        """
        Extracts the directory path from a schema that specifies where the metadata will be stored.
        Inputs: myDict: JSON file schema (dictionary)
                parent_key (list)
        Output: keys_path (list) ## [('entry', 'title'), ...]
        """
        keys_list = []
        keys_path = []

        for key, value in myDict.items():
            new_key = [key] if parent_key is None else parent_key + [key]
            if isinstance(value, dict):
                for el in MetadataProcessor.extract_keys_from_myDict(value, new_key):
                    if el[-1] not in MetadataProcessor.avoidFields:
                        keys_list.append(el)
                    else:
                        keys_list.append(el[:-1])
                        break
            elif isinstance(value, list):
                for i, item in enumerate(value):
                    if isinstance(item, dict):
                        for el in MetadataProcessor.extract_keys_from_myDict(item, new_key):
                            if el[-1] not in MetadataProcessor.avoidFields:
                                keys_list.append(el)
                            else:
                                keys_list.append(el[:-1])
                                break
                    else:
                        keys_list.append(new_key)
            else:
                keys_list.append(new_key)

            keys_tuple = [tuple(item) for item in keys_list]
            for item in keys_tuple:
                if item not in keys_path:
                    keys_path.append(item)
        return keys_path


    @staticmethod
    def set_nested_value(d, key_path, value):
        """
        Creates nested dictionary from the key_path and assign the corresponding value.
        Inputs: d (dictionary)
                key_path (tuple)
                value (object)
        Output: nested dictionary (dictionary)
        """
        for part in key_path[:-1]:
            if part not in d:
                d[part] = {}
            d = d[part]
        d[key_path[-1]] = value


    @staticmethod
    def process_gas_flux(metadata_dict):
        """
        Transforms gas flux metadata as required by the schema.
            Inputs: metadata_dict (dictionary) 
            Output: ### {('entry', 'sample', 'gas_flux'):[(value1, gas_name1), (value2, gas_name2), ...]}
        """
        transformed_gasFlux = {('entry', 'sample', 'gas_flux'): []}
        gasFlux_pattern = re.compile('gas_flux')

        for key, value in metadata_dict.items():
            if len(gasFlux_pattern.findall(key[-1])) > 0:
                gas_name = key[-1].split('_')[-1]
                transformed_gasFlux[('entry', 'sample', 'gas_flux')].append((value, gas_name))
        metadata_dict[('entry', 'sample', 'gas_flux')] = transformed_gasFlux[('entry', 'sample', 'gas_flux')]

        return metadata_dict


    @staticmethod
    def map_equivalencies(metadata_dict, keys_path_schema, equivalencies):
        """
        Maps required metadata keys to the corresponding schema keys based on equivalencies.
            Inputs: metadata_dict (dictionary)
                    keys_path_schema (list)
                    equivalencies (dictionary)
            Output: mapped_metadata_dict (nested dictionary)
        """
        mapped_metadata_dict = {}
        keys_metadata = list(metadata_dict.keys())

        for key in keys_path_schema:
            matched_key = equivalencies.get(key, key)
            if matched_key in keys_metadata and matched_key in metadata_dict:
                value = metadata_dict[matched_key]
                MetadataProcessor.set_nested_value(mapped_metadata_dict, key, value)

        return mapped_metadata_dict

    @staticmethod
    def create_metadata_document(mySchema_dict, mapped_metadata_dict):
        """
        Populates a schema dictionary with values from a metadata dictionary.
            Inputs: mySchema_dict: JSON file schema (dictionary)
                    mapped_metadata_dict: JSON file metadata (dictionary)
            Outputs: metadata document (dictionary)
        """
        mappedDict_keys = MetadataProcessor.extract_keys_from_myDict(mapped_metadata_dict)

        for key_path in mappedDict_keys:
            meta_ref = mapped_metadata_dict
            sche_ref = mySchema_dict
            try:
                for key in key_path[:-1]:
                    meta_ref = meta_ref[key]
                    if key not in sche_ref:
                        sche_ref[key] = {}
                    sche_ref = sche_ref[key]

                last_key = key_path[-1]
                if last_key in meta_ref:
                    MetadataProcessor._process_schema_values(sche_ref, meta_ref, key_path, last_key)
            except KeyError as e:
                logging.warning(f"KeyError while processing {key_path}: {e}")
            except Exception as e:
                logging.error(f"Unexpected error while processing {key_path}: {e}")

        return mySchema_dict
        

    @staticmethod
    def _process_schema_values(sche_ref, meta_ref, key_path, last_key):
        try:
            if last_key in meta_ref:
                if sche_ref[last_key] in ["", -9999]:
                    sche_ref[last_key] = meta_ref[last_key]
                elif isinstance(sche_ref[last_key], dict) and 'value' in sche_ref[last_key]:
                    if isinstance(meta_ref[last_key], float):
                        sche_ref[last_key]['value'] = meta_ref[last_key]
                    elif isinstance(meta_ref[last_key], str):
                        sche_ref[last_key]['value'] = float(meta_ref[last_key])
                    elif isinstance(meta_ref[last_key], np.ndarray):
                        sche_ref[last_key]['value'] = meta_ref[last_key][-1]
                    else:
                        logging.warning(f"Unsupported type for 'value' key in {key_path}: {type(meta_ref[last_key])}")
                elif isinstance(sche_ref[last_key], dict) and 'min_value' in sche_ref[last_key] and meta_ref[last_key].size > 0:
                    try:
                        MetadataProcessor._apply_arithmetic(sche_ref, meta_ref, last_key)
                    except Exception as e:
                        logging.warning(f"Error computing min or max for {key_path}: {e}")
                elif last_key == 'gas_flux':
                    try:
                        gas_flux_list = [{'value': el[0], 'unit': 'ml/min', 'gas_name': el[1]} for el in meta_ref[last_key]]
                        sche_ref[last_key] = gas_flux_list
                    except Exception as e:
                        logging.warning(f"Error processing gas_flux for {key_path}: {e}")
            else:
                pass
        except Exception as e:
            logging.warning(f"Error while processing {key_path}: {e}")

    @staticmethod
    def _apply_arithmetic(sche_ref, meta_ref, last_key):
        minValue = np.nanmin(meta_ref[last_key])
        maxValue = np.nanmax(meta_ref[last_key])
        avgValue = (np.nanmin(meta_ref[last_key]) + np.nanmax(meta_ref[last_key])) / 2.
    
        arithmetic = [round(el, 3) if not np.isnan(el) else el for el in [minValue, maxValue, avgValue]]

        sche_ref[last_key] = {'min_value': arithmetic[0], 'max_value': arithmetic[1], 'average_value': arithmetic[2]}

    @staticmethod
    def validate_file_path(file_path, expected_extension):
        """
        Validates the file path and extension.
        """
        if not Path(file_path).is_file():
            raise FileNotFoundError(f"The file '{file_path}' does not exist.")
        if not file_path.endswith(expected_extension):
            raise ValueError(f"The file '{file_path}' does not have the expected '{expected_extension}' extension.")
