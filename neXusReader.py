import os
import h5py
import pandas as pd
import numpy as np
import zipfile
import logging
import shutil
from pathlib import Path

class NeXusReader:
    def __init__(self, file_path):
        self.file_path = file_path
        self.all_metadata = {}
        self.all_metadata_zip = {}
        self.temp_folder = os.path.splitext(self.file_path)[0]

    def get_file_contain(self):
        if zipfile.is_zipfile(self.file_path):
            self._process_zip_file()
            for nxs_file in Path(self.temp_folder).rglob("*.nxs"):
                if "__MACOSX" not in str(nxs_file):
                    self.file_path = nxs_file
                    file_name = os.path.basename(os.path.splitext(nxs_file)[0])
                    logging.info(f"Processing extracted file: {file_name}")
                    self.all_metadata_zip[file_name] = self._read_nxs_file()
                    
            logging.info(f"Cleaning up temporary folder: {self.temp_folder}")
            try:
                shutil.rmtree(self.temp_folder)
            except Exception as e:
                #logging.error(f"Error deleting temporary folder: {e}")
                logging.info(f"Error deleting temporary folder: {e}")
                
            return self.all_metadata_zip, "_zip"
            
        else:
            logging.info(f"Processing NeXus file: {self.file_path}")
            return self._read_nxs_file(), "_nxs"    
            
    def _read_nxs_file(self):
        try:
            with h5py.File(self.file_path, 'r') as f:
                self.all_metadata = self.extract_metadata(f)
        except Exception as e:
            raise ValueError(f"Error reading Nexus file: {e}")
            #logging.info(f"Error reading Nexus file: {e}")
        return self.all_metadata

    def _process_zip_file(self):
        try:
            os.makedirs(self.temp_folder, exist_ok = True)
            
            with zipfile.ZipFile(self.file_path, 'r') as zip_ref:
                zip_ref.extractall(self.temp_folder)
        except FileNotFoundError:
            #logging.error("Error: Zip file not found.")
            logging.info("Error: Zip file not found.")
        except Exception as e:
            logging.warning(f"Error processing the zip file: {e}")
            
        
    @staticmethod
    def extract_metadata(obj, group=''):
        """
        Recursive function to travel all over the nexus file tree and extract all the metadata as a dictionary.
        Inputs: obj: h5py (object)
               group: path to a directory (string)
        Output: metadata (dictionary)
        """
        metadata = {}
        if isinstance(obj, h5py.Group):
            for key in obj.keys():
                full_directory = f"{group}/{key.strip()}" if group else key
                metadata.update(NeXusReader.extract_metadata(obj[key], full_directory))
        elif isinstance(obj, h5py.Dataset):
            try:
                if isinstance(obj[()], np.ndarray):
                    metadata[group] = obj[()]
                else:
                    metadata[group] = obj[()].decode('utf-8')
            except Exception as e:
                logging.warning(f"Error decoding dataset {group}: {e}")
        return metadata