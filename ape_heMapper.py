import json
from metadataProcessor import MetadataProcessor
from neXusReader import NeXusReader

class APE_HE_Mapper:
    def __init__(self, mySchema, metadata_dict):
        self.mySchema = mySchema
        self.metadata_dict = metadata_dict

        self.keys_path_schema = MetadataProcessor.extract_keys_from_myDict(self.mySchema)
        self.metadata = {tuple(key.split('/')): value for key, value in self.metadata_dict.items()}

        self.equivalencies = {
            ('entry', 'sample', 'transformations', 'phi(x)'): ('entry', 'sample', 'transformations', 'phi'),
            ('entry', 'sample', 'transformations', 'theta(z)'): ('entry', 'sample', 'transformations', 'theta')
        }
    
    def output_the_document(self):
            
            # Process metadata
            metadata = MetadataProcessor.process_gas_flux(self.metadata)

            myDict_parsed = MetadataProcessor.map_equivalencies(metadata, self.keys_path_schema, self.equivalencies)

            # Generate document
            myDoku = MetadataProcessor.create_metadata_document(self.mySchema, myDict_parsed)

            return myDoku