
import argparse
import json
import logging
from neXusReader import NeXusReader
from ape_heMapper import APE_HE_Mapper
from jsonOutputter import JsonOutputter

def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    parser = argparse.ArgumentParser(
        description="Process a NeXus file (.nxs) and generate a JSON document based on a given schema.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("ape_he_schema", type=str, help="Path to the JSON schema file.")
    parser.add_argument("nexus_file", type=str, help="Path to the NeXus (.nxs) file.")
    parser.add_argument("document_name", type=str, help="Name of the output JSON file.")
    args = parser.parse_args()

    try:
        # Validate files
        #validate_file_path(args.ape_he_schema, '.json')
        #validate_file_path(args.nexus_file, '.nxs')

        # Load the schema
        with open(args.ape_he_schema, 'r') as f:
            ape_he_schema = json.load(f)

        # Load the NeXus files
        nxs = NeXusReader(args.nexus_file)
        all_metadata, file_type = nxs.get_file_contain()

        # Process the metadata, create the document and save
        if file_type == "-nxs":
            mapper = APE_HE_Mapper(ape_he_schema, all_metadata)
            myDoku = mapper.output_the_document()
            JsonOutputter.save_the_file(myDoku, args.document_name)
        else:
            nxs_file_names = list(all_metadata.keys())
            file_path_list =  []
            for file_name in nxs_file_names:
                all_metadata_fn = all_metadata[file_name]
                mapper = APE_HE_Mapper(ape_he_schema, all_metadata_fn)
                myDoku = mapper.output_the_document()
                JsonOutputter.save_the_file(myDoku, file_name+".json")
                file_path_list.append(file_name+".json")

            JsonOutputter.save_to_zip(file_path_list, args.document_name)

    except Exception as e:
        logging.error(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
