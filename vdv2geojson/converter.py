import json
import logging
import os
import rdp
import yaml
import zipfile

class VdvGeoJsonConverter:

    def __init__(self, config_filename=None, dialect='vdvstandard'):
        self._dialect = dialect

        if config_filename is not None:
            with open(config_filename, 'r') as config_file:
                self._config = yaml.safe_load(config_file)
        else:
            self._config = dict()

            self._config['config'] = dict()
            self._config['config']['prefer_international_ids'] = True
            self._config['config']['flatten_shapes'] = True
            self._config['config']['flatten_shapes_epsilon'] = 0.000005

            self._config['data'] = dict()
            self._config['data']['extract_shapes'] = True

        self._geojson_linestring_features = list()
        self._geojson_files = list()

    def convert(self, input, output, line_filter):
        
        if input.endswith('.zip'):
            input_directory = os.path.dirname(input)
        else:
            input_directory = input

        if output.endswith('.zip'):
            output_directory = os.path.dirname(output)
        else:
            output_directory = output
        
        if input.endswith('.zip'):
            logging.info(f"unpacking ZIP archive {input} ...")

            with zipfile.ZipFile(input, 'r') as zip_file:
                zip_file.extractall(input_directory)

        if self._dialect == 'vdvstandard':
            from vdv2geojson.dialect import vdvstandard
            vdvstandard.convert(self, input_directory, output_directory, line_filter)
        else:
            logging.error(f"unknown dialect {self._dialect}")

        if output.endswith('.zip'):
            logging.info(f"creating ZIP archive {output} ...")

            with zipfile.ZipFile(output, 'w') as zip_file:
                for json_file in self._geojson_files:
                    zip_file.write(
                        json_file,
                        os.path.basename(json_file),
                        compress_type=zipfile.ZIP_DEFLATED
                    )

                    os.remove(json_file)

        if input.lower().endswith('.zip'):
            for file in os.listdir(input_directory):
                if file.lower().endswith('.x10') or file.lower().endswith('.geojson'):
                    os.remove(os.path.join(input_directory, file))

    def _add_linestring_feature(self, coordinates, properties):
        if self._config['config']['flatten_shapes']:
            num_coordinates = len(coordinates)
            logging.info(f"compressing shape of {num_coordinates} points ...")
            coordinates = rdp.rdp(coordinates, self._config['config']['flatten_shapes_epsilon'])
            logging.info(f"compressed shape from {num_coordinates} to {len(coordinates)} points")
        
        self._geojson_linestring_features.append({
            'type': 'Feature',
            'geometry': {
                'type': 'LineString',
                'coordinates': coordinates
            },
            'properties': properties
        })

    def _write_linestring_geojson_file(self, geojson_filename):
        self._write_geojson_file(geojson_filename, {
            'type': 'FeatureCollection',
            'features': self._geojson_linestring_features
        })

        self._geojson_linestring_features = list()
    
    def _write_geojson_file(self, geojson_filename, geojson_data):
        self._geojson_files.append(geojson_filename)
        
        with open(geojson_filename, 'w', encoding='utf-8') as geojson_file:
            json.dump(geojson_data, geojson_file, indent=4)
            