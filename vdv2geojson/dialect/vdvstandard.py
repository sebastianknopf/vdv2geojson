import logging
import os

from vdv2geojson.x10 import read_x10_file

def convert(converter_context, input_directory, output_directory, line_filter):
    # load general data
    logging.info('loading REC_ORT.x10 ...')
    x10_REC_ORT = read_x10_file(
        os.path.join(input_directory, 'REC_ORT.x10'), 
        converter_context._config['config']['x10']['null_value'], 
        converter_context._config['config']['x10']['encoding']
    )

    logging.info('indexing point data ...')
    idx_point_data = dict()
    for record in x10_REC_ORT.records:
        identifier = (record['ONR_TYP_NR'], record['ORT_NR'])
        idx_point_data[identifier] = (
            record['ORT_REF_ORT_NAME'],
            record['HST_NR_INTERNATIONAL'],
            _convert_coordinate_vdv(record['ORT_POS_LAENGE']),
            _convert_coordinate_vdv(record['ORT_POS_BREITE'])
        )

    x10_REC_ORT.close()
    
    # export shapes if configured
    if converter_context._config['data']['extract_shapes']:
        # generate network index ...
        logging.info('loading REC_SEL_ZP.x10 ...')
        x10_REC_SEL_ZP = read_x10_file(
            os.path.join(input_directory, 'REC_SEL_ZP.x10'),
            converter_context._config['config']['x10']['null_value'], 
            converter_context._config['config']['x10']['encoding']
        )

        logging.info('indexing section intermediate points ...')
        idx_section_intermediate_data = dict()
        for record in x10_REC_SEL_ZP.records:
            identifier = (record['ONR_TYP_NR'], record['ORT_NR'], record['SEL_ZIEL_TYP'], record['SEL_ZIEL'])
            if not identifier in idx_section_intermediate_data.keys():
                idx_section_intermediate_data[identifier] = list()
                
            idx_section_intermediate_data[identifier].append((record['ZP_TYP'], record['ZP_ONR']))

        x10_REC_SEL_ZP.close()

        logging.info('loading REC_SEL.x10 ...')
        x10_REC_SEL = read_x10_file(
            os.path.join(input_directory, 'REC_SEL.x10'),
            converter_context._config['config']['x10']['null_value'], 
            converter_context._config['config']['x10']['encoding']
        )

        logging.info('indexing sections ...')
        idx_section_data = dict()
        for record in x10_REC_SEL.records:
            identifier = (record['ONR_TYP_NR'], record['ORT_NR'], record['SEL_ZIEL_TYP'], record['SEL_ZIEL'])
            idx_section_data[identifier] = (
                record['SEL_LAENGE'],
            )

        x10_REC_SEL.close()

        logging.info('loading REC_LID.x10 ...')
        x10_REC_LID = read_x10_file(
            os.path.join(input_directory, 'REC_LID.x10'),
            converter_context._config['config']['x10']['null_value'], 
            converter_context._config['config']['x10']['encoding']
        )

        logging.info('loading LID_VERLAUF.x10 ...')
        x10_LID_VERLAUF = read_x10_file(
            os.path.join(input_directory, 'LID_VERLAUF.x10'),
            converter_context._config['config']['x10']['null_value'], 
            converter_context._config['config']['x10']['encoding']
        )

        # run over each line ...
        for rec_lid_record in x10_REC_LID.records:
            line_nr = rec_lid_record['LI_NR']
            line_name = rec_lid_record['LIDNAME']
            line_direction = rec_lid_record['LI_RI_NR']
            line_id = rec_lid_record['LinienID'] if 'LinienID' in rec_lid_record else ''
            route_nr = rec_lid_record['ROUTEN_NR']
            route_name = rec_lid_record['STR_LI_VAR']

            # check for active line filter
            if len(line_filter) > 0 and not line_nr in line_filter:
                continue

            route_coordinates = list()
            route_intermediate_stops_meta = list()

            logging.info(f"found (LineNr-LineDirection-LineVariantName) {line_nr}-{line_direction}-{route_name} - converting now ...")

            lid_verlauf_items = x10_LID_VERLAUF.find_records(rec_lid_record, ['LI_NR', 'STR_LI_VAR'])
            
            # initialize 
            last_stop_point_identifier = (lid_verlauf_items[0]['ONR_TYP_NR'], lid_verlauf_items[0]['ORT_NR'])
            last_stop_point = idx_point_data[last_stop_point_identifier]

            stop_dist_travelled = 0.0

            # add first stop to all required datasets
            intermediate_stop_id = lid_verlauf_items[0]['ORT_NR']
            if converter_context._config['config']['prefer_international_ids'] and not last_stop_point[1] == '':
                intermediate_stop_id = last_stop_point[1]

            route_coordinates.append([
                last_stop_point[2],
                last_stop_point[3]
            ])

            route_intermediate_stops_meta.append({
                'stop_id': intermediate_stop_id,
                'shape_dist_traveled': stop_dist_travelled
            })

            # run over remaining items
            for lid_verlauf_item in lid_verlauf_items[1:]:
                stop_point_identifier = (lid_verlauf_item['ONR_TYP_NR'], lid_verlauf_item['ORT_NR'])
                stop_point = idx_point_data[stop_point_identifier]

                # select route section point
                section = idx_section_data[last_stop_point_identifier + stop_point_identifier]

                if (last_stop_point_identifier + stop_point_identifier) in idx_section_intermediate_data.keys():
                    section_intermediate_points = idx_section_intermediate_data[last_stop_point_identifier + stop_point_identifier]
                else:
                    section_intermediate_points = list()

                # increase distance
                stop_dist_travelled = stop_dist_travelled + section[0]

                # if there were some intermediate points find
                section_intermediate_point_coordinates = list()
                if len(section_intermediate_points) > 0:
                    # select route section intermediate points
                    for intermediate_point_reference in section_intermediate_points:
                        intermediate_point = idx_point_data[intermediate_point_reference]

                        section_intermediate_point_coordinates.append([
                            intermediate_point[2],
                            intermediate_point[3]
                        ])
                else:
                    # if there was no intermediate point added, add the current stop point instead
                    section_intermediate_point_coordinates.append([
                        stop_point[2],
                        stop_point[3]
                    ])

                route_coordinates = route_coordinates + section_intermediate_point_coordinates

                # generate meta data
                intermediate_stop_id = lid_verlauf_item['ORT_NR']
                if converter_context._config['config']['prefer_international_ids'] and not stop_point[1] == '':
                    intermediate_stop_id = stop_point[1]

                route_intermediate_stops_meta.append({
                    'stop_id': intermediate_stop_id,
                    'shape_dist_traveled': (stop_dist_travelled / 1000.0)
                })

                # set last_stop_point in order to process next section
                last_stop_point_identifier = stop_point_identifier
                last_stop_point = stop_point

            # add GeoJSON feature
            meta_data = dict({
                'line_nr': line_nr,
                'line_name': line_name,
                'line_id': line_id,
                'line_direction': line_direction,
                'route_nr': route_nr,
                'route_name': route_name,
            })

            if converter_context._config['data']['extract_shapes_intermediate_stops']:
                meta_data['intermediate_stops'] = route_intermediate_stops_meta

            converter_context._add_linestring_feature(route_coordinates, meta_data)

            # write GeoJOSN file finally
            geojson_filename = f"{line_nr}-{line_direction}-{route_name}.geojson"
            converter_context._write_linestring_geojson_file(os.path.join(output_directory, geojson_filename))

def _convert_coordinate_vdv(input):
    input_string = str(input)
    degree_angle_coordinate = input_string.replace('-', '').rjust(10, '0')

    degrees = float(degree_angle_coordinate[0:3])
    minutes = float(degree_angle_coordinate[3:5])
    seconds = float(degree_angle_coordinate[5:]) / 1000.0

    degrees = abs(degrees)

    return degrees + (minutes / 60.0) + (seconds / 3600.0) * (-1.0 if input_string.startswith('-') else 1.0)
