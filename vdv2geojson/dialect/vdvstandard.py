import logging
import os

from vdv2geojson.x10 import read_x10_file

def convert(converter_context, input_directory, output_directory):
    # load general data
    logging.info('loading REC_ORT.x10 ...')
    x10_REC_ORT = read_x10_file(os.path.join(input_directory, 'REC_ORT.x10'))

    logging.info('indexing point data ...')
    idx_point_data = dict()
    for record in x10_REC_ORT.records:
        identifier = (record['ONR_TYP_NR'], record['ORT_NR'])
        idx_point_data[identifier] = (
            record['ORT_REF_ORT_NAME'],
            _convert_coordinate_vdv(record['ORT_POS_LAENGE']),
            _convert_coordinate_vdv(record['ORT_POS_BREITE'])
        )

    x10_REC_ORT.close()
    
    # export shapes if configured
    if converter_context._config['data']['extract_shapes']:
        # generate network index ...
        logging.info('loading REC_SEL_ZP.x10 ...')
        x10_REC_SEL_ZP = read_x10_file(os.path.join(input_directory, 'REC_SEL_ZP.x10'))

        logging.info('indexing section intermediate points ...')
        idx_section_intermediate_data = dict()
        for record in x10_REC_SEL_ZP.records:
            identifier = (record['ONR_TYP_NR'], record['ORT_NR'], record['SEL_ZIEL_TYP'], record['SEL_ZIEL'])
            if not identifier in idx_section_intermediate_data.keys():
                idx_section_intermediate_data[identifier] = list()
                
            idx_section_intermediate_data[identifier].append((record['ZP_TYP'], record['ZP_ONR']))

        x10_REC_SEL_ZP.close()

        logging.info('loading REC_SEL.x10 ...')
        x10_REC_SEL = read_x10_file(os.path.join(input_directory, 'REC_SEL.x10'))

        logging.info('indexing sections ...')
        idx_section_data = dict()
        for record in x10_REC_SEL.records:
            identifier = (record['ONR_TYP_NR'], record['ORT_NR'], record['SEL_ZIEL_TYP'], record['SEL_ZIEL'])
            idx_section_data[identifier] = (
                record['SEL_LAENGE'],
            )

        x10_REC_SEL.close()

        logging.info('loading REC_LID.x10 ...')
        x10_REC_LID = read_x10_file(os.path.join(input_directory, 'REC_LID.x10'))

        logging.info('loading LID_VERLAUF.x10 ...')
        x10_LID_VERLAUF = read_x10_file(os.path.join(input_directory, 'LID_VERLAUF.x10'))

        # run over each line ...
        for rec_lid_record in x10_REC_LID.records:
            line_nr = rec_lid_record['LI_NR']
            line_name = rec_lid_record['LIDNAME']
            route_nr = rec_lid_record['ROUTEN_NR']
            route_name = rec_lid_record['STR_LI_VAR']

            route_coordinates = list()
            route_intermediate_stops_meta = list()

            logging.info(f"found (LineNr-LineVariantName) {line_nr}-{route_name} - converting now ...")

            lid_verlauf_items = x10_LID_VERLAUF.find_records(rec_lid_record, ['LI_NR', 'STR_LI_VAR'])
            
            # initialize 
            last_stop_point_identifier = (lid_verlauf_items[0]['ONR_TYP_NR'], lid_verlauf_items[0]['ORT_NR'])
            last_stop_point = idx_point_data[last_stop_point_identifier]

            for lid_verlauf_item in lid_verlauf_items[1:]:
                stop_point_identifier = (lid_verlauf_item['ONR_TYP_NR'], lid_verlauf_item['ORT_NR'])
                stop_point = idx_point_data[stop_point_identifier]

                # select route section point
                section = idx_section_data[last_stop_point_identifier + stop_point_identifier]
                section_intermediate_points = idx_section_intermediate_data[last_stop_point_identifier + stop_point_identifier]
                
                # select route section intermediate points
                """section_intermediate_point_coordinates = list()
                if section_identifier in _section_intermediate_cache.keys():
                    section_intermediate_point_coordinates = _section_intermediate_cache[section_identifier]
                else:
                    section_intermediate_point_items = x10_REC_SEL_ZP.find_records(section_selector, ['ORT_NR', 'SEL_ZIEL'])
                    for intermediate_point_item in section_intermediate_point_items:
                        intermediate_point_identifier = (intermediate_point_item['ZP_ONR'], intermediate_point_item['ZP_TYP'])
                        if intermediate_point_identifier in _point_coordinate_cache.keys():
                            section_intermediate_point_coordinates.append(_point_coordinate_cache[intermediate_point_identifier])
                        else:
                            intermediate_selector = {'ORT_NR': intermediate_point_item['ZP_ONR'], 'ONR_TYP_NR': intermediate_point_item['ZP_TYP']}
                            intermediate_point = x10_REC_ORT.find_record(intermediate_selector, ['ORT_NR', 'ONR_TYP_NR'])

                            intermediate_point_coordinates = _convert_coordinates_vdv(
                                intermediate_point['ORT_POS_LAENGE'], 
                                intermediate_point['ORT_POS_BREITE']
                            )

                            section_intermediate_point_coordinates.append(intermediate_point_coordinates) 

                            _point_coordinate_cache[intermediate_point_identifier] = intermediate_point_coordinates

                        _section_intermediate_cache[section_identifier] = section_intermediate_point_coordinates"""
    
                section_intermediate_point_coordinates = list()
                for intermediate_point_reference in section_intermediate_points:
                    intermediate_point = idx_point_data[intermediate_point_reference]

                    section_intermediate_point_coordinates.append([
                        intermediate_point[1],
                        intermediate_point[2]
                    ])
                
                route_coordinates = route_coordinates + section_intermediate_point_coordinates

                # generate meta data
                route_intermediate_stops_meta.append((
                    lid_verlauf_item['ORT_NR'],
                    section[0]
                ))

                # set last_stop_point in order to process next section
                last_stop_point_identifier = stop_point_identifier
                last_stop_point = stop_point

            # add GeoJSON feature
            converter_context._add_linestring_feature(route_coordinates, {
                'line_nr': line_nr,
                'line_name': line_name,
                'route_nr': route_nr,
                'route_name': route_name,
                'intermediate_stops': route_intermediate_stops_meta
            })

        # write GeoJOSN file finally
        converter_context._write_linestring_geojson_file(os.path.join(output_directory, 'Lines.geojson'))

def _convert_coordinate_vdv(input):
    input_string = str(input)
    degree_angle_coordinate = input_string.replace('-', '').rjust(10, '0')

    degrees = float(degree_angle_coordinate[0:3])
    minutes = float(degree_angle_coordinate[3:5])
    seconds = float(degree_angle_coordinate[5:]) / 1000.0

    degrees = abs(degrees)

    return degrees + (minutes / 60.0) + (seconds / 3600.0) * (-1.0 if input_string.startswith('-') else 1.0)
