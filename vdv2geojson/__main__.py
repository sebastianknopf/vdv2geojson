import click
import logging

from vdv2geojson.converter import VdvGeoJsonConverter

logging.basicConfig(
    level=logging.INFO, 
    format= '[%(asctime)s] %(levelname)s: %(message)s',
    datefmt='%H:%M:%S'
)

@click.command
@click.option('--input', default='./input', help='input directory or ZIP file')
@click.option('--output', default='./output', help='output directory or ZIP file')
@click.option('--lines', default=None, help='comma-separated line IDs to be processed; if None, all lines are processed')
@click.option('--config', default=None, help='additional config file')
def main(input, output, lines, config):
    if not lines is None:
        line_filter = lines.split(',')
        line_filter = [int(x.strip()) for x in line_filter]
    else:
        line_filter = []
    
    converter = VdvGeoJsonConverter(config)
    converter.convert(input, output, line_filter)

if __name__ == '__main__':
    main()