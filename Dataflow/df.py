import apache_beam as beam
import argparse
from apache_beam.io import ReadFromText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        dest = 'input',
        default='gs://dataflow-samples/shakespeare/kinglear.txt',
        help = 'Input file to process.'
    )
    parser.add_argument(
        '--output',
        dest='output',
        required=True,
        help='Output file to write results to.'
    )
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session
    p = beam.Pipeline(options=pipeline_options)
    lines = p| 'read' >> ReadFromText(known_Args.input)