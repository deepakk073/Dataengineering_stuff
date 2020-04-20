import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.options.pipeline_options import PipelineOptions
with beam.Pipeline(options=PipelineOptions()) as p:
    table_schema={'fields':[{'name':'key','type':'STRING','mode':'NULLABLE'},
                           {'name':'value','type':'INTEGER','mode':'NULLABLE'}
                           ]}
    
    table_spec = bigquery.TableReference(
    projectId='XXXXX',
    datasetId='XXXXx',
    tableId='word_cnt')

    def sum_val(tup):
        (key,val) = tup
        return {'key': key,'value': sum(val)}#'%s  - %d'%(key, sum(val))
    out= (
        p
        |"read fro txt " >>ReadFromText("F:\codebase\Dataengineering_stuff\Dataflow\dee.txt.txt")
        |beam.FlatMap(lambda x: x.split(' '))
        |beam.Map(lambda x: (x,1))
        |beam.GroupByKey()
        |beam.Map(sum_val)
        
        
       #|WriteToText("F:\codebase\Dataengineering_stuff\Dataflow\dee1.txt")
    )
    out|beam.io.WriteToBigQuery(
            table_spec,
            schema =table_schema,
            write_disposition = beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            #create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
        )