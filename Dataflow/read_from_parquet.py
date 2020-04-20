from __future__ import absolute_import

import platform
import sys
from functools import partial

from apache_beam.io import filebasedsink
from apache_beam.io import filebasedsource
from apache_beam.io.filesystem import CompressionTypes
from apache_beam.io.iobase import RangeTracker
from apache_beam.io.iobase import Read
from apache_beam.io.iobase import Write
from apache_beam.transforms import PTransform

if not (platform.system() == 'Windows' and sys.version_info[0] == 2):
  import pyarrow as pa
  import pyarrow.parquet as pq

__all__ = ['ReadFromParquet', 'ReadAllFromParquet', 'WriteToParquet']

class ReadFromParquet(PTransform):
    def __init__(self,file_pattern=None, min_bundle_size=0, validate = True, columns=None):
        super(ReadFromParquet, self).__init__()
        self._source = _create_parquet_source(
            file_pattern,
            min_bundle_size,
            validate = validate,
            columns = columns
        )
        def expand(self, pvalue):
            return pvalue.pipeline | Read(self._source)

        def display_data(self):
            return {'source_aa': self._source}

class ReadAllFromParquet(PTransform):
    DEFAULT_DESIRED_BUNDLE_SIZE = 64*1024*1024
    def __init__(self, min_bundle_size=0, desired_bundle_size = DEFAULT_DESIRED_BUNDLE_SIZE, columns = None, label='ReadAllFiles'):
        super(ReadAllFormParquet, self).__init__()
        source_from_file = partial(
            _create_parquet_source,
            min_bundle_size=min_bundle_size,
            columns=columns
        )
        self._read_all_files = filebasedsource.ReadAllFiles(
            True, CompressionTypes.UNCOMPRESSED, desired_bundle_size,
            min_bundle_size, source_from_file
        )
        self.label = label
        def expand(self, pvalue):
            return pvalue | self.label >> self._read_all_files
    def _create_parquet_source(file_pattern=None, min_bundle_size=0, validate=False, columns=None):
        return _parquetSource(file_pattern=file_pattern, min_bundle_size=min_bundle_size,validate=validate,columns=columns)
class _ParquetUtils(object):
  @staticmethod
  def find_first_row_group_index(pf, start_offset):
    for i in range(_ParquetUtils.get_number_of_row_groups(pf)):
      row_group_start_offset = _ParquetUtils.get_offset(pf, i)
      if row_group_start_offset >= start_offset:
        return i
    return -1

  @staticmethod
  def get_offset(pf, row_group_index):
    first_column_metadata =\
      pf.metadata.row_group(row_group_index).column(0)
    if first_column_metadata.has_dictionary_page:
      return first_column_metadata.dictionary_page_offset
    else:
      return first_column_metadata.data_page_offset

  @staticmethod
  def get_number_of_row_groups(pf):
    return pf.metadata.num_row_groups
