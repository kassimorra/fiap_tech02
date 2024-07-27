from datetime import datetime, timedelta
from sampleFile.sampleDF import SampleFile
import utils

last_day = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')
base_file = last_day + '-B3'

utils.download()
utils.csv2Parquet('files/', base_file, '.csv')
sampler = SampleFile()
sampler.sampler('files/' + base_file + '.parquet', 'upload/' + base_file + '_sample.parquet', 100)
utils.moveFile2B3(base_file + '_sample.parquet', "fiap-mlet52-raw-bucket", utils.s3_client())