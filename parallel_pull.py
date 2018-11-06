from time import sleep
from datetime import datetime, time, timedelta
import pytz
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import os
import dataPull as dp


ibPull = dp.DataPull(3, 0, 'GC', 1, '127.0.0.1')
start = pd.to_datetime('2018-11-01 12:30')
print(ibPull.pull_1000(start, pytz.timezone('America/New_York')))
print(ibPull.print_data())
print(ibPull.disconnect())