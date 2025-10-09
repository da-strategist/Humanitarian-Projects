import numpy as np
import pandas as pd
from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset

Configuration.create(hdx_site= 'prod', user_agent = 'A_Quick_Example', hdx_read_only = True)
dataset = Dataset.search_in_hdx('State of Palestine')
print(dataset)