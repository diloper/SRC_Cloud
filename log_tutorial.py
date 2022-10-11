# -*- coding: utf-8 -*-
"""log_tutorial.ipynb"""

from datetime import date, timedelta, datetime
import logging
import os


# dir=os.path.abspath(os.getcwd())
# print(dir)
dir="log/"
if not os.path.exists(dir):
    os.makedirs(dir)
today = date.today().strftime("%Y-%m-%d")
logging.basicConfig(
    filename=dir+today+'.log', # write to this file
    filemode='a', # open in append mode
    format='%(name)s - %(levelname)s - %(message)s'
    )
FORMAT = '%(asctime)s %(levelname)s: %(message)s'
logging.debug("Debug logging test...")
try:
    x = 5 / 0
except:
    logging.error("Catch an exception.", exc_info=True)
