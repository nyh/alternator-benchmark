import interval_hdrs
import argparse
import os
import pickle

parser = argparse.ArgumentParser()
parser.add_argument("input_dir", type=str)
parser.add_argument("--hdr_interval", type=int,default=10000,help="The interval in which to divide the measurements by")
parser.add_argument("--no_parallel", action='store_true',help="Whether to parallelize the load or not")

params = parser.parse_args()
params.input_dir = os.path.realpath(params.input_dir)
if not os.path.exists(params.input_dir):
    raise Exception(f'The directory {params.input_dir} does not exist.')
hdr_intervals = interval_hdrs.RawData(params.input_dir,params.hdr_interval,not params.no_parallel)
with open(params.input_dir +f"/accumulated_{params.hdr_interval}.pkl", 'wb') as f:
    pickle.dump(hdr_intervals,f)