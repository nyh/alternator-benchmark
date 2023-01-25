import argparse
import struct
import os
import gzip
parser = argparse.ArgumentParser()


parser.add_argument("input_file",type=str)
parser.add_argument("output_dir", type=str)
parser.add_argument("unique_name", type=str)
params = parser.parse_args()
packer = struct.Struct('qq')

params.input_file = os.path.realpath(params.input_file)
params.output_dir = os.path.realpath(params.output_dir)
FILE_SCHEME='{measurement}-' + "{}.bin".format(params.unique_name)
LIMITS_FILE_SCHEME="{}.bin.limits".format(params.unique_name)
limits_file = os.path.join(params.output_dir,LIMITS_FILE_SCHEME)
if not os.path.exists(params.input_file):
    raise Exception("input file: {} doesn't exist".format(params.input_file))
if not os.path.exists(params.output_dir):
    raise Exception("output dir: {} doesn't exist".format(params.output_dir) )
if not os.path.isdir(params.output_dir):
    raise Exception("{} is not a directory".format(params.output_dir))

output_files = {}
current_output_file = None
min_time = 2**64 -1
max_time = 0
min_latency = 2**64 - 1
max_latency = 0
BUFF_COUNT = 1000
ELEMENT_SIZE = packer.size
BUFF_SIZE = BUFF_COUNT * ELEMENT_SIZE
buff = bytearray(BUFF_SIZE)
buff_offset = 0
try:
    with open(params.input_file) as input_file, gzip.open(os.path.join(params.output_dir,params.unique_name + ".raw.gz"),'wt',compresslevel=1) as compressed_file:
        for line in input_file:
            compressed_file.write(line)
            fields =list(map(lambda x: x.strip() ,line.strip().split(',')))
            if len(fields) !=3:
                print("invalid length of fields for |{}|".format(fields))
                continue
            if not fields[1][1].isdigit():
                if buff_offset > 0 and current_output_file:
                    current_output_file.write(buff[:buff_offset])
                    buff_offset = 0
                op=fields[0].split(' ')[0].replace('-','_')
                if op not in output_files:
                    meas_file = os.path.join(params.output_dir,FILE_SCHEME.format(measurement = op))
                    output_files[op] = gzip.open(meas_file,'wb',compresslevel=1)
                current_output_file = output_files[op]
            else:
                timestamp = int(fields[1])
                latency = int(fields[2])
                packer.pack_into(buff, buff_offset, timestamp,latency)
                buff_offset += packer.size
                if buff_offset >= BUFF_SIZE:
                    current_output_file.write(buff)
                    buff_offset = 0
                min_time = min(min_time, timestamp)
                max_time = max(max_time, timestamp)
                min_latency = min(min_latency, latency)
                max_latency = max(max_latency, latency)
    if buff_offset > 0 and current_output_file:
            current_output_file.write(buff[:buff_offset])
            buff_offset = 0
    with open(limits_file,'wb') as f:
        f.write(packer.pack(min_time,max_time))
        f.write(packer.pack(min_latency,max_latency))
        
finally:            
    for f in output_files.values():
        f.close()