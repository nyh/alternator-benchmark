import argparse
import math
parser=argparse.ArgumentParser()
parser.add_argument("--target",action='store',type=int,default=None)
parser.add_argument("--wcu",action='store',type=int,default=None)
parser.add_argument("--rcu",action='store',type=int,default=None)
parser.add_argument("--insert_proportion",action='store',type=float,default=None)
parser.add_argument("--read_proportion",action='store',type=float,default=None)
parser.add_argument("--scan_proportion",action='store',type=float,default=None)
parser.add_argument("--update_proportion",action='store',type=float,default=None)
parser.add_argument("--op",action='store',type=str,choices = [ "load","run" ], default="run" ,required=True)
parser.add_argument("--fieldlength",action='store',type=int,default=None)
parser.add_argument("--fieldcount",action='store',type=int,default=None)

KB = 1024
#WCU per KB item per second
WCU_STANDARD= 1*KB
RCU_STRONGLY_CONSISTENT=4*KB
RCU_EVENTUALLY_CONSISTENT=8*KB

params = parser.parse_args()


if params.op == "load":
    params.insert_proportion=1
    params.read_proportion=0
    params.update_proportion=0
    params.scan_proportion=0


def normalize_proportions(params):
    proportion_sum = params.insert_proportion + params.read_proportion + params.update_proportion + params.scan_proportion
    params.insert_proportion = params.insert_proportion / proportion_sum
    params.read_proportion = params.read_proportion / proportion_sum
    params.update_proportion = params.update_proportion / proportion_sum
    params.scan_proportion = params.scan_proportion / proportion_sum


normalize_proportions(params)
single_item_units_for_write = math.ceil((params.fieldlength * params.fieldcount) / WCU_STANDARD)
single_item_units_for_read = math.ceil((params.fieldlength * params.fieldcount) / RCU_EVENTUALLY_CONSISTENT)
write_proportion= params.insert_proportion + params.update_proportion
read_proportion = params.read_proportion
calculated_rcu = params.rcu
calculated_wcu = params.wcu
calculated_target = params.target
if params.target:
    writes_per_second = math.floor( write_proportion * params.target)
    #TODO: add support for scans
    reads_per_second = math.floor(read_proportion * params.target)
    calculated_wcu = writes_per_second * single_item_units_for_write
    calculated_rcu = reads_per_second * single_item_units_for_read
    if not params.wcu:
        params.wcu = calculated_wcu
    if not params.rcu:
        params.rcu = calculated_rcu
else:
    if params.wcu:
        potential_write_per_second = math.floor(params.wcu / single_item_units_for_write)
        target_from_wcu = math.floor(potential_write_per_second / write_proportion)
    if params.rcu:
        potential_reads_per_second = math.floor(params.rcu / single_item_units_for_read)
        target_from_rcu = math.floor(potential_reads_per_second / read_proportion)
    
    if params.wcu and not params.rcu:
        params.target = target_from_wcu
        reads_per_second = math.floor(read_proportion * params.target)
        params.rcu =  math.floor(single_item_units_for_read * reads_per_second)
    elif params.rcu and not params.wcu:
        params.target = target_from_rcu
        writes_per_second = math.floor(write_proportion * params.target )
        params.wcu = math.floor(single_item_units_for_write * potential_write_per_second)
    elif params.rcu and params.wcu:
        params.target = min(target_from_rcu,target_from_wcu)
    else:
        raise Exception("At least one of 'wcu', 'rcu', 'target' must be dfined")

print(f"rcu={params.rcu}; wcu={params.wcu} ; target={params.target}")


