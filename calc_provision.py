import argparse
import math
parser=argparse.ArgumentParser()
parser.add_argument("--target",action='store',type=int,default=None)
parser.add_argument("--wcu",action='store',type=int,default=None)
parser.add_argument("--rcu",action='store',type=int,default=None)
parser.add_argument("--insert_proportion",action='store',type=float,default=0)
parser.add_argument("--read_proportion",action='store',type=float,default=0)
parser.add_argument("--scan_proportion",action='store',type=float,default=0)
parser.add_argument("--update_proportion",action='store',type=float,default=0)
parser.add_argument("--op",action='store',type=str,choices = [ "load","run" ], default="run" ,required=True)
parser.add_argument("--fieldlength",action='store',type=int,default=None)
parser.add_argument("--fieldcount",action='store',type=int,default=None)
parser.add_argument("--field_prefix", action='store',type=str,default="field")
parser.add_argument("--key_name", action='store',type=str,default="p")
parser.add_argument("--max_rcu",action='store',type= int,default = 200000)
parser.add_argument("--max_wcu",action='store',type= int,default = 200000)

#Item Size Calculation according to: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/CapacityUnitCalculations.html

# The total size of an item is the sum of the lengths of its attribute names and values, 
# plus any applicable overhead as described below. You can use the following guidelines to estimate attribute sizes:
#
# * Strings are Unicode with UTF-8 binary encoding. The size of a string is (length of attribute name) + (number of UTF-8-encoded bytes).
# * Numbers are variable length, with up to 38 significant digits. Leading and trailing zeroes are trimmed. The size of a number is approximately (length of attribute name) + (1 byte per two significant digits) + (1 byte).
# * A binary value must be encoded in base64 format before it can be sent to DynamoDB, but the value's raw byte length is used for calculating size. The size of a binary attribute is (length of attribute name) + (number of raw bytes).
# * The size of a null attribute or a Boolean attribute is (length of attribute name) + (1 byte).
# * An attribute of type List or Map requires 3 bytes of overhead, regardless of its contents. The size of a List or Map is (length of attribute name) + sum (size of nested elements) + (3 bytes) . The size of an empty List or Map is (length of attribute name) + (3 bytes).
# * Each List or Map element also requires 1 byte of overhead.

KB = 1024
#WCU per KB item per second
WCU_STANDARD= 1*KB
RCU_STRONGLY_CONSISTENT=4*KB
RCU_EVENTUALLY_CONSISTENT=8*KB

params = parser.parse_args()


item_size_bytes=0

#calculate the total attribute names size
for i in range(params.fieldcount):
    attr_name=params.field_prefix + str(i)
    item_size_bytes += len(attr_name)
item_size_bytes += len(params.key_name)

# add the fields data size
item_size_bytes += params.fieldlength * params.fieldcount

#since we can't know the actual length of the key - we will go for the maximal length
JAVA_LONG_MAX= 2**64 - 1
item_size_bytes += len(str(JAVA_LONG_MAX))
    
if params.op == "load":
    params.insert_proportion=1
    params.read_proportion=0
    params.update_proportion=0
    params.scan_proportion=0

def normalize_proportions(params):
    proportion_sum = params.insert_proportion + params.read_proportion + params.update_proportion + params.scan_proportion
    if proportion_sum == 0:
        raise Exception("At least one proportion should be set")
    params.insert_proportion = params.insert_proportion / proportion_sum
    params.read_proportion = params.read_proportion / proportion_sum
    params.update_proportion = params.update_proportion / proportion_sum
    params.scan_proportion = params.scan_proportion / proportion_sum


normalize_proportions(params)

single_item_units_for_write = math.ceil(item_size_bytes / WCU_STANDARD)
single_item_units_for_read = math.ceil(item_size_bytes / RCU_STRONGLY_CONSISTENT)
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
        params.wcu = min(calculated_wcu,params.max_wcu)
    if not params.rcu:
        params.rcu = min(calculated_rcu,params.max_wcu)
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
        params.wcu = math.floor(single_item_units_for_write * writes_per_second)
    elif params.rcu and params.wcu:
        params.target = min(target_from_rcu,target_from_wcu)
    else:
        raise Exception("At least one of 'wcu', 'rcu', 'target' must be dfined")
    print(f"before adjustment:rcu={params.rcu}; wcu={params.wcu} ; target={params.target}; item_size_bytes={item_size_bytes};")    
    if params.rcu > params.max_rcu:
        params.rcu = params.max_rcu
        params.target = math.floor((params.rcu / single_item_units_for_read) / read_proportion)
        params.wcu = (params.target * write_proportion) * single_item_units_for_write
    if params.wcu > params.max_wcu:
        params.wcu = params.max_wcu
        params.target = math.floor((params.wcu / single_item_units_for_write) / write_proportion)
        params.rcu = (params.target * read_proportion) * single_item_units_for_read
print(f"rcu={params.rcu}; wcu={params.wcu} ; target={params.target}; item_size_bytes={item_size_bytes};")


