import hdrh.dump
import hdrh.histogram
from hdrh.histogram import HdrHistogram
import base64
import os
import argparse
import re
parser = argparse.ArgumentParser()
parser.add_argument("logs_dir")
params=parser.parse_args()

hdr_category= re.compile(r"loader-[\d]+-(?P<category>.*)\.hdr")
categories = {}
highest = {}
lowest = {}
for f in os.listdir(params.logs_dir):
    fpath=os.path.join(params.logs_dir,f)
    if not os.path.isfile(fpath):
        continue
    if m:=hdr_category.fullmatch(f) :
        print("processing " + fpath)
        category=m.groupdict()['category']
        with open(fpath,'r') as f:
            # It is the last field of the last line in the file
            hist_buff=f.readlines()[-1].split(',')[-1]
        hist=HdrHistogram.decode(hist_buff)
        with open(str(fpath)+".txt", 'wb') as f:
            hist.output_percentile_distribution(f,1000)
        with open(str(fpath)+".csv", 'wb') as f:
            hist.output_percentile_distribution(f,1000, use_csv = True)
        if category not in categories:
            categories[category] = [hist]
            highest[category] = hist.highest_trackable_value
            lowest[category] = hist.lowest_trackable_value
        else:
            categories[category].append(hist)
            if highest[category] < hist.highest_trackable_value:
                highest[category] = hist.highest_trackable_value
            if lowest[category] > hist.lowest_trackable_value:
                lowest[category] = hist.lowest_trackable_value


print("Writing accumulated histogram...")

for category,hist_list in categories.items():
    accumulated_hist = HdrHistogram(lowest[category], highest[category], hist_list[0].significant_figures, hist_list[0].word_size)
    for hist in hist_list:
        accumulated_hist.add(hist)
    with open(params.logs_dir + "/accumulated-" + category + '.hdr.txt','wb') as f:
        accumulated_hist.output_percentile_distribution(f,1000)
    with open(params.logs_dir + "/accumulated-" + category + '.hdr.csv','wb') as f:
        accumulated_hist.output_percentile_distribution(f,1000, use_csv = True)
# l=""


# with open('/home/eliransin/SourceCode/alternator-benchmark/latest/loaders/loader-0-HdrINSERT.hdr','r') as f:
#      l = f.readlines()[-1]
# pathlib.Path()
# l=l.split(',')[-1]
# l =l.strip()

 #hdrh.dump.dump([l])

