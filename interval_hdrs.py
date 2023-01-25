from hdrh.histogram import HdrHistogram
import re
import os
import math
import multiprocessing
from tqdm.autonotebook import trange, tqdm
import subprocess
from time import sleep
import copyreg
import argparse
import pickle
import numpy as np
import enum
from pathlib import Path
import gzip
import struct
from functools import partial
packer = struct.Struct("qq")
BUFFER_COUNT = 1000
ELEMENT_SIZE = packer.size
BUFFER_SIZE = BUFFER_COUNT * ELEMENT_SIZE
buff_offset = 0

@enum.unique
class Measurement(enum.Enum):
    READ = 0
    READ_FAILED = 1
    INSERT = 2
    INSERT_FAILED = 3
    UPDATE = 4
    UPDATE_FAILED = 5
    DELETE = 6
    DELETE_FAILED = 7
    SCAN = 8
    SCAN_FAILED = 9
    CLEANUP = 10
    CLEANUP_FAILED = 11
    @classmethod
    def from_string(cls,name):
        return cls[name.replace("-","_")]
    @property
    def failed(self):
        return self.value % 2 == 1
    @property
    def succeeded(self):
        return not self.failed
    @property
    def negated(self):
        if self.value % 2 ==0:
            return Measurement(self.value+1)
        else:
            return Measurement(self.value-1)
    @property
    def successful_equivalent(self):
        if self.succeeded:
            return self
        else:
            return self.negated
    @property
    def failed_equivalet(self):
        return self.successful_equivalent.negated
    @classmethod
    def all_succeeded(cls):
        return set(filter(lambda x: x.succeeded, cls ))
    @classmethod
    def all_failed(cls):
        return set(filter(lambda x: x.failed, cls ))
    @classmethod
    def all_except(cls, measurements = None, filter_equivalents = False):
        if not measurements:
            return set(cls)
        if not is_iterable(measurements):
            measurements = [measurements]
        final_filter = set(measurements)
        if filter_equivalents:
            final_filter = final_filter.union({lambda m: m.negated for m in measurements})
        return set(cls).difference(final_filter)
        
        

class HdrHistogramPickler():
    @staticmethod    
    def make_histogram(lowest_trackable_value, highest_trackable_value, significant_figures,start_time_stamp, end_time_stamp, encoded_data):
        hdr = HdrHistogram(lowest_trackable_value, highest_trackable_value, significant_figures)
        hdr.decode_and_add(encoded_data)
        hdr.set_start_time_stamp(start_time_stamp)
        hdr.set_end_time_stamp(end_time_stamp)
        return hdr
    @staticmethod
    def hdr_histogram_serializer(hist : HdrHistogram):
        return HdrHistogramPickler.make_histogram, (hist.lowest_trackable_value, hist.highest_trackable_value, 
                                hist.significant_figures, hist.get_start_time_stamp(),hist.get_end_time_stamp(), hist.encode())

copyreg.pickle(HdrHistogram, HdrHistogramPickler.hdr_histogram_serializer)
            
def is_iterable(x):
    try:
        iter(x)
    except:
        return False
    return True
class RawData():
    class Samples():
        class TimeFrame():
            def __init__(self):
                self.min_time = None
                self.max_time = None
            def observe_time(self,t):
                self.min_time = t if not self.min_time else min(self.min_time, t)
                self.max_time = t if not self.max_time else max(self.max_time,t)
                
        SIGNIFICAN_FIGURES = 3
        def __init__(self, start_time, end_time, min_latency, max_latency,measurement_set, hdr_interval_ms):
            self.start_time = start_time
            self.end_time = end_time
            self.min_latency = min_latency
            self.max_latency = max_latency
            self.measurement_set = measurement_set
            self.total_time = self.end_time - self.start_time
            self.hdr_reset(hdr_interval_ms)
        def hdr_reset(self, hdr_interval_ms):
            self.hdr_interval_ms = hdr_interval_ms
            self.num_hdrs = math.ceil(self.total_time / self.hdr_interval_ms)
            self.interval_hdrs = { m: [] for m in self.measurement_set}
            self.interval_hdrs_observed_times = {m : [] for m in self.measurement_set}
            for op in self.measurement_set:
                hdr_lst = self.interval_hdrs[op]
                times_lst = self.interval_hdrs_observed_times[op]
                for i in range(self.num_hdrs):
                    hdr = HdrHistogram(self.min_latency,self.max_latency, self.SIGNIFICAN_FIGURES)
                    start_time = self.start_time + i * self.hdr_interval_ms
                    hdr.set_start_time_stamp(start_time)
                    hdr.set_end_time_stamp(min(hdr.get_start_time_stamp() + self.hdr_interval_ms - 1, self.end_time))
                    hdr_lst.append(hdr)
                    times_lst.append(self.TimeFrame())
                    
        def add_sample(self,op, timestamp, latency):
            hdr_idx = (timestamp - self.start_time) // self.hdr_interval_ms
            self.interval_hdrs[op][hdr_idx].record_value(latency)
            self.interval_hdrs_observed_times[op][hdr_idx].observe_time(timestamp)
        def refresh_intervals(self):
            for op in self.measurement_set:
                for hdr,timeframe in zip(self.interval_hdrs[op], self.interval_hdrs_observed_times[op]):
                    if timeframe.min_time:
                        hdr.set_start_time_stamp(timeframe.min_time)
                    if timeframe.max_time:
                        hdr.set_end_time_stamp(timeframe.max_time)
        def add(self,other_samples):
            assert isinstance(other_samples, RawData.Samples)
            self.start_time = min(self.start_time, other_samples.start_time)
            self.end_time = max(self.end_time, other_samples.end_time)
            other_ops = other_samples.get_valid_ops()
            my_ops = self.get_valid_ops()
            ops_only_on_other = other_ops.difference(my_ops)
            ops_on_both = my_ops.intersection(other_ops)
            
            for op in ops_only_on_other:
                self.interval_hdrs[op] = other_samples.interval_hdrs[op]
                self.interval_hdrs_observed_times[op] = other_samples.interval_hdrs_observed_times[op]
            for op in ops_on_both:
                for idx,hdrs in enumerate(zip(self.interval_hdrs[op], other_samples.interval_hdrs[op])):
                    my_hdr ,other_hdr = hdrs
                    if my_hdr.highest_trackable_value >= other_hdr.highest_trackable_value:
                        my_hdr.add(other_hdr)
                    else:
                        new_hdr = HdrHistogram(other_hdr.lowest_trackable_value, other_hdr.highest_trackable_value,self.SIGNIFICAN_FIGURES)
                        new_hdr.add(my_hdr)
                        new_hdr.add(other_hdr)
                        self.interval_hdrs[op][idx] = new_hdr
                    self.interval_hdrs_observed_times[op][idx].observe_time(other_hdr.get_start_time_stamp())
                    self.interval_hdrs_observed_times[op][idx].observe_time(other_hdr.get_end_time_stamp())
        def get_valid_ops(self,ops = None):
            if not ops:
                ops = { op for op in Measurement }
            if not is_iterable(ops):
                ops = {ops}
            for op in ops:
                assert isinstance(op, Measurement)
            existing_ops = { key for key in self.interval_hdrs.keys() }
            return existing_ops.intersection(ops)
        def get_total_hdr(self, ops = None):
            ops = self.get_valid_ops(ops)
            if len(ops) == 0:
                return None
            result = HdrHistogram(self.min_latency, self.max_latency, self.SIGNIFICAN_FIGURES)
            result.set_start_time_stamp(self.start_time)
            result.set_end_time_stamp(self.end_time)
            for op in ops:
                for interval_hdr in self.interval_hdrs[op]:
                    result.add(interval_hdr)
            return result
        def get_total_per_op_hdr(self, ops = None):
            ops = self.get_valid_ops(ops)
            result = {}
            for op in ops:
                result[op] = self.get_total_hdr(op)
            return result
        def get_ops_unified_interval_hdr(self, ops = None):
            ops = self.get_valid_ops(ops)
            interval_hdrs = [HdrHistogram(self.min_latency, self.max_latency, self.SIGNIFICAN_FIGURES) for x in range(self.num_hdrs)]
            for hdr in interval_hdrs:
                hdr.set_start_time_stamp(2**63 - 1)
                hdr.set_end_time_stamp(0)
            for op in ops:
                for result_hdr, op_hdr in zip(interval_hdrs, self.interval_hdrs[op]):
                    result_hdr.add(op_hdr)
            return interval_hdrs
            
    def __init__(self, dir_name, hdr_interval_ms = 10000, parallel = False):
        self.dir_name = Path(dir_name).absolute()
        self.sample_parser= re.compile(r'(?:Raw)?(?P<op>[^,]+),(?P<ts>\d+),(?P<latency>\d+)')
        self.intended_sample_parser= re.compile(r'(?:Raw)?Intended-(?P<op>[^,]+),(?P<ts>\d+),(?P<latency>\d+)')
        self.start_time = None
        self.end_time = None
        self.min_latency = None
        self.max_latency = None
        self.num_samples = 0
        self.num_samples_intended = 0
        self.measurement_set = Measurement
        self.samples = None
        self.samples_intended = None
        if parallel:
            self.load_parallel(hdr_interval_ms)
        else:
            self.load(hdr_interval_ms)
    def load_times_and_latencies(self):
        self.start_time = 2**63 - 1
        self.end_time = 0
        self.min_latency = 2**63 - 1
        self.max_latency = 0
        
        self.num_samples = 0
        self.num_samples_intended = 0
        #retrieve the limits for the hdrs
        limits_files = self.dir_name.glob("*.limits")
        for limits_file in limits_files:
            if not limits_file.is_file():
                continue
            with open(limits_file,'rb') as f:
                start_time, end_time = packer.unpack(f.read(packer.size))
                min_latency, max_latency = packer.unpack(f.read(packer.size))
                self.start_time = min(self.start_time, start_time)
                self.end_time = max(self.end_time, end_time)
                self.min_latency = min(self.min_latency, min_latency)
                self.max_latency = max(self.max_latency, max_latency)
        self.min_latency = 1
        print(f"limits aquired, start time: {self.start_time}, end time: {self.end_time}, min latency: {self.min_latency}, max latency: {self.max_latency}")
    def load(self, hdr_interval_ms = 10000):
        # First get start_time, end_time, min_latency, max_latency - we unify this for all ops and intensions
        # for simplicity sake
        # Possible optimization - every legend line probably only contains one measuremet type
        # We can also force it in the benchmark script using sort.
        self.load_times_and_latencies()
        print("Will now build histograms:")
        self.samples = RawData.Samples(self.start_time, self.end_time, self.min_latency, self.max_latency,self.measurement_set, hdr_interval_ms)
        self.samples_intended = RawData.Samples(self.start_time, self.end_time, self.min_latency, self.max_latency, self.measurement_set, hdr_interval_ms)
        histogram_files = list(Path(self.dir_name).glob("*.bin"))
        intended_filename_parser = re.compile(r"(?:Raw)?Intended_(?P<op>[^-]+)-(?P<loader>.*)\.bin")
        filename_parser = re.compile(r"(?:Raw)?(?P<op>[^-]+)-(?P<loader>.*)\.bin")
        position = 1
        global_progress_bar = tqdm(desc = f"Processing histogram files", position = position)
        position += 1
        for histfile in histogram_files:
            global_progress_bar.update(1)
            if m:=intended_filename_parser.match(str(histfile.name)):
                op , loader = m.groups()
                op = Measurement.from_string(op)
                interval = "intended interval"
                samples = self.samples_intended
            elif m:=filename_parser.match(str(histfile.name)):
                op , loader = m.groups()
                op = Measurement.from_string(op)
                interval = "normal interval"
                samples = self.samples
            total_lines = int(subprocess.check_output(f"wc -c {histfile}", shell=True).split()[0]) // packer.size
            
            progress_bar =  tqdm(desc = f"Processing {loader} measurements for {interval} for operaion {op.name}", position = position)
            position += 1
            with gzip.open(str(histfile), 'rb') as f:
                while(buff := f.read(BUFFER_SIZE)):
                    buff_offset = 0
                    while(buff_offset < len(buff)):
                        progress_bar.update(1)
                        timestamp, latency = packer.unpack_from(buff, buff_offset)
                        buff_offset += packer.size
                        assert timestamp >= self.start_time and timestamp <= self.end_time
                        assert (latency >= self.min_latency and latency <= self.max_latency) or latency == 0
                        samples.add_sample(op,timestamp,latency)
            self.samples.refresh_intervals()
            self.samples_intended.refresh_intervals()
    @staticmethod
    def samples_from_file(histfile,op,loader,interval, start_time,end_time,min_latency,max_latency, hdr_interval_ms, position):
        samples = RawData.Samples(start_time,end_time,min_latency,max_latency, [op], hdr_interval_ms)
        progress_bar = tqdm(desc = f"Processing {loader} measurements for {interval} for operation {op.name}",  leave = False, position=position)
        try:
            file_size = int(subprocess.check_output(f"gzip -l {histfile} | sed -E 's/[[:space:]]+/ /g' | sed -E 's/^[[:space:]]*//g' | cut -d ' ' -f 2 | tail -1", shell = True))// packer.size
            progress_bar.reset(total = file_size)
        except:
            pass
        with gzip.open(str(histfile), 'rb') as f:
            while(buff := f.read(BUFFER_SIZE)):
                buff_offset = 0
                while(buff_offset < len(buff)):
                    progress_bar.update(1)
                    timestamp, latency = packer.unpack_from(buff, buff_offset)
                    buff_offset += packer.size
                    assert timestamp >= start_time and timestamp <= end_time
                    assert (latency >= min_latency and latency <= max_latency) or latency == 0
                    samples.add_sample(op,timestamp,latency)
        return samples
    def load_parallel(self, hdr_interval_ms = 10000):
        # First get start_time, end_time, min_latency, max_latency - we unify this for all ops and intensions
        # for simplicity sake
        # Possible optimization - every legend line probably only contains one measuremet type
        # We can also force it in the benchmark script using sort.
        self.load_times_and_latencies()
        print("Will now build histograms parallely:")
        self.samples = RawData.Samples(self.start_time, self.end_time, self.min_latency, self.max_latency,self.measurement_set, hdr_interval_ms)
        self.samples_intended = RawData.Samples(self.start_time, self.end_time, self.min_latency, self.max_latency, self.measurement_set, hdr_interval_ms)
        histogram_files = list(Path(self.dir_name).glob("*.bin"))
        intended_filename_parser = re.compile(r"(?:Raw)?Intended_(?P<op>[^-]+)-(?P<loader>.*)\.bin")
        filename_parser = re.compile(r"(?:Raw)?(?P<op>[^-]+)-(?P<loader>.*)\.bin")
        position = 0
        global_progress_bar = tqdm(desc = f"Processing histogram files", total = len(histogram_files),position = position)
        position += 1        
        def add_results(intended, samples : RawData.Samples):
            global_progress_bar.update(1)
            if intended:
                self.samples_intended.add(samples)
            else:
                self.samples.add(samples)
        pool = multiprocessing.Pool()
        for histfile in histogram_files:
            if m:=intended_filename_parser.match(str(histfile.name)):
                intended = True
                op , loader = m.groups()
                op = Measurement.from_string(op)
                interval = "intended interval"
                samples = self.samples_intended
            elif m:=filename_parser.match(str(histfile.name)):
                op , loader = m.groups()
                op = Measurement.from_string(op)
                interval = "normal interval"
                samples = self.samples
                intended = False
            args_for_func = (histfile,op,loader,interval, self.start_time,self.end_time,self.min_latency,self.max_latency, hdr_interval_ms, position)
            pool.apply_async(RawData.samples_from_file,args_for_func, callback=partial(add_results,intended))
            position += 1
        pool.close()
        pool.join()
        self.samples.refresh_intervals()
        self.samples_intended.refresh_intervals()
class HdrDataWrapper():
    def __init__(self, file_name):
        self.file_name = os.path.realpath(file_name)
        with open(self.file_name, 'rb') as f:
            self.intervals_hdr : RawData = pickle.load(f)
        self.grand_total_hdr_ = None
    @staticmethod
    def get_hdr_total_time(hdr : HdrHistogram, time_scale = 1000):
        return (hdr.get_end_time_stamp() - hdr.get_start_time_stamp()) / time_scale
    @staticmethod
    def get_hdr_timepoint(hdr : HdrHistogram, time_scale, normalization_value = 0):
        start_time = (hdr.start_time_stamp_msec - normalization_value) / time_scale
        end_time = (hdr.end_time_stamp_msec - normalization_value) / time_scale
        return (start_time/2) + (end_time/2)
    def get_samples(self, intended = False) -> RawData.Samples :
        return self.intervals_hdr.samples_intended if intended else self.intervals_hdr.samples
    def get_ops_unified_interval_hdrs(self, ops = None, intended = False):
        """_summary_
        Return a list of **non empty** interval hdrs.
        Args:
            ops (_type_, optional): A list of operations to include. Defaults to None - which means everything.
            intended (bool, optional): The sample type , intended if True , normal otherwise. Defaults to False.
        """
        samples = self.get_samples(intended)
        # Filter out empty sections
        return list(filter(lambda hdr: hdr.total_count > 0, samples.get_ops_unified_interval_hdr(ops)))
    def total_count(self, ops = None, intended = False):
        return self.get_samples(intended).get_total_hdr(ops).total_count
    def total_time(self, ops = None, time_scale = 1000, intended = False):
        hdrs = self.get_ops_unified_interval_hdrs(ops,intended)
        if len(hdrs) <= 0:
            return 0
        return (hdrs[-1].get_end_time_stamp() - hdrs[0].get_start_time_stamp()) / time_scale
    def total_throughput(self,ops = None, time_scale = 1000, intended = False):
        return self.total_count(ops,intended) / self.total_time(ops,time_scale)
    def get_ops(self, intended = False):
        result = set()
        for op in Measurement:
            if len(self.get_ops_unified_interval_hdrs(op)) != 0:
                result.add(op)
        return result
        
    def get_time_points(self, hdrs, time_scale = 1000,normalize = True,additional_normalization = 0):
        if isinstance(hdrs, HdrHistogram):
            hdrs = [hdrs]
        assert is_iterable(hdrs)
        for hdr in hdrs:
            assert isinstance(hdr, HdrHistogram)
        assert isinstance(additional_normalization,(int,float))
        normalization_value = hdrs[0].get_start_time_stamp() if normalize else 0
        normalization_value += additional_normalization
        return [ self.get_hdr_timepoint(hdr,time_scale, normalization_value) for hdr in hdrs ]
    def get_throughput_graph(self,ops = None, time_scale = 1000, normalize = True,intended = False):
        samples = self.get_ops_unified_interval_hdrs(ops, intended)
        y = np.array([ hdrhist.total_count / (self.get_hdr_total_time(hdrhist)) for hdrhist in samples])
        x = np.array(self.get_time_points(samples,time_scale,normalize))
        return x, y
    
    def get_mean_latency_graph(self,ops = None, time_scale = 1000, latency_scale = 1000,normalize = True,intended = False):
        samples = self.get_ops_unified_interval_hdrs(ops, intended)
        x = np.array(self.get_time_points(samples, time_scale, normalize))
        y = np.array([ hdrhist.get_mean_value()/latency_scale for hdrhist in samples])
        return x, y
    def get_precentiles_latency_graph(self, precentiles, ops = None,time_scale = 1000, latency_scale = 1000,normalize = True,intended = False):
        if isinstance(precentiles,int) or isinstance(precentiles,float):
            precentiles = [precentiles]
        for p in precentiles:
            assert isinstance(p,int) or isinstance(p,float)
        samples = self.get_ops_unified_interval_hdrs(ops,intended)
        x = self.get_time_points(samples,time_scale,normalize)
        graphs = {}
        for p in precentiles:
            graphs[p] = np.array([ hdrhist.get_value_at_percentile(p)/latency_scale for hdrhist in samples])
        return x , graphs
    def get_total_count_per_op(self, ops = None, intended = False):
        if not ops:
            ops = set(Measurement)
        if not is_iterable(ops):
            assert isinstance(ops,Measurement)
            ops  = [ops]
        ops = set(ops)
        samples = self.get_samples(intended).get_total_per_op_hdr(ops)
        non_existent_ops = ops.difference(set(samples.keys()))
        result = {m : 0 for m in non_existent_ops}
        for m,hdr in samples.items():
            result[m] = hdr.total_count
        return result