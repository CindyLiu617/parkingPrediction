import csv
import heapq
import os

import utils

LINES_PER_FILE = 100000
DATE_FORMAT = "%Y-%m-%d  %H:%M:%S\n"


def external_sort(local_file):
    def record_constructor(uploaded):
        with open(uploaded, 'rb') as f:
            csv_reader = csv.reader(f, delimiter='\t')
            entry_times = []
            exit_times = []
            file_index = 0
            entry_files = []
            exit_files = []
            for i, line in enumerate(csv_reader):
                if i > 0:
                    splited = line[0].split(',')
                    entry_time = "%s\n" % splited[0]
                    exit_time = "%s\n" % splited[1]
                    entry_times.append(entry_time)
                    exit_times.append(exit_time)
                    if i % LINES_PER_FILE == 0:
                        entry_time = sort_string_list(entry_times)
                        exit_time = sort_string_list(exit_times)
                        try:
                            entry_file_name = 'entryTime%d.txt' % file_index
                            exit_file_name = 'exitTime%d.txt' % file_index
                            entry_files.append(entry_file_name)
                            exit_files.append(exit_file_name)
                            with open(entry_file_name, 'w') as entry_time_txt,\
                                    open(exit_file_name, 'w') as exit_time_txt:
                                entry_time_txt.writelines(entry_time)
                                exit_time_txt.writelines(exit_time)
                                file_index += 1
                        except IOError as e:
                            print('Operation failed: %s' % e.strerror)
                        entry_times = []
                        exit_times = []
            return entry_files, exit_files

    def sort_string_list(time_string_list):
        def compare(left, right):
            left_time = utils.str_to_datetime(left, DATE_FORMAT)
            right_time = utils.str_to_datetime(right, DATE_FORMAT)
            if left_time > right_time:
                return 1
            elif left_time < right_time:
                return -1
            else:
                return 0

        sorted_list = sorted(time_string_list, cmp=compare)
        return sorted_list

    def merge_k_sorted_files(file_list, merged_file_name):
        with open(merged_file_name, 'w') as merged_file:
            file_readers = []
            try:
                for file_name in file_list:
                    f = open(file_name, 'r')
                    file_readers.append(f)
                heap = []
                for reader in file_readers:
                    line_read = reader.readline()
                    heap.append((utils.str_to_datetime(line_read, DATE_FORMAT), line_read, reader))
                heapq.heapify(heap)
                while heap:
                    pop = heapq.heappop(heap)
                    merged_file.write(pop[1])
                    line_read = pop[2].readline().strip()
                    if line_read != '':
                        heapq.heappush(heap, (utils.str_to_datetime(line_read, DATE_FORMAT), line_read, pop[2]))
                merged_file.close()
                for reader in file_readers:
                    reader.close()
                for file_name in file_list:
                    os.remove(os.getcwd() + '/' + file_name)
                return merged_file_name
            except IOError as e:
                print('Operation failed: %s' % e.strerror)

    entry_file_list, exit_file_list = record_constructor(local_file)
    entries = merge_k_sorted_files(entry_file_list, 'entry_time.txt')
    exits = merge_k_sorted_files(exit_file_list, 'exit_time.txt')
    return entries, exits

