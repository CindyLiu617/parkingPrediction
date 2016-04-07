from appserver import utils


def number_of_overlap(entries_file, exits_file):
    # heap  = []
    # heapq.heapify(heap)
    # overlap_num = 0
    # with open(intervals_file, 'r') as f:
    #     for line in f:
    #         if len(heap) == 0:
    #             heapq.heappush(heap, line[1])
    #             overlap_num += 1
    #             continue
    #         if heap[0] <= line[0]:
    #             heapq.heappop(heap)
    #             heapq.heappush(heap, line[1])
    #         else:
    #             heapq.heappush(heap, line[1])
    #             overlap_num += 1
    # return overlap_num

    num_of_cars = 0
    with open(entries_file, 'r') as etry, open(exits_file, 'r') as ext:
        fmt = "%Y-%m-%d  %H:%M:%S\n"
        entry_line = etry.readline().strip()
        exit_line = ext.readline().strip()
        while entry_line != '' or exit_line != '':
            entry_time = utils.str_to_datetime(entry_line, fmt)
            exit_time = utils.str_to_datetime(exit_line, fmt)
            if exit_line == '' or entry_time < exit_time:
                num_of_cars += 1
                entry_line = etry.readline().strip()
            else:
                num_of_cars -= 1
                exit_line = ext.readline().strip()

            # TODO: write num_of_cars to database
