import math

total_count = 1000000
per_count = 100000
batch_count = 5000
for start in range(0, total_count, per_count):
    sample_start_idx = 0
    index = math.ceil(start / per_count) + 1
    with open(r'gen_start_load_{}.bat'.format(index), 'w+') as f:
        f.write("@echo Started: %date% %time%" + '\n')
        while per_count > 0:
            s = 'start comparison.exe --mode=2 --start_idx={} --count={} --batch=20 --period=2'.format(start, batch_count)
            f.write(s + '\n')
            start += batch_count
            per_count -= batch_count

        f.write('pause')
