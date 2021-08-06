import time
start = time.time()

a = open("data_source_BOH/output/300721-dtdo-new_output2-00000-of-00001.txt", "r").readlines()

new_set = set()
duplicate_set = set()

for data in a:
    if data not in new_set:
        new_set.add(data)
    else:
        duplicate_set.add(data)

# for data in new_set:
#     print(data)

print(len(new_set))
print(len(duplicate_set))
end = time.time()

print(end-start)