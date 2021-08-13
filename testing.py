import json
import time
import ast


a = open("pipeline_script/data_source_BOH/output/300721-dtdo_output2-00000-of-00001.txt").readlines()
for data in a:
    data = data.strip("/n")

start_time1 = time.time()
# Process using eval
data1 = []
for i in a:
    data1.append(eval(i))

print("Time using eval:", time.time() - start_time1)


start_time2 = time.time()
# Process using json.loads
data2 = []
for i in a:
    i = i.replace("'",'"')
    i = i.replace("None", "null")
    data2.append(json.loads(i))

print("Time using json.loads:", time.time() - start_time2)


# file = open("test.txt", "w")
# for i in data2:
#     print(i, file=file)

for i in range(len(data1)):
    if data1[i] != data2[i]:
        print(data1[i])
        print(data2[i])
        print()

# print(data1[1])
# print(data2[1])

# test1 = {"test":"test"}
# test2 = json.loads(json.loads(json.dumps("{'test':'test'}")))
# print(test1)
# print(type(test2))
# print(test1==test2)