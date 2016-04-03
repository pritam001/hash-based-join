# Hash Based Join
# Input: relation1 file, relation 2 file, record size, page size,
#           number of available pages, maximum number of hashing rounds
# Example: relation1.txt, relation2.txt, 180, 130, 400, 5, 3
# Number of records that fit per page are floor of page size/ record size.
import math

__author__ = 'Prince Leo'
__copyright__ = "Copyright 2016, IIT Guwahati"
__license__ = "MIT"
__version__ = "1.0.1"
__maintainer__ = "Pritam Sarkar"
__email__ = "001pritam2012@gmail.com"
__status__ = "Development"

line = raw_input()
line = line.split(',')
for i in range(0, len(line), 1):
    line[i] = line[i].strip()
filename1 = line[0]
filename2 = line[1]
record_size1 = int(line[2])
record_size2 = int(line[3])
page_size = int(line[4])
no_of_available_pages = int(line[5])
remaining_hash_rounds = int(line[6])

rec_per_page1 = int(math.floor(page_size / record_size1))
rec_per_page2 = int(math.floor(page_size / record_size2))
no_of_buckets = no_of_available_pages - 1

f1 = open(filename1, 'r')
f2 = open(filename2, 'r')

rel_count1 = 0
rel_count2 = 0
text1 = f1.read()
text2 = f2.read()
rel1_list = []
rel2_list = []
for i in text1.split("\n"):
    if i == "":
        continue
    rel1_list.append(int(i))
    rel_count1 += 1
for i in text2.split("\n"):
    if i == "":
        continue
    rel2_list.append(int(i))
    rel_count2 += 1


# hashes using y = x mod(var) + 1 ; z = y mod(4) + 1
def hash_func(round_no, total_buckets, integer):
    z = total_buckets + 3*(round_no - 1)
    temp = integer - int(math.floor(integer / z)) * z + 1
    temp2 = temp - int(math.floor(temp / total_buckets)) * total_buckets + 1
    return temp2


# input : list of int from a relation
def hashing_round(round_no, total_buckets, int_list1, int_list2):
    print "\n\nHashing Round " + str(round_no) + ":"
    global rec_per_page1
    global rec_per_page2
    global remaining_hash_rounds
    if round_no > remaining_hash_rounds:
        print "Maximum hash rounds limit exceeded.\nUnable to complete join operation.\nAborting . . . "
        exit()

    print "Reading relation1:"
    bucket_list1 = []
    sec_bucket_list1 = []
    page_count_list1 = []
    for i in range(0, total_buckets, 1):
        bucket_list1.append([])
        sec_bucket_list1.append([])
        page_count_list1.append(0)

    for i in range(0, len(int_list1), 1):
        x = hash_func(round_no, total_buckets, int_list1[i])
        # x = int_list1[i] - int(math.floor(int_list1[i] / total_buckets)) * total_buckets + 1
        bucket_list1[x - 1].append(int_list1[i])
        print "Tuple " + str(i + 1) + ": " + str(int_list1[i]) + " Mapped to bucket: " + str(x) + ""
        if len(bucket_list1[x - 1]) >= rec_per_page1:
            print "Page for bucket " + str(x) + " full. Flushed to secondary storage."
            sec_bucket_list1[x - 1].append(bucket_list1[x - 1])
            bucket_list1[x - 1] = []
            page_count_list1[x - 1] += 1

    for i in range(0, len(bucket_list1), 1):
        if bucket_list1[i]:
            page_count_list1[i] += 1

    print "Done with relation1."
    print "Created following files."
    for i in range(0, len(page_count_list1), 1):
        if page_count_list1[i] != 0:
            print "rel1.round" + str(round_no) + ".bucket" + str(i + 1) + ": " + str(page_count_list1[i]) + " pages"
    print "list1 : " + str(int_list1)
    print "bucket1 : " + str(bucket_list1)
    print "Secondary_storage1 : " + str(sec_bucket_list1)
    print "page_count_list1 : " + str(page_count_list1)
    # for i in range(1, total_buckets, 1):
    # temp_f = open("rel1_round" + str(round_no) + ".txt", 'w')
    print "\nReading relation2:"
    bucket_list2 = []
    sec_bucket_list2 = []
    page_count_list2 = []
    for j in range(0, total_buckets, 1):
        bucket_list2.append([])
        sec_bucket_list2.append([])
        page_count_list2.append(0)
    for j in range(0, len(int_list2), 1):
        x = hash_func(round_no, total_buckets, int_list2[j])
        # x = int_list2[j] - int(math.floor(int_list2[j] / total_buckets)) * total_buckets + 1
        bucket_list2[x - 1].append(int_list2[j])
        print "Tuple " + str(j + 1) + ": " + str(int_list2[j]) + " Mapped to bucket: " + str(x) + ""
        if len(bucket_list2[x - 1]) >= rec_per_page2:
            print "Page for bucket " + str(x) + " full. Flushed to secondary storage."
            sec_bucket_list2[x - 1].append(bucket_list2[x - 1])
            bucket_list2[x - 1] = []
            page_count_list2[x - 1] += 1

    for j in range(0, len(bucket_list2), 1):
        if bucket_list2[j]:
            page_count_list2[j] += 1

    print "Done with relation2."
    print "Created following files."
    for j in range(0, len(page_count_list2), 1):
        if page_count_list2[j] != 0:
            print "rel2.round" + str(round_no) + ".bucket" + str(j + 1) + ": " + str(page_count_list2[j]) + " pages"
    print "list2 : " + str(int_list2)
    print "bucket2 : " + str(bucket_list2)
    print "Secondary_storage2 : " + str(sec_bucket_list2)
    print "page_count_list2 : " + str(page_count_list2)
    # for j in range(1, total_buckets, 1):
    # temp_f = open("rel1_round" + str(round_no) + ".txt", 'w')

    print "\nJoining . . ."
    for i in range(0, total_buckets, 1):
        if page_count_list1[i] == 0:
            print "Round " + str(round_no) + " Bucket " + str(i + 1) + ": No matching tuple from relation2. No further processing required.\n"
            continue
        if page_count_list2[i] == 0:
            print "Round " + str(round_no) + " Bucket " + str(i + 1) + ": No matching tuple from relation1. No further processing required.\n"
            continue
        if page_count_list1[i] + page_count_list2[i] <= no_of_available_pages - 1:
            print "Round " + str(round_no) + " Bucket " + str(i + 1) + ": Total size is " + str(page_count_list1[i] + page_count_list2[i]) + " pages"
            print "Total available pages " + str(no_of_available_pages) + "."
            print "Performing in memory join."
            matching_pair_list = []
            list1 = bucket_list1[i]
            for page in sec_bucket_list1[i]:
                list1 += page
            list2 = bucket_list2[i]
            for page in sec_bucket_list2[i]:
                list2 += page

            for i in range(0, len(list1), 1):
                for j in range(0, len(list2), 1):
                    if list1[i] == list2[j]:
                        matching_pair_list.append((list1[i], list2[j]))

            if matching_pair_list:
                print "Matching pairs are "
                for pair in matching_pair_list:
                    print pair
                print "\n"
            else:
                print ": No matching tuple found.\n"
        else:
            print "Round " + str(round_no) + " Bucket " + str(i + 1) + ": Total size is " + str(page_count_list1[i] + page_count_list2[i]) + " pages. Cannot perform in memory join."
            print "Performing round " + str(round_no + 1) + " of hashing for round1.bucket1"
            list1 = bucket_list1[i]
            for page in sec_bucket_list1[i]:
                list1 += page
            list2 = bucket_list2[i]
            for page in sec_bucket_list2[i]:
                list2 += page
            print "Size of relation 1: " + str(page_count_list1[i]) + " pages"
            print "Size of relation 2: " + str(page_count_list2[i]) + " pages"
            print "Total number of available pages: " + str(no_of_available_pages)
            print "Number of buckets in hash table: " + str(total_buckets)
            print "\nNext round lists"
            print "list1 : " + str(list1) + " list2 : " + str(list2) + ""
            hashing_round(round_no + 1, total_buckets, list1, list2)

    return 1

# Output printing
print "Output:"
print "Size of relation 1: " + str(rel_count1 * record_size1 / page_size) + " pages"
print "Size of relation 2: " + str(rel_count2 * record_size2 / page_size) + " pages"
print "Total number of available pages: " + str(no_of_available_pages)
print "Number of buckets in hash table: " + str(no_of_buckets)

hashing_round(1, no_of_buckets, rel1_list, rel2_list)

f1.close()
f2.close()
