# Hadoop_MR_project
Project made for Hadoop and MapReduce Udacity program.

This project involved completing specific tasks, working on Udacity forum dataset. I wrote all the codes in Python thanks to Hadoop streaming utility which enables running MapReduce codes written in other languages than Java.
The basic structure is - the forum has nodes. All nodes have a body and author_id. Top level nodes are called questions, and will also have a title and tags. Questions can have answers. Both questions and answers can have comments.
The structure of the original data line is:
- "id": id of the node
- "title": title of the node. in case "node_type" is "answer" or "comment", this field will be empty
- "tagnames": space separated list of tags
- "author_id": id of the author
- "body": content of the post
- "node_type": type of the node, either "question", "answer" or "comment"
- "parent_id": node under which the post is located, will be empty for "questions"
- "abs_parent_id": top node where the post is located
- "added_at": date added
- and several other columns (score, state_string, last_edited_id, last_activity_by_id, last_activity_at, active_revision_id, extra, extra_ref_id, extra_count, marked).

# Task 1
Our first task is to find for each student what is the hour during which the student has posted the most posts. The output we want to get is:

- "author_id"   hour

First we need to construct the mapper code that outputs important, sorted information to the reducer. In this case we want the mapper to output from each post, the "author_id" and the hour of the post. The dates in "added_at" are given with nanoseconds after a '.', so we split it and take the first part which contains the data in '%Y-%m-%d %H:%M:%S' format. We use the datetime functionality to get the hour.

```
#!/usr/bin/python

import sys
import csv
from datetime import datetime

reader = csv.reader(sys.stdin, delimiter='\t')
writer = csv.writer(sys.stdout, delimiter='\t', quotechar='"', quoting=csv.QUOTE_ALL)

reader.next()
for line in reader:
    post_id, title, tagnames, author_id, body, node_type, parent_id, abs_parent_id, added_at, score, state_string, last_edited_id, last_activity_by_id, last_activity_at, active_revision_id, extra, extra_ref_id, extra_count, marked = line
    hour = added_at.split('.')[0]
    hour = datetime.strptime(hour, '%Y-%m-%d %H:%M:%S').hour
    print author_id, "\t", hour
```

The MapReduce framework sorts the data before sending it to the reducer, which enables the reducer to do its work. 
Our reducer gets sorted data by the author_id from our mapper. We could set a variable for each hour, but that would take a lot of space and additional lines, so instead we make an hour dictionary. The code works so that, if the author_id is still the same it updates the dictionary with a key - an hour, and a value - times an hour occurs for each author. And when the author_id changes we get the key with the highest value and print the output of "author_id" hour. The code for reducer looks like this:

```
#!/usr/bin/python

import sys
from operator import itemgetter

old_id = None
hour_dic = {}

for line in sys.stdin:
    data = line.strip().split('\t')
    if len(data) != 2:
        continue
        
    this_id, this_hour = data

    if old_id and old_id != this_id:
        sorted_hours = sorted(hour_dic.items(), key = itemgetter(1), reverse = True)
        top_count = sorted_hours[0][1]
        for tuple in sorted_hours:
            if tuple[1]==top_count:
                print old_id, '\t', tuple[0]
        old_id = this_id
        hour_dic = {}

    old_id = this_id
    if this_hour in hour_dic:
        hour_dic[this_hour] += 1
    else:
        hour_dic[this_hour] = 1

if old_id != None:
    sorted_hours = sorted(hour_dic.items(), key = itemgetter(1), reverse = True)
    top_count = sorted_hours[0][1]
    for tuple in sorted_hours:
        if tuple[1]==top_count:
            print old_id, '\t', tuple[0] 
```

 # Task 2
 In this task we are interested to see if there is a correlation between the length of a post and the length of answers.
 We want our mapreduce job to process the dataset and get an output of:
 - post_length     average_answer_length

This time our mapper will have two options of possible output. If the node_type is a 'question' it prints the post_id, node_type, and the length of the post (body of the post). If the node_type is 'answer' it print the parent_id (the ID of the question it answers), node_type and the length of the answer.

```
#!/usr/bin/python

import sys
import csv

reader = csv.reader(sys.stdin, delimiter='\t')
writer = csv.writer(sys.stdout, delimiter='\t', quotechar='"', quoting=csv.QUOTE_ALL)

next(reader)
for line in reader:
    post_id, title, tagnames, author_id, body, node_type, parent_id, abs_parent_id, added_at, score, state_string, last_edited_id, last_activity_by_id, last_activity_at, active_revision_id, extra, extra_ref_id, extra_count, marked = line

    if node_type == 'question':
        print post_id, "\t", node_type, "\t", len(body)

    elif node_type == 'answer':
        print parent_id, "\t", node_type, "\t", len(body)
```

The mapper outputs 3 pieces of information. The data is send to the reducer sorted by the id of the question. There is always one question with unique ID, and there might be multiple answers with this ID. The reducer determines if the current line comes from a question or an answer thanks to the mapper output. If the input is a question the reducer stores its length in a variable q_len. If the input is an answer it stores the cumulative length of all the answers in the total_ans variable and also stores number of answers in num_ans variable. When the ID changes it calculates the average answer (total_ans / num_ans). And outputs the post ID, the length of a question and the length of the average answer.

```
#!/usr/bin/python

import sys

old_id = None
total_ans = 0
q_len = 0
num_ans = 0
num_q = 0

for line in sys.stdin:
  data = line.rstrip().split('\t')
  if len(data) != 3:
        	continue

  this_id, this_type, this_length = data
  this_length = int(this_length)
	
  if old_id and old_id != this_id:
    if num_q != 0:
      q_len = q_len
    else:
      q_len = 'No such question'
    if num_ans != 0:
      av_ans = total_ans / num_ans
    else:
      av_ans = 'no answer'

    print old_id, "\t", q_len, "\t", av_ans
    old_id = this_id
    num_q = 0
    num_ans = 0
    total_ans = 0
    q_len = 0

  old_id = this_id
  if this_type.strip() == 'question':
    q_len += this_length
    num_q += 1
  elif this_type.strip() == 'answer':
    total_ans += this_length
    num_ans +=1

if old_id != None:
  if num_q != 0:
    q_len = q_len
  else:
    q_len = 'No such question'
  if num_ans != 0:
    av_ans = total_ans / num_ans
  else:
    av_ans = 'no answer'
  print old_id, "\t", q_len, "\t", av_ans
 ```
