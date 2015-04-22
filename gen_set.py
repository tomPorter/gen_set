#!/usr/bin/env python
import csv
import gzip
def gen_set(file_name,*start_end):
  """Given a filename (for a fixed format file) and 1 or more start/end positions, returns a set of distinct 'keys' extracted from the file."""
  if 'gz' in file_name:
      iflines = gzip.open(file_name,'rb').readlines()
  else:
      iflines = open(file_name,'r').xreadlines()
  file_set = set([])
  x = iter(start_end)
  start_end_pairs = [(item, x.next()) for item in x]
  for line in iflines:
    keys = []
    for s,e in start_end_pairs:
      keys.append(line[s-1:e].strip())
    file_set.add('|'.join(keys))
  return file_set

def gen_list(file_name,*start_end):
  """Given a filename (for a fixed format file) and 1 or more start/end positions, returns a list of 'keys' extracted from the file."""
  if 'gz' in file_name:
      iflines = gzip.open(file_name,'rb').readlines()
  else:
      iflines = open(file_name,'r').xreadlines()
  x = iter(start_end)
  start_end_pairs = [(item, x.next()) for item in x]
  for line in iflines:
    keys = []
    for s,e in start_end_pairs:
      keys.append(line[s-1:e].strip())
    yield '|'.join(keys)

def gen_list_csv(file_name,delim, *field_positions):
  """Given a filename (for a delimited file), a delimiter and 1 or more field positions, returns a list of 'keys' extracted from the file."""
  if 'gz' in file_name:
      fh = csv.reader(gzip.open(file_name,'rb'),delimiter=delim)
  else:
      fh = csv.reader(open(file_name,'r'),delimiter=delim)
  for fields in fh:
    keys = []
    for x in field_positions:
      keys.append(fields[x-1].strip())
    yield '|'.join(keys)

def array_to_hash_count(array):
  """Given a list, returns a hash of unique values found in the list and count of occurances."""
  hash = {}
  for x in array:
    if hash.has_key(x):
      hash[x] += 1
    else:
      hash[x] = 1
  return hash

def array_to_hash_values(array):
  """Given a list of pipe-delimited keys and single values ("k|v"), returns a hash of unique keys with a list of all distinct values for key as the hash value."""
  hash = {}
  for x in array:
    k,v = x.split('|')
    if hash.has_key(k):
      hash[k].append(v)
    else:
      hash[k] = [v,]
  return hash

def sort_hash_by_count(hash):
  """Given a hash where the values are counts, returns the hash sorted by the counts in descending order."""
  items = hash.items()
  backitems = [[v[1],v[0]] for v in items]
  #backitems.sort(reverse=True)
  backitems.sort()
  sortedlist = [(backitems[i][1],backitems[i][0]) for i in range(0,len(backitems))]
  return sortedlist 

def gen_sorted_counts(file_name,*start_end):
  """Given a filename (for a fixed format file) and 1 or more start/end positions, returns a list of distinct 'keys' and counts extracted from the file."""
  return sort_hash_by_count(array_to_hash_count(gen_list(file_name,*start_end)))

def import_result_type(string):
  """Given a specific ExactTarget list import result, determine it's general type."""
  if 'correct format' in string:
    return 'invalid'
  elif 'global unsub' in string:
    return 'global'
  elif 'master unsubscribe' in string:
    return 'Client unsub'
  elif 'triggered our spam' in string:
    return 'list detective'
  else:
    return string

def get_fields(string):
  """Given a pipe-delimited string, return a list of tokens in the string."""
  if '|' in string:
    return string.split('|')
  else:
    return list()
