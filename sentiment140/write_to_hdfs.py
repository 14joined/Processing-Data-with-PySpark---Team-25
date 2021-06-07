import os
from subprocess import PIPE, Popen

file_name = "<...>"

hdfs_path = os.path.join(os.sep, 'user', 'bowser', file_name)

put = Popen(["hadoop", "fs", "-put", file_name, hdfs_path], stdin=PIPE, bufsize=-1)
put.communicate()