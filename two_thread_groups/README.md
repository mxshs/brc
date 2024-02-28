# Two threads approach

Read file by chunks (allows for less file access + single pass read), split into lines, pass on to threads.
Everything else is pretty much the same as in the naive approach.
*No profiling done yet*
