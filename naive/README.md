# Naive (almost) approach

Read file line by line, pass on to several threads. Each thread has its own unique map of aggregates, so there are is no synchronization happening.
It is guaranteed that all data about a station belongs to one thread only.
*No profiling done yet*
