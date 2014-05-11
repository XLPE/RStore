# RSTORE
======
## Introduction
It is widely recognized that OLTP and OLAP
queries have different data access patterns, processing needs
and requirements. Hence, the OLTP queries and OLAP queries
are typically handled by two different systems, and the data
are periodically extracted from the OLTP system, transformed
and loaded into the OLAP system for data analysis. With the
awareness of the ability of big data in providing enterprises useful
insights from vast amounts of data, effective and timely decisions
derived from real-time analytics are important. It is therefore
desirable to provide real-time OLAP querying support, where
OLAP queries read the latest data while OLTP queries create
the new versions.

In this work, we propose R-Store, a scalable distributed
system for supporting real-time OLAP by extending the MapReduce framework.
We extend an open source distributed key/value
system, HBase, as the underlying storage system that stores data
cube and real-time data. When real-time data are updated, they
are streamed to a streaming MapReduce, namely Hstreaming, for
updating the cube on incremental basis. Based on the metadata
stored in the storage system, either the data cube or OLTP
database or both are used by the MapReduce jobs for OLAP
queries. 

## Executable Files
The executable files are compressed in rstore.tar.gz.a*. 
Please download the four files (rstore.tar.gz.a*) and execute 
`cat rstore.tar.gz.a* | tar -zxv`
in order to decompress the files.

## Source Code
