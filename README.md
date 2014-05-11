# RSTORE
## Introduction
R-Store is a scalable distributed
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
Please download the compressed files (rstore.tar.gz.a*) in the folder [binaries](https://github.com/lifeng5042/RStore/tree/master/binaries) and execute the following command in order to decompress the files.
```bash
cat rstore.tar.gz.a* | tar -zxv
```
## Getting Started
Refer to the file [guide.md](https://github.com/lifeng5042/RStore/blob/master/guide.md)
## Source Code
## Publication
[R-Store: A Scalable Distributed System for Supporting Real-time Analytics](http://www.comp.nus.edu.sg/~ooibc/icde14-rstore.pdf)

