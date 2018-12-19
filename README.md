# vertica-udf

Contains c++ transform functions for fast data processing on Vertica OLAP database:
* RapidJsonExtractor - fast sax-based json-to-columns parser using rapid_json library.
* RapidArrayExtractor - same for simple arrays.
* DistinctHashCounter - pipelined group by for many dimention combinations in single run.
* Transpose - column-to-row transpose.

Best performance is archived on over(partition by auto) clause.

