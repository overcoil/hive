Compression in HiveServer2
==========================

The compressors in this folder can compress 1, 2, 4, 8 byte ints, doubles and strings. For integers, we handle the sign bit and remove trailing zeroes. For doubles, we do a similar operation. For strings, we have implemented prefix compression. All the methods have a compress function (it can be compressIntegers, compressDoubles, compressStrings. depends on the input type) that take in either an array or an arraylist and return a byte array. 

Regardless of the input arguments, the compression methods *always* return a byte array. And each of the methods also return a packed length array which has the lengths of the individual bytes of the byte array mentioned before. (To do the length packing, we use the packLengths function inside CompressionUtils.java.) 

To compress the information, the following is a summary of the code in the folder: 

Compression methods
-------------------

- (Int | SmallInt | Tiny | BigInt | Double | Prefix)Compressors -- compression method implementations
- CompressionUtils.java -- common methods are part of this class. as of now, it only has packLengths() to do length packing 
- SimbaCompressorCommon.java -- some of the common members across all compressors are part of this class. 



Data Transfer 
=============

After the compression is done, we want to send that data to client. As part of Hive, we use thrift to send the data across. To do so, we have created the struct TEnColumn inside TCLIService.thrift to enable this. But before that, we want to write our data into a bytebuffer that can then be used to write to thrift. To do so, we use either *BaseEnDataLayout* or *StringEnDataLayout*. Why have two? 

The reason is that for an int or a double, we send the following information: 

* minLen 
* bitsPerLen 
* encodedData
* packedLengths

But for prefix compression, we want to send a bit more

* prefixLength
* prefixBytes

This is on top of what we already send for integers. So, we created StringEnDataLayout for strings while we use BaseEnDataLayout for the other types. To write into the class, we use writeDataToBuffer() which creates a new bytebuffer and writes into it. 


Interface
========

The .java files here are actually part of a plugin, an independent jar file that can compress any (most) Hive data types. To help Hiveserver2 identify these, we implement an "interface", ColumnCompressor.java, which is part of service/src/java/org/apache/hive/service/cli. ColumnCompressorImpl.java is the implementor and this is called by any data type being compressed inside EnColumn.java. Based on the type passed, a different compressor is created and the compressed data is returned. 

In short, service/java/src/java/org/apache/hive/service/cli/EnColumn.java creates the columns we want to compress. From EnColumn.java, we call the ColumnCompressorImpl (inside SimbaCompressor) which sees the type being passed and calls the right compressor for this data type. 

Compression Methods
===================

So, how does this compression work? We have a description about [Integer compression](http://teak:8090/display/hyperspace/Integer+compression%3A+under+the+hood) on confluence. All the int and double compression methods work in a similar way. They keep accumulating bits into a long var and then empty it, byte by byte, when it is filled with 8 bytes. That step takes care of compressing the data. But, to help the decoder, we send a packed length array which helps the decoder know where one integer ends and the second starts. To do that, we always use packLengths() in CompressionUtils.  Note that we do not compress the lengths themselves, they just go in their binary form. 

For strings, we use prefix compression. Which of course, works only if you have a prefix in that whole block. As of now, we dont use any heuristics before hand to find out if a given set of rows actually have a unique prefix to them. 
