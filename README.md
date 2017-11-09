# Genomics_EpiQuant_LASSO

The Distributed ADMM algorithm to solve Linear LASSO problem implemented in Scala/Spark.

## Introduction

This 

## Usage

### Prerequisites

   * [Apache Spark](https://spark.apache.org) - Release 2.x
   * [Maven](https://maven.apache.org/) - Dependency Management
   * [HDFS](http://hadoop.apache.org) - Distributed file system

### Installing

* Git clone the repository

`git clone https://github.com/ncsa/Genomics_EpiQuant_LASSO`

* Build with [Maven](https://maven.apache.org/) - Dependency Management

`cd Genomics_EpiQuant_LASSO`
`mvn -Dmaven.test.skip=true install`

## Running the tests

* The resources folder contain some small test datasets to run the prototype.
`spark-submit --master local --class epiquant.Main target/lasso-spark-1.0-SNAPSHOT.jar [path/to/test/file] [number of phenotypes]`

* Currently only supports analyzing 1 phenotype at a time

## Acknowledgments

This research is part of a project funded by the NSF's Center for  Computational Biology and Genomic Medicine (CCBGM) at UIUC.

