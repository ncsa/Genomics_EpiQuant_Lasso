# Genomics_EpiQuant_LASSO

The Distributed ADMM algorithm to solve Linear LASSO problem implemented in Scala/Spark.

## Introduction

This is a preprocessing step in the EpiQuant data analytic pipeline. The LASSO linear regression effectively eliminates independent x variables that does not significantly contribute to the response variable, so that the search space is reduced in the main stepwise model selection.

## Usage

### Prerequisites

   * [Apache Spark](https://spark.apache.org) - Release 2.x
   * [Maven](https://maven.apache.org/) - Dependency Management
   * [HDFS](http://hadoop.apache.org) - Distributed file system

### Installing

* Git clone the repository

	*`git clone https://github.com/ncsa/Genomics_EpiQuant_LASSO`

* Build with Maven

	*`cd Genomics_EpiQuant_LASSO`
	*`mvn -Dmaven.test.skip=true install`

## Running the tests

* The resources folder contain some small, already formatted datafiles to run and test the prototype. 
	
	*`spark-submit --master local --class epiquant.Main target/lasso-spark-1.0-SNAPSHOT.jar resources/Genotypes/randomMatrix.tsv resources/Phenotypes/randomY.tsv [maxiter] [abstol] [reltol] [lambda]`

* Currently only supports analyzing 1 phenotype at a time

## Acknowledgments

This research is part of a project funded by the NSF's Center for  Computational Biology and Genomic Medicine (CCBGM) at UIUC.

