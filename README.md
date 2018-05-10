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
	
	*`spark-submit --master local --class epiquant.Main target/lasso-spark-1.0-SNAPSHOT.jar [path/to/Genotyp] [maxiter] [abstol] [reltol] [lambda]`

* Currently only supports analyzing 1 phenotype at a time

### Validation results
	*`spark-submit --master local --class epiquant.Main target/lasso-spark-1.0-SNAPSHOT.jar resources/Genotypes/randomMatrix.tsv resources/Phenotypes/randomY.tsv 100 1e-05 1e-05 0.5`

	```R
	library(glmnet)
	set.seed(1)
	n=100
	p=20
	m=5
	y = matrix(c(runif(m), rep(0, p-m)))
	A = matrix(rnorm(n*p, mean=1.2, sd=2), n, p)
	b = 5 + A %*% y + rnorm(n)

	fit = glmnet(A, b, intercept= FALSE, lambda = 0.128)
	out_glmnet = coef(fit, s=0.128, exact=TRUE)

	out_epiquant = #results from epiquant

	data.frame(epiquant=as.numeric(res), glmnet = as.numeric(out_glmnet[-1]))	
	```

	```
	  epiquant     glmnet
1   0.640177418 0.63032933
2   0.263225825 0.26043376
3   0.809289780 0.81464664
4   1.027031377 1.02288164
5   0.283272421 0.27490206
6   0.306545577 0.28805917
7   0.339843474 0.32535034
8   0.320162455 0.31513002
9   0.172334969 0.16041786
10  0.167934333 0.14284565
11  0.204824611 0.17795188
12  0.251576765 0.24558217
13 -0.009142948 0.00000000
14  0.033703259 0.02953242
15  0.286475924 0.26827899
16  0.125916431 0.11806301
17  0.154255841 0.15272102
18  0.259960659 0.26040785
19  0.252801284 0.24396617
20  0.277613182 0.25075847
	```

## Acknowledgments

This research is part of a project funded by the NSF's Center for  Computational Biology and Genomic Medicine (CCBGM) at UIUC.

