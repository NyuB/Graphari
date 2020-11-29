# Graphari
## Graph algorithms implemented with Apache Giraph
### INSA Toulouse SDBD Project
### Community detection algorithms applied to large graphs
### SDBD B2-1

### Configuration

Prototyped on the following configuration

- OS
  + Ubuntu 12.0.4 (osboxes VI -> https://www.osboxes.org/ubuntu/ )
- JDK
  + 1.8 (specified in pom.xml)
- Hadoop : 
  + 0.20
- Maven
  + 3.6

### Installation

####Follow the Apache Giraph QuickStart guide 
https://giraph.apache.org/quick_start.html
####With the following warnings/modifications :

- Java
  + Install jdk 8 or higher
- Maven
 + the versions anterior to 3.2.3 still use http to download from maven central, which is not supported since 15/01/2020 (see -> https://support.sonatype.com/hc/en-us/articles/360041287334 )
 + install 3.2.3 or higher (tested with 3.6)
 + OR apply the following modification to your maven user settings (most voted response) : https://stackoverflow.com/questions/25393298/what-is-the-correct-way-of-forcing-maven-to-use-https-for-maven-central/59784045#59784045
- Giraph : 
  + If you are compiling this project you do not need to clone Giraph as indicated in the repo.
  + You can still clone and build Giraph origin repo (in particular to access giraph-examples as this project only includes giraph-core)
  + When cloning, take care of checking out to a release branch before build
  
### Using Giraph:
#### Command
Let us examine the Apache QuickStart example in detail :

```
$HADOOP_HOME/bin/hadoop jar $GIRAPH_HOME/giraph-examples/target/giraph-examples-1.2.0-SNAPSHOT-for-hadoop-0.20.203.0-jar-with-dependencies.jar org.apache.giraph.GiraphRunner org.apache.giraph.examples.SimpleShortestPathsComputation -vif org.apache.giraph.io.formats.JsonLongDoubleFloatDoubleVertexInputFormat -vip /user/hduser/input/tiny_graph.txt -vof org.apache.giraph.io.formats.IdWithValueTextOutputFormat -op /user/hduser/output/shortestpaths -w 1
```

```
#skeleton
hadoop jar my_giraph_project_jar giraph_runner my_giraph_project_computation_class -vif graphFormat -vip graphHdfsPath -of resultFormat -op resultHdfsPath
```

+ We are 'only' executing Hadoop -> Giraph consists Java Libraries built on top of hadoop MapReduce and HDFS system offering possibilities to run as Hadoop Jobs
+ We are targeting a jar, containing at least giraph-core, plus our classes and additionnal libraries. The extension correspond to the maven denomination of a project output archived with all libraries.
+ We are using Giraph Runner as the target class for hadoop in this jar, then the following arguments are passed to this Runner.
+ We indicate the classes of our Computation implementation, our logic core.
+ option -vif (Vertex Input Format) designates a class in the jar and indicates how our input graph is encoded. This class should extend/implement one of the inputformat classes/interfaces provided by Giraph-core accordingly to our Vertexes nature
+ option -vip (Vertex Input Path) indicates where(in HDFS) our input graph is located/distributed
+ option -vof (Vertex Output Format) same as vif but for convrting vertextes to output result file.
+ options -eif and -eip are availables with the same meanings as vif and vip applied to edges
+ option -op (Output Path) specify the directory(in HDFS) where our computation infos and results will be stored
+ option -mc is available to specify a MasterCompute (orchestrator) class to use 
+ option -w indique le nombre de workers/thread alloué à cette computation (hadoop option)

Note on the quickstart example : the given description isn't accurate, as this job computes the shortest paths from node 1 instead of the first node appearing in the graph input file.


### Algorithms and examples available in this project:

- reversion : classes to perform graph reversion
	+ vertex format : Long,Long,Double nodes 
	+ computation class : any
	+ master-compute : insa.sdbd.community.reversion.GraphReversionMasterCompute
