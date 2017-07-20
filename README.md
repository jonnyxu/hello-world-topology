# hello-world-topology
My first topology project for studying Apache Storm

1. Running Topology on local cluster:
	 mvn compile exec:java -Dexec.classpathScope=compile -Dexec.mainClass=com.jonny.storm.HelloWorldTopology

2. Deploying Topology to Storm cluster:
   ./storm/bin/storm jar hello-world-topology-1.0-SNAPSHOT.jar com.jonny.storm.HelloWorldTopology HelloWorldTopology

3. Stopping Storm Topology:
   storm kill HelloWorldTopology
