CCOMPILE=mpic++
CPPFLAGS= -I$(HADOOP_HOME)/include -I$(JAVA_HOME)/include -I$(JAVA_HOME)/include/linux -I /data/yuzhen/yuzhen/pregelplus/  -Wno-deprecated -O2
LIB = -L$(HADOOP_HOME)/lib/native
LDFLAGS = -lhdfs

all: Pagerank

Pagerank: Pagerank.cpp
	$(CCOMPILE)  Pagerank.cpp $(CPPFLAGS) $(LIB) $(LDFLAGS)  -o Pagerank

clean:
	-rm Pagerank
