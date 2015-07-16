CCOMPILE=mpic++
CPPFLAGS= -I$(HADOOP_HOME)/include -I$(JAVA_HOME)/include -I$(JAVA_HOME)/include/linux -I /data/yuzhen/yuzhen/pregelplus  -Wno-deprecated -O2
LIB = -L$(HADOOP_HOME)/lib/native
LDFLAGS = -lhdfs

all: splitFile

splitFile: splitFile.cpp
	$(CCOMPILE) splitFile.cpp $(CPPFLAGS) $(LIB) $(LDFLAGS)  -o splitFile

clean:
	-rm splitFile

