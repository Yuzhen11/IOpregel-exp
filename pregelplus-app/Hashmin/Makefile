CCOMPILE=mpic++
CPPFLAGS= -I$(HADOOP_HOME)/include -I$(JAVA_HOME)/include -I$(JAVA_HOME)/include/linux -I /data/yuzhen/yuzhen/pregelplus/  -Wno-deprecated -O2
LIB = -L$(HADOOP_HOME)/lib/native
LDFLAGS = -lhdfs

all: Hashmin

Hashmin: Hashmin.cpp
	$(CCOMPILE)  Hashmin.cpp $(CPPFLAGS) $(LIB) $(LDFLAGS)  -o Hashmin

clean:
	-rm Hashmin
