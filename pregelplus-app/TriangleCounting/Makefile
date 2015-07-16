COMPILE=mpic++
CPPFLAGS= -I$(HADOOP_HOME)/include -I$(JAVA_HOME)/include -I$(JAVA_HOME)/include/linux -I ~/yuzhen/pregelplus/  -Wno-deprecated -O2
LIB = -L$(HADOOP_HOME)/lib/native
LDFLAGS = -lhdfs

all: tc

tc: tc.cpp
	$(COMPILE)  tc.cpp $(CPPFLAGS) $(LIB) $(LDFLAGS)  -o tc

clean:
	-rm tc
