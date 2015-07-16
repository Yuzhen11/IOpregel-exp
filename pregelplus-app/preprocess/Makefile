CCOMPILE=mpic++
CPPFLAGS= -I$(HADOOP_HOME)/include -I$(JAVA_HOME)/include -I$(JAVA_HOME)/include/linux -I ~/yuzhen/pregelplus/  -Wno-deprecated -O2
LIB = -L$(HADOOP_HOME)/lib/native
LDFLAGS = -lhdfs

all: preprocessing

preprocessing: preprocessing.cpp
	$(CCOMPILE)  preprocessing.cpp $(CPPFLAGS) $(LIB) $(LDFLAGS)  -o preprocessing

clean:
	-rm preprocessing
