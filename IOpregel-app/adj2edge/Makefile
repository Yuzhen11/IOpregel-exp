CCOMPILE=mpic++
PLATFORM=Linux-amd64-64
CPPFLAGS= -I$(HADOOP_HOME)/include -I ~/yanda_run/yuzhen_test/yd_test
LIB = -L$(HADOOP_HOME)/lib/native
LDFLAGS = -lhdfs -Wno-deprecated -O2

all: run

run: run.cpp
	$(CCOMPILE) -std=c++11 run.cpp $(CPPFLAGS) $(LIB) $(LDFLAGS)  -o run

clean:
	-rm run
