CC=g++
RM=rm
CFLAGS= -c -Wall -g -pthread -std=c++11
LDFLAGS= -pthread 
GEN_SRC= ./lib/gen-cpp/InterNodeComm.cpp	\
				 ./lib/gen-cpp/master_slave_constants.cpp	\
				 ./lib/gen-cpp/master_slave_types.cpp
GEN_INC= -I./gen-cpp
INCS_DIRS= -I/usr/local/include/ -I/usr/local/include/thrift
LIB_DIRS= -L/usr/local/lib/
LIBS= -lthrift
DEPS=$(wildcard */*.h)
SOURCES=$(wildcard *.cpp) $(wildcard */*.cpp) 
CSOURCES=$(wildcard */*.c)
OBJECTS=$(SOURCES:.cpp=.o) $(CSOURCES:.c=.o)
EXECUTABLE=cs426_graph_server
FILES_TO_CLEAN = $(EXECUTABLE) $(OBJECTS)

all: $(SOURCES) $(CSOURCES) $(EXECUTABLE)
	    
$(EXECUTABLE): $(OBJECTS) 
	$(CC) $(LDFLAGS) $(OBJECTS) -o $@ ${GEN_INC} ${INCS_DIRS} ${GEN_SRC} ${LIBS_DIRS} ${LIBS}

.cpp.o:
	$(CC) $(CFLAGS) $< -o $@ 
clean:
	$(RM) $(FILES_TO_CLEAN)
