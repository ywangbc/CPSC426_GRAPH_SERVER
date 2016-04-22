CC=g++
RM=rm
CFLAGS= -c -Wall -g -std=c++11
LDFLAGS=
DEPS=$(wildcard */*.h)
SOURCES=$(wildcard *.cpp) $(wildcard */*.cpp) 
CSOURCES=$(wildcard */*.c)
OBJECTS=$(SOURCES:.cpp=.o) $(CSOURCES:.c=.o)
EXECUTABLE=cs426_graph_server
FILES_TO_CLEAN = $(EXECUTABLE) $(OBJECTS)

all: $(SOURCES) $(CSOURCES) $(EXECUTABLE)
	    
$(EXECUTABLE): $(OBJECTS) 
	$(CC) $(LDFLAGS) $(OBJECTS) -o $@

.cpp.o:
	$(CC) $(CFLAGS) $< -o $@
clean:
	$(RM) $(FILES_TO_CLEAN)
