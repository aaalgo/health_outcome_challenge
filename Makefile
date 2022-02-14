CXXFLAGS += -O3 -std=c++1y
LDFLAGS +=
LDLIBS +=  -lglog

all:


docker:
	docker build -t aaalgo/cms .
