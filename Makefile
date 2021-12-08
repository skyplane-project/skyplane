APPNAME := skylark-dataplane
CXX := g++
CXXFLAGS := -g -std=c++11
INCLUDES_PATH := -I./src/
SRC_PATH := ./src
OBJ_FILES := main.o skylark.o

all: $(APPNAME)

$(APPNAME): $(OBJ_FILES)
	$(CXX) $(CXXFLAGS) $^ -o $@
	g++ -fpermissive $(SRC_PATH)/gfg-server.cpp -o server 
	g++ -fpermissive $(SRC_PATH)/gfg-client.cpp -o client
	rm -f $(OBJ_FILES)

main.o: $(SRC_PATH)/main.cpp
	$(CXX) $(CXXFLAGS) -c $(INCLUDES_PATH) $< -o $@

skylark.o: $(SRC_PATH)/skylark.cpp
	$(CXX) $(CXXFLAGS) -c $(INCLUDES_PATH) $< -o $@

clean:
	rm -f $(OBJ_FILES)
	rm -f $(APPNAME)
	rm -f server
	rm -f client
