CXX = g++-11
CXXFLAGS = -std=c++17
LIBS = -lboost_system -lpthread

TARGET = leitorwebsocket
SRCS = leitorwebsocket.cpp
OBJS = $(SRCS:.cpp=.o)

$(TARGET): $(OBJS)
	$(CXX) $(OBJS) -o $(TARGET) $(LIBS)

%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c $< -o $@

clean:
	rm -f $(OBJS) $(TARGET)

.PHONY: clean
