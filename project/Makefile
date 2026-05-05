CC = g++
STD = c++20
CFLAGS = -Wall -Wextra -Werror -pedantic -g -std=$(STD) -Wno-unused-variable -Wno-unused-parameter
LDFLAGS = -lstdc++ -lpthread -lstdc++fs
BINARY=jsq

all: main.cxx
	$(CC) main.cxx $(CFLAGS) $(LDFLAGS)  -o $(BINARY)

run: all
	./$(BINARY)
clean:
	rm -f jsq
