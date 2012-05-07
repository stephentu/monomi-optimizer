INC_PATH=/usr/local/Cellar/postgresql/9.1.3/include/server

all:
	$(CC) -c dummy_udfs.c -I$(INC_PATH)
	$(CC) -bundle -flat_namespace -undefined suppress -o dummy_udfs.so dummy_udfs.o

clean:
	rm -rf *.o *.so
