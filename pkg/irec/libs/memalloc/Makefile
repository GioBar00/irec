SHELL := /bin/bash

all: sbrk.o libptmalloc3.a michelfralloc.o

sbrk.o: sbrk.h sbrk.c
	gcc -c sbrk.c -o sbrk.o -fPIC
	ls -al
	cp -rpv sbrk.o $(PREFIX)

michelfralloc.o: michelfralloc.h michelfralloc.c
	gcc -c michelfralloc.c -o michelfralloc.o -fPIC
	cp -rpv michelfralloc.o $(PREFIX)

libptmalloc3.a:
	pushd ptmalloc3 && make libptmalloc3.a && popd
	cp ptmalloc3/libptmalloc3.a ./
	cp -rpv libptmalloc3.a $(PREFIX)

clean:
	pushd ptmalloc3 && make clean && popd
	rm -f libptmalloc3.a; rm -f libmichelfralloc.a

.PHONY: clean