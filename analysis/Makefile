UNAME := $(shell uname)

ifeq ($(UNAME), Linux)
LIBTYPE=so
endif
ifeq ($(UNAME), Darwin)
LIBTYPE=dylib
endif

all: clean build

clean:
	rm -f *.so *.dylib *.gch

build:
	gcc -O3 -shared -fPIC -Winline -o libcpartition.$(LIBTYPE) uthash.h cpartition.h cpartition.c

debug:
	gcc -O3 -shared -fPIC -g -o libcpartition.$(LIBTYPE) uthash.h cpartition.h cpartition.c
