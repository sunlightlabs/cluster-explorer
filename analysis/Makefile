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
	gcc -O3 -shared -fPIC -Winline -o libcpartition.$(LIBTYPE) uthash.h cpartition.h cpartition.c lz4.h lz4.c speedmerge.h speedmerge.c

debug:
	gcc -O3 -shared -fPIC -Winline -g -o libcpartition.$(LIBTYPE) uthash.h cpartition.h cpartition.c lz4.h lz4.c speedmerge.h speedmerge.c
