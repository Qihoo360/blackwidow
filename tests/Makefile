CXX=g++
LDFLAGS= -lpthread -lrt -lsnappy -lz -lbz2
CXXFLAGS=-O2 -std=c++11 -fno-builtin-memcmp -msse -msse4.2
PROCESSOR_NUMS= 24

.PHONY: clean all

ifndef BLACKWIDOW_PATH
  $(warning Warning: missing blackwidow path, using default)
  BLACKWIDOW_PATH=../
endif
BLACKWIDOW_INCLUDE_DIR=$(BLACKWIDOW_PATH)/include
BLACKWIDOW_LIBRARY=$(BLACKWIDOW_PATH)/lib/libblackwidow.a

ifndef ROCKSDB_PATH
  $(warning Warning: missing rocksdb path, using default)
	ROCKSDB_PATH=../deps/rocksdb
endif
ROCKSDB_INCLUDE_DIR=$(ROCKSDB_PATH)/include
ROCKSDB_LIBRARY=$(ROCKSDB_PATH)/librocksdb.a

ifndef GOOGLETEST_PATH
  $(warning Warning: missing googletest path, using default)
	GOOGLETEST_PATH=../deps/googletest/googletest/
endif
GOOGLETEST_INCLUDE_DIR=$(GOOGLETEST_PATH)/include
GOOGLETEST_LIBRARY=$(GOOGLETEST_PATH)/libgtest.a

CXXFLAGS+= -I$(BLACKWIDOW_PATH) -I$(BLACKWIDOW_INCLUDE_DIR) -I$(ROCKSDB_INCLUDE_DIR) -I$(GOOGLETEST_INCLUDE_DIR)

DEP_LIBS = $(BLACKWIDOW_LIBRARY) $(ROCKSDB_LIBRARY) $(GOOGLETEST_LIBRARY)
LDFLAGS := $(DEP_LIBS) $(LDFLAGS)

all: GOOGLETEST ROCKSDB lock_mgr gtest_strings
	mkdir db
	./gtest_strings
	rm -rf db

GOOGLETEST:
	cd $(GOOGLETEST_PATH); if [ ! -f ./Makefile ]; then  cmake .; fi; make;

ROCKSDB:
	$(AM_V_at)make -j $(PROCESSOR_NUMS) -C $(ROCKSDB_PATH)/ static_lib DISABLE_JEMALLOC=1

lock_mgr: lock_mgr.cc
	$(CXX) $(CXXFLAGS) $^ -o$@ $(LDFLAGS)

gtest_strings: gtest_strings.cc
	$(CXX) $(CXXFLAGS) $^ -o$@ $(LDFLAGS)

clean:
	find . -name "*.[oda]" -exec rm -f {} \;
	rm -rf ./lock_mgr ./gtest_strings
