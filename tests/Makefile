CXX=g++
CXXFLAGS=-O2 -std=c++11 -fno-builtin-memcmp -msse -msse4.2

.PHONY: clean all

# Get processor numbers
dummy := $(shell ("$(CURDIR)/../detect_environment" "$(CURDIR)/make_config.mk"))
include make_config.mk
LDFLAGS = -lpthread -lrt $(ROCKSDB_LDFLAGS)

ifndef BLACKWIDOW_PATH
  $(warning Warning: missing blackwidow path, using default)
  BLACKWIDOW_PATH=..
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
	GOOGLETEST_PATH=../deps/googletest/googletest
endif
GOOGLETEST_INCLUDE_DIR=$(GOOGLETEST_PATH)/include
GOOGLETEST_LIBRARY=$(GOOGLETEST_PATH)/libgtest.a

CXXFLAGS+= -I$(BLACKWIDOW_PATH) -I$(BLACKWIDOW_INCLUDE_DIR) -I$(ROCKSDB_INCLUDE_DIR) -I$(GOOGLETEST_INCLUDE_DIR)

DEP_LIBS = $(BLACKWIDOW_LIBRARY) $(ROCKSDB_LIBRARY) $(GOOGLETEST_LIBRARY)
LDFLAGS := $(DEP_LIBS) $(LDFLAGS)

all: GOOGLETEST ROCKSDB lock_mgr gtest_strings gtest_hashes gtest_strings_filter

test: GOOGLETEST ROCKSDB lock_mgr gtest_strings gtest_hashes gtest_strings_filter
	@rm -rf db
	@mkdir db
	@./gtest_strings
	@./gtest_hashes
	@./gtest_strings_filter
	@rm -rf db

GOOGLETEST:
	@cd $(GOOGLETEST_PATH); if [ ! -f ./Makefile ]; then  cmake .; fi; make;

ROCKSDB:
	$(AM_V_at)make -j $(PROCESSOR_NUMS) -C $(ROCKSDB_PATH)/ static_lib

lock_mgr: lock_mgr.cc
	$(CXX) $(CXXFLAGS) $^ -o $@ $(LDFLAGS)

gtest_strings: gtest_strings.cc
	$(CXX) $(CXXFLAGS) $^ -o $@ $(LDFLAGS)

gtest_hashes: gtest_hashes.cc
	$(CXX) $(CXXFLAGS) $^ -o $@ $(LDFLAGS)

gtest_strings_filter: gtest_strings_filter.cc
	$(CXX) $(CXXFLAGS) $^ -o $@ $(LDFLAGS)

clean:
	find . -name "*.[oda]" -exec rm -f {} \;
	rm -f ./make_config.mk
	rm -rf ./lock_mgr ./gtest_strings ./gtest_hashes ./gtest_strings_filter
