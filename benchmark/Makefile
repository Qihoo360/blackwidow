CXX=g++
LDFLAGS= -lpthread -lrt -lsnappy -lz -lbz2
CXXFLAGS=-O2 -std=c++11 -fno-builtin-memcmp -msse -msse4.2

.PHONY: clean all

all: blackwidow_bench

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

ifndef SLASH_PATH
  $(warning Warning: missing slash path, using default)
	SLASH_PATH=../deps/slash
endif
SLASH_INCLUDE_DIR=$(SLASH_PATH)
SLASH_LIBRARY=$(SLASH_PATH)/slash/lib/libslash.a

ifndef GOOGLETEST_PATH
  $(warning Warning: missing googletest path, using default)
	GOOGLETEST_PATH=../deps/googletest/googletest
endif
GOOGLETEST_INCLUDE_DIR=$(GOOGLETEST_PATH)/include
GOOGLETEST_LIBRARY=$(GOOGLETEST_PATH)/libgtest.a

ifndef BENCHMARK_PATH
  $(warning Warning: missing benchmark path, using default)
	BENCHMARK_PATH=../deps/benchmark
endif
BENCHMARK_INCLUDE_DIR=$(BENCHMARK_PATH)/include


CXXFLAGS+= -I$(BLACKWIDOW_PATH) -I$(BLACKWIDOW_INCLUDE_DIR) -I$(ROCKSDB_INCLUDE_DIR) -I$(SLASH_INCLUDE_DIR) -I$(GOOGLETEST_INCLUDE_DIR) -I$(BENCHMARK_INCLUDE_DIR) $(OPT)

DEP_LIBS = $(BLACKWIDOW_LIBRARY) $(ROCKSDB_LIBRARY) $(SLASH_LIBRARY) $(GOOGLETEST_LIBRARY)
LDFLAGS := $(DEP_LIBS) $(LDFLAGS)

OBJECTS= GOOGLETEST ROCKSDB SLASH BENCHMARK blackwidow_bench


blackwidow_bench: blackwidow_bench.cc
	@rm -rf db
	@mkdir -p db/keys db/strings db/hashes db/hash_meta db/sets db/hyperloglog db/list_meta db/lists db/zsets
	$(CXX) $(CXXFLAGS) $^ -o $@ $(LDFLAGS)
	@./blackwidow_bench
	@rm -rf db

clean:
	find . -name "*.[oda]" -exec rm -f {} \;
	rm -f ./make_config.mk
	rm -rf ./db/
	rm -rf ./blackwidow_bench
