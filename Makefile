CXX ?= c++
CXXFLAGS ?= -std=c++23 -Wall -Wextra -Wpedantic -Iinclude

BUILD_DIR := build
OBJ_DIR := $(BUILD_DIR)/obj
TARGET := $(BUILD_DIR)/kafka

SOURCES := $(shell find src -name '*.cpp')
OBJECTS := $(patsubst src/%.cpp,$(OBJ_DIR)/%.o,$(SOURCES))
DEPS := $(OBJECTS:.o=.d)
TEST_SOURCES := $(filter-out src/main.cpp,$(SOURCES)) $(shell find tests -name '*.cpp' 2>/dev/null)
TEST_OBJECTS := $(patsubst src/%.cpp,$(OBJ_DIR)/%.o,$(filter src/%,$(TEST_SOURCES))) \
	$(patsubst tests/%.cpp,$(OBJ_DIR)/tests/%.o,$(filter tests/%,$(TEST_SOURCES)))
TEST_TARGET := $(BUILD_DIR)/broker_api_tests

.PHONY: all clean run test

all: $(TARGET)

$(TARGET): $(OBJECTS)
	@mkdir -p $(dir $@)
	$(CXX) $(CXXFLAGS) $^ -o $@

$(OBJ_DIR)/%.o: src/%.cpp
	@mkdir -p $(dir $@)
	$(CXX) $(CXXFLAGS) -MMD -MP -c $< -o $@

$(OBJ_DIR)/tests/%.o: tests/%.cpp
	@mkdir -p $(dir $@)
	$(CXX) $(CXXFLAGS) -MMD -MP -c $< -o $@

run: $(TARGET)
	./$(TARGET)

$(TEST_TARGET): $(TEST_OBJECTS)
	@mkdir -p $(dir $@)
	$(CXX) $(CXXFLAGS) $^ -o $@

test: $(TEST_TARGET)
	./$(TEST_TARGET)

clean:
	rm -rf $(BUILD_DIR)

-include $(DEPS)
