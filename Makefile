CXX ?= c++
CXXFLAGS ?= -std=c++23 -Wall -Wextra -Wpedantic -Iinclude

BUILD_DIR := build
OBJ_DIR := $(BUILD_DIR)/obj
TARGET := $(BUILD_DIR)/kafka

SOURCES := $(shell find src -name '*.cpp')
OBJECTS := $(patsubst src/%.cpp,$(OBJ_DIR)/%.o,$(SOURCES))
DEPS := $(OBJECTS:.o=.d)

.PHONY: all clean run

all: $(TARGET)

$(TARGET): $(OBJECTS)
	@mkdir -p $(dir $@)
	$(CXX) $(CXXFLAGS) $^ -o $@

$(OBJ_DIR)/%.o: src/%.cpp
	@mkdir -p $(dir $@)
	$(CXX) $(CXXFLAGS) -MMD -MP -c $< -o $@

run: $(TARGET)
	./$(TARGET)

clean:
	rm -rf $(BUILD_DIR)

-include $(DEPS)
