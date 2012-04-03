#include "stdafx.hpp"

#include "non_blocking_unordered_map.hpp"

#include <string>

// force instantiation

// primitive (no-destructor) types
template struct non_blocking_unordered_map<int, int>;

// complex (class) types
template struct non_blocking_unordered_map<std::string, std::string>;

