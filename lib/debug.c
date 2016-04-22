#include "debug.h"

void print_graph(const std::unordered_map<u64, std::unordered_set<u64>>& edgelist) {
  printf("Graph has %lu nodes\n\n", edgelist.size());
  auto it = edgelist.begin();
  for(; it!=edgelist.end(); it++) {
    auto it2 = it->second.begin();
    printf("node %llu has %lu neighbours: \n", (unsigned long long)it->first, it->second.size());
    for( ;it2!=it->second.end(); it2++) {
      printf("%llu ", (unsigned long long)*it2);
    }
    printf("\n\n");
  }
}
