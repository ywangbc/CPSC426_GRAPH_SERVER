#ifndef STORAGELOG_H
#define STORAGELOG_h


#include <vector>
#include "mongoose.h"
#include "debug.h"
#include "graph_util.h"


class StorageLog {
  public:
    std::unordered_map<u64, std::unordered_set<u64>>& edge_list;
    int fd;
    uint32_t lastblock;
    uint32_t lastentry;
    
    StorageLog(std::unordered_map<u64, std::unordered_set<u64>>& edge_list_ref
        ,int fd, uint32_t lastblock, uint32_t lastentry):edge_list(edge_list_ref) {
      this->fd = fd;
      this->lastblock = lastblock;
      this->lastentry = lastentry;      
    }

    //check if there is enough space
    //return -1 if there is no enough space
    //return 0 if there is enough space 
    int checkspace(std::unordered_map<u64, std::unordered_set<u64>>edge_list);

    //store the current checkpoint
    //increase generation number in superblock
    //update lastblock and lastentry
    //return 0 on success
    //return -1 indicating no enough space for checkpoint
    int checkpoint(int fd);

    void update_tail(uint32_t& lastblock, uint32_t& lastentry);
    
    //write the next available entry on log
    //update lastblock and lastentry
    //return 0 on success
    //return -1 on log full
    int update_log(uint32_t method, u64 node1, u64 node2);

    void format(int fd);

    void play_log(int fd, uint32_t generation);

    void on_start_up(int fd);
};


#endif
