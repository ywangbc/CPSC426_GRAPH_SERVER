#include "storagelog.h"


int StorageLog::checkspace(std::unordered_map<u64, std::unordered_set<u64>>edge_list) {
  u64 available = (MAXBLOCKNUM - MAXLOGBLOCKNUM) * PAGESIZE;
  u64 totalsize = 0;
  totalsize += 8;
  for(auto it = edge_list.begin(); it != edge_list.end(); it++) {
    totalsize += 8;
    totalsize += 8;
    totalsize += it->second.size();
  }
  if(totalsize <= available) {
    return 0;
  }
  else {
    printf("No enough space for checkpoint, space required: %llu bytes, available: %llu bytes\n", (unsigned long long)totalsize, (unsigned long long)available);
    return -1;
  }
}

/*************************************
 *store the current checkpoint
 *increase generation number in superblock
 *update lastblock and lastentry
 *return 0 on success
 *return -1 indicating no enough space for checkpoint
 **************************************/
int StorageLog::checkpoint(int fd) {
  if(checkspace(edge_list) == -1) {
    return -1;
  }
  unsigned char* superblock_pt;
  ssize_t freeresult;
  superblock_pt = get_superblock(fd);
  Superblock* pt = (Superblock*) superblock_pt;
  if(isValid(superblock_pt)) {
    int storeresult = storegraph(pt->logsize, fd, edge_list);
    if(storeresult < 0) {
      printf("store checkpoint failed in checkpoint, too large\n");
      return -1;
    }
  }
  else {
    printf("superblock is invalid in checkpoint\n");
    exit(-1);
  }
  pt->generation++;
  update_checksum_super(superblock_pt);
  write_superblock(fd, superblock_pt); 
  lastblock = LOGSTARTBLOCK;
  lastentry = 0;

  freeresult = munmap(superblock_pt, PAGESIZE); 
  if(freeresult < 0) {
    printf("free superblock failed in checkpoint\n");
  }
  return 0;
}


void StorageLog::update_tail(uint32_t& lastblock, uint32_t& lastentry) {
  lastentry++;
  if(lastentry >= MAX_ENTRY_IN_BLOCK) {
    lastblock++;
    lastentry = 0;
  }
}


//write the next available entry on log
//update lastblock and lastentry
//return 0 on success
//return -1 on log full
int StorageLog::update_log(uint32_t method, u64 node1, u64 node2) {
  if(lastblock >= MAXBLOCKNUM) {
    printf("maximum block num reached in update_log\n");
    return -1;
  }
  uint32_t generation;
  unsigned char* superblock_pt;
  ssize_t freeresult;
  superblock_pt = get_superblock(fd);
  Superblock* pt = (Superblock*) superblock_pt;
  if(isValid(superblock_pt)) {
    generation = pt->generation;
  }
  else {
    printf("superblock is invalid in update_log\n");
    exit(-1);
  }
  freeresult = munmap(superblock_pt, PAGESIZE); 
  if(freeresult < 0) {
    printf("free superblock failed in update_logp\n");
  }

  Requestentry entry;
  entry.method = method;
  entry.node1 = node1;
  entry.node2 = node2;
  printf("Before updating log entry: \n");
  update_log_entry(fd, lastblock, lastentry, entry, generation);
  update_tail(lastblock, lastentry);
  return 0;
}



void StorageLog::format(int fd) {
  printf("Formatted\n");
  unsigned char* superblock_pt;
  int freeresult;
  //update superblock
  superblock_pt = get_superblock(fd);
  Superblock* pt = (Superblock*) superblock_pt;
  if(isValid(superblock_pt)) {
    pt->generation++;
    pt->logstart = LOGSTARTBLOCK; // which block
    pt->logsize = MAXLOGBLOCKNUM; // which block
  }
  else {
    pt->generation = 0;
    pt->logstart = LOGSTARTBLOCK; // which block
    pt->logsize = MAXLOGBLOCKNUM; // which block
  }
  update_checksum_super(superblock_pt);
  //printf("update_checksum_super finished\n");
  write_superblock(fd, superblock_pt);
  //printf("write_super finished\n");

  //clear check point
  edge_list.clear();
  //printf("edge_list cleared\n");
  assert(edge_list.empty());
  //printf("edge_list empty asserted\n");
  storegraph(pt->logsize, fd, edge_list);
  //printf("graph stored\n");
  //free in memory superblock
  freeresult = munmap(superblock_pt, PAGESIZE); 
  if(freeresult == -1) {
    printf("free superblock memory block failed in format\n");
  }
}


void StorageLog::play_log(int fd, uint32_t generation)
{ 
  uint32_t whichentry = 0;
  uint32_t whichblock = LOGSTARTBLOCK;                                                                
  unsigned char* blockp = NULL;
  Storageblockheader blockheader;                                                                     
  Requestentry request;
  for(;whichblock < MAXLOGBLOCKNUM; whichblock++) {                                                   
    blockp = get_block(fd, whichblock);                                                               
    //Invalid block, log all played
    if(!isValid(blockp)) {                                                                            
      break;                                                                                          
    }
    //Check if the generation number is correct
    blockheader = readblockheader(blockp);
    if(blockheader.generation != generation) {                                                       
      break;                                                                                          
    }
    whichentry = 0; 
    for(;whichentry < blockheader.numofentries; whichentry++) {                                      
      request = readblockentry(blockp, whichentry);
      std::vector<u64> args;      
      if(request.method == ADD_NODE) {
        args.push_back(request.node1);
        if(!(add_node(this->edge_list, args))) {
          printf("node %" PRIu64 " is already added when playlog, generation %" PRIu32 "\n", args[0], generation);
        }
      }
      else if(request.method == ADD_EDGE) {
        args.push_back(request.node1);
        args.push_back(request.node2);
        int result = add_edge(this->edge_list, args);
        if(result == 0) {
          printf("node %" PRIu64 ", node %" PRIu64 " edge is already added when playlog, generation %" PRIu32 "\n", args[0], args[1], generation);
        }
        else if(result == -1) {
          printf("node %" PRIu64 ", node %" PRIu64 " edge are equal or do not exist when palying log, generation %" PRIu32 "\n", args[0], args[1], generation);
        }

      }
      else if(request.method == REMOVE_NODE) {
        args.push_back(request.node1);
        if(!(remove_node(this->edge_list, args))) {
          printf("node %" PRIu64 " remove does exist not when playlog, generation %" PRIu32 "\n", args[0], generation);
        }
      }
      else if(request.method == REMOVE_EDGE) {
        args.push_back(request.node1);
        args.push_back(request.node2);
        int result = remove_edge(this->edge_list, args);
        if(result == 0) {
          printf("node %" PRIu64 ", node %" PRIu64 " edge does not exist playlog, generation %" PRIu32 "\n", args[0], args[1], generation);
        }
      }      
      else {
        printf("Invalid method when palying log: %" PRIu32 "\n", request.method);
      }
    }                                                                                                 
  }
  this->lastblock = whichblock-1;
  this->lastentry = whichentry;  
  if(lastblock == 0) {
    lastblock = 1;
  }
  else if(lastentry > MAX_ENTRY_IN_BLOCK) {
    printf("Enough entries in a block, move to next block\n");
    printf("Now last block: %llu, last entry: %llu\n", (unsigned long long)lastblock, (unsigned long long)lastentry);
    lastblock++;
    lastentry = 0;
  }
  printf("Exiting play_log, last block is: %llu, last entry: %llu \n", (unsigned long long)lastblock, (unsigned long long)lastentry);
}   

void StorageLog::on_start_up(int fd) {
  unsigned char* superblock_pt;
  ssize_t freeresult;
  superblock_pt = get_superblock(fd);
  Superblock* pt = (Superblock*) superblock_pt;
  if(isValid(superblock_pt)) {
    edge_list = readgraph(pt->logsize, fd);
    play_log(fd, pt->generation);
    print_graph(edge_list);
  }
  else {
    printf("superblock is invalid on startup\n");
    exit(-1);
  }
  printf("one_start_up, edge_list is of size: %lu\n", edge_list.size());
  freeresult = munmap(superblock_pt, PAGESIZE); 
  if(freeresult < 0) {
    printf("free superblock failed in on_start_up\n");
  }
}








