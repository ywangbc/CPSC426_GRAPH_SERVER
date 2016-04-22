#include "disk_lib.h"

//read the file from certain offset and set its read index
ssize_t read_set(int fd, unsigned char* buf, size_t count, off_t offset) {
  ssize_t readnum;
  lseek(fd, offset, SEEK_SET);
  //printf("lseek errno is: %s\n", strerror(errno));
  readnum = read(fd, buf, count); //return -1 for error
  if(readnum == -1) {
    printf("read failed at file %d, offset %jd for %zu bytes with error: %s\n", fd, (intmax_t)offset, count, strerror(errno));
  }
  if((long long)readnum != (long long)count) {
    printf("read incomplete at file %d, offset %jd for %zu bytes\n", fd, (intmax_t)offset, count);
  }
  return readnum;
}

//write the file from certain index and set its rw offset
ssize_t write_set(int fd, unsigned char* buf, size_t count, off_t offset) {
  ssize_t writenum;
  lseek(fd, offset, SEEK_SET);
  //printf("lseek errno is: %s\n", strerror(errno));
  writenum = write(fd, buf, count); //return -1 for error
  if(writenum == -1) {
    printf("write failed at file %d, offset %jd for %zu bytes with error: %s\n", fd, (intmax_t)offset, count, strerror(errno));
  }
  if((long long)writenum != (long long)count) {
    printf("write incomplete at file %d, offset %jd for %zu bytes\n", fd, (intmax_t)offset, count);
  }
  return writenum;
}

//Return a pointer to a page of memory, which contains a super block at the start
unsigned char* get_superblock(int fd) { //fd is file discriptor of disk
  //printf("In get_superblock, allocating %llu bytes of superblock\n", PAGESIZE);
  unsigned char* buf = (unsigned char*)mmap(NULL, PAGESIZE, PROT_READ | PROT_WRITE, MAP_ANONYMOUS | MAP_PRIVATE, -1, 0); 
  //printf("Allocated buf is: %p\n", buf);
  read_set(fd, buf, PAGESIZE, 0);
  return buf; 
}

//Write memory pointed by buf (which must be a superblock of PAGESIZE) into file fd
//do not unmap the memory for superblock
ssize_t write_superblock(int fd, unsigned char* buf) {
  ssize_t writenum;
  //printf("In write_superblock\n");
  writenum = write_set(fd, buf, PAGESIZE, 0);
  return writenum;
}

//Return a pointer to a page of memory, which contains #whichblock'th block from 0
unsigned char* get_block(int fd, uint32_t whichblock) {
  //printf("In get_block\n");
  unsigned char* buf = (unsigned char*)mmap(NULL, PAGESIZE, PROT_READ | PROT_WRITE, MAP_ANONYMOUS | MAP_PRIVATE, -1, 0); 
  //printf("Allocated buf is: %p\n", buf);
  read_set(fd, buf, PAGESIZE, whichblock*PAGESIZE);
  return buf; 
}

//Write memory pointed by buf (which must be a storage block of PAGESIZE) into file fd's whichblock
//do not unmap the memory for block
ssize_t write_block(int fd, unsigned char* buf, uint32_t whichblock) {
  ssize_t writenum;
  //printf("In write_block\n");
  writenum = write_set(fd, buf, PAGESIZE, whichblock*PAGESIZE);
  return writenum;
}

//check the validity of a block, the block must be of size PAGESIZE
uint32_t isValid(unsigned char* block) {
  uint32_t bytepos;
  uint32_t result = 0;
  for(bytepos = 0; bytepos < PAGESIZE; bytepos+=4) {
    uint32_t word = *(uint32_t*)(block+bytepos);
    result ^= word;
  }
  return result == 0; //To be valid xor of all words must be 0
}

//return the struct representing the storage block header
Storageblockheader readblockheader(unsigned char* block) {
  Storageblockheader header;
  header.generation = *(uint32_t*)block;
  header.numofentries = *(uint32_t*)(block+4);
  header.checksum = *(uint32_t*)(block+8);
  return header;
}

//read whichentry'th entry into a Requestentry struct
Requestentry readblockentry(unsigned char* block, uint32_t whichentry) {
  Requestentry entry;
  block += BLOCKHEADERSIZE;
  block += ENTRYSIZE*whichentry;
  entry.method = *(uint32_t*)block;
  block += 4;
  entry.node1 = *(uint64_t*)block;
  block += 8;
  entry.node2 = *(uint64_t*)block;
  return entry;
}

//store Storageblockheader object into a block
//notice this only write to the inmemory copy of header
void writeblockheader(unsigned char* block, const Storageblockheader& header) {
  *(uint32_t*)block = header.generation;
  block += 4;
  *(uint32_t*)block = header.numofentries;
  block += 4;
  *(uint32_t*)block = header.checksum;
}

//store whichentry'th Requestentry into a block
//notice this only write to the inmemory copy of entry
void writeblockentry(unsigned char* block, uint32_t whichentry, const Requestentry& entry) {
  block += BLOCKHEADERSIZE;
  block += ENTRYSIZE*whichentry;
  
  *(uint32_t*)block = entry.method;
  block += 4;
  *(uint64_t*)block = entry.node1;
  block += 8;
  *(uint64_t*)block = entry.node2;
}

//calculate and update checksum of a super block (in memory block)
void update_checksum_super(unsigned char* superblock) {
  uint32_t checksum = calcchecksum_super(superblock);
  *(uint32_t*)(superblock+4) = checksum;
}

//calculate and update checksum of a super block (in memory block)
void update_checksum_storage(unsigned char* block) {
  uint32_t checksum = calcchecksum_storage(block);
  *(uint32_t*)(block+8) = checksum;
}

//update a log entry, and the header of its block, then write to disk
//return 0 on success
//return -1 on block exceeds MAXBLOCKNUM, or entry exceeds MAX_ENTRY_IN_BLOCK
int update_log_entry(int fd, uint32_t whichblock, uint32_t whichentry, const Requestentry& entry, uint32_t generation) {
  if(whichblock >= MAXBLOCKNUM) {
    printf("whichblock %u too large in update_log_entry\n", (unsigned int)whichblock);
    return -1;
  }
  if(whichentry >= MAX_ENTRY_IN_BLOCK) {
    printf("whichentry %u too large in update_log_entry\n", (unsigned int)whichentry);
    return -1;
  }

  printf("Method is: %u\n", (unsigned int)entry.method);
  printf("Node 1 is: %u\n", (unsigned int)entry.node1);
  printf("Node 2 is: %u\n", (unsigned int)entry.node2);

  printf("Written to %u block, %u entry\n", (unsigned int)whichblock, (unsigned int)whichentry);

  unsigned char* block;
  block = get_block(fd, whichblock);
  //read out header and entry
  Storageblockheader header = readblockheader(block);
  if(whichentry == 0) {
    header.numofentries = 1;
  }
  else {
    header.numofentries++;
  }
  header.generation = generation;
  printf("Block header.numerofentries: %u\n", header.numofentries);
  printf("Block header.generation: %u\n", header.generation);
  //put result back to block
  writeblockheader(block, header);
  writeblockentry(block, whichentry, entry);
  update_checksum_storage(block);
  write_block(fd, block, whichblock);
  return 0;
}


//Calculate the checksum of a super block
uint32_t calcchecksum_super(unsigned char* superblock) {
  uint32_t bytepos;
  uint32_t result = 0;
  for(bytepos = 0; bytepos < PAGESIZE; bytepos+=4) {
    if(bytepos == 4) { //skip the checksum entry
      continue;
    }
    uint32_t word = *(uint32_t*)(superblock+bytepos);
    result ^= word;
  }
  return result;
}

//Calculate the checksum of a storage block
uint32_t calcchecksum_storage(unsigned char* block) {
  uint32_t bytepos;
  uint32_t result = 0;
  for(bytepos = 0; bytepos < PAGESIZE; bytepos+=4) {
    if(bytepos == 8) { //skip the checksum entry
      continue;
    }
    uint32_t word = *(uint32_t*)(block+bytepos);
    result ^= word;
  }
  return result;
}

//write content to disk at blockstart'th block and offsetstart, using buf as buffer
//need to allocate PAGESIZE for buf before use, and call buffered_write_finish after use
void buffered_write(int fd, uint32_t& blockstart, uint32_t& offsetstart, unsigned char* buf, uint64_t content){
  *(uint64_t*)(buf+offsetstart) = content;
  offsetstart += 8;
  if(offsetstart >= PAGESIZE) {//one block finished, write to disk, update block pos and offset
    write_block(fd, buf, blockstart);
    offsetstart = 0;
    blockstart++;
  }
}
//write last incomplete block on disk, at blockstart'th block
//free the memory buffer
void buffered_write_finish(int fd, uint32_t blockstart, unsigned char* buf) {
  ssize_t freenum;
  if(blockstart < MAXBLOCKNUM) {
    write_block(fd, buf, blockstart);
  }
  else {
    printf("Reached end of disk, which not write block in buffered_write_finish\n");
  }
  freenum = munmap(buf, PAGESIZE);
  if(freenum < 0) {
    printf("Free block failed for fd %d in buffered_write_finish\n", fd);
  }
}

//Store the graph on disk, from checkstart'th block
//Start from check start, stored in superblock
//return -1 if store failed (graph too large)
int storegraph(uint32_t checkstart, int fd,
  std::unordered_map<u64, std::unordered_set<u64>>& edgelist) {
  //printf("In store graph\n");
  //printf("checkstart is: %u\n", (unsigned int)checkstart);
  //printf("fd is: %d\n", fd);

  uint32_t blockstart = checkstart; //The block you are writing on
  uint32_t offsetstart = 0; //Offset in block you are writing on
  std::unordered_map<u64, std::unordered_set<u64>>::iterator mapit = edgelist.begin();
  std::unordered_set<u64>::iterator setit; 
  //printf("fd is: %d\n", fd);
  //printf("size of edgelist is: %lu\n", edgelist.size());

  unsigned char* buf = (unsigned char*)mmap(NULL, PAGESIZE, PROT_READ | PROT_WRITE, MAP_ANONYMOUS | MAP_PRIVATE, -1, 0); 

  //printf("Allocated buf is: %p\n", buf);
  //printf("Error of mmap is : %s\n", strerror(errno));
  buffered_write(fd, blockstart, offsetstart, buf, edgelist.size()); //first write how many nodes
  //printf("buffered write is done\n");
  for(; mapit!=edgelist.end(); mapit++) {
    if(blockstart >= MAXBLOCKNUM) {
      printf("graph too large, writing on block %u\n", (unsigned int)blockstart);
      return -1;
    }
    buffered_write(fd, blockstart, offsetstart, buf, mapit->first); //write source node
    buffered_write(fd, blockstart, offsetstart, buf, mapit->second.size()); //write number of neighbours
    //Write the the list of neighbours
    for(setit = mapit->second.begin(); setit != mapit->second.end(); setit++) {
      if(blockstart >= MAXBLOCKNUM) {
        printf("graph too large, writing on block %u\n", (unsigned int)blockstart);
        return -1;
      }
      buffered_write(fd, blockstart, offsetstart, buf, *setit); 
    }
  }
  //printf("Before buffered write finish\n");
  //clean up, write rest buffer and free memory
  buffered_write_finish(fd, blockstart, buf);
  //printf("After buffered write finish\n");
  return 0;
}

//read from disk at blockstart'th block, at offsetstart in block, buffered by buf
u64 buffered_read(int fd, uint32_t& blockstart, uint32_t& offsetstart, unsigned char*& buf) {
  ssize_t freenum;
  u64 content;
  if(offsetstart == 0) { //if read from a new block
    buf = get_block(fd, blockstart); //get next block from disk
    content = *(u64*)(buf+offsetstart);
  }
  else {
    assert(buf != NULL);
    content = *(u64*)(buf+offsetstart);
  }
  offsetstart += 8;
  if(offsetstart == PAGESIZE) {
    offsetstart = 0;
    blockstart++;
    freenum = munmap(buf, PAGESIZE);
    buf = NULL;
    if(freenum < 0) {
      printf("Free block failed for fd %d in buffered_read\n", fd);
    }
  }
  return content;
}

//free the memory might be used as buffer when buffered_read
void buffered_read_finish(unsigned char*& buf) {
  if(buf == NULL) return;
  ssize_t freenum;
  freenum = munmap(buf, PAGESIZE);
  buf = NULL;
  if(freenum < 0) {
    printf("Free block failed in buffered_read_finish\n");
  }
}

std::unordered_map<u64, std::unordered_set<u64>> readgraph(uint32_t checkstart, int fd) {
  u64 numnodes;
  u64 numneighbours;
  u64 srcnode;
  u64 curnode;
  uint32_t blockstart = checkstart; //The block you are reading from
  uint32_t offsetstart = 0; //Offset in block you are reading from
  std::unordered_map<u64, std::unordered_set<u64>> edgelist;
  uint32_t i, j;
  unsigned char* buf = NULL; 
  //read total number of nodes
  numnodes = buffered_read(fd, blockstart, offsetstart, buf);
  for(i = 0; i < numnodes; i++) {
    //read current source node
    srcnode = buffered_read(fd, blockstart, offsetstart, buf);
    printf("Source node %llu has neighbours: \n", (unsigned long long)srcnode);
    //read how many neighbours source node has
    numneighbours = buffered_read(fd, blockstart, offsetstart, buf);
    std::unordered_set<u64> neighbours;
    for(j = 0; j < numneighbours; j++) {
      //read and insert all its neighbours
      curnode = buffered_read(fd, blockstart, offsetstart, buf);
      neighbours.insert(curnode);
      printf("%llu \n", (unsigned long long)curnode);
    }
    edgelist[srcnode] = neighbours;
  }
  buffered_read_finish(buf);
  return edgelist;
}











