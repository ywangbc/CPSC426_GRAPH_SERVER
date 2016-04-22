#include <string>
#include <unistd.h>
#include <sys/types.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <assert.h>
#include <unordered_map>
#include <unordered_set>
#include <errno.h>
#include <string.h>

#ifndef DISK_LIB_H
#define DISK_LIB_H

#define MAXLOGBLOCKNUM 524288ULL //maximum number of blocks used for log
#define MAXBLOCKNUM 2621440ULL //maximum number of blocks on disk
#define LOGSTARTBLOCK 1ULL //The starting block of log
#define PAGESIZE 4096ULL
#define SUPERSIZE 16ULL
#define BLOCKHEADERSIZE 12ULL
#define ENTRYSIZE 20ULL
#define ADD_NODE 0ULL
#define ADD_EDGE 1ULL
#define REMOVE_NODE 2ULL
#define REMOVE_EDGE 3ULL
#define MAX_ENTRY_IN_BLOCK  ((PAGESIZE-BLOCKHEADERSIZE)/ENTRYSIZE)


typedef uint64_t u64;


struct Superblock {
  uint32_t generation;
  uint32_t checksum;
  uint32_t logstart; //Which block, start from block 1
  uint32_t logsize; //2G, 500k blocks
};

struct Storageblockheader {
  uint32_t generation;
  uint32_t numofentries;
  uint32_t checksum;
};

struct Requestentry {
  uint32_t method;
  uint64_t node1;
  uint64_t node2;
};

//read the file from certain index and set its rw offset
ssize_t read_set(int fd, void* buf, size_t count, off_t offset);

//write the file from certain index and set its rw offset
ssize_t write_set(int fd, void* buf, size_t count, off_t offset);

//Return a pointer to a page of memory, which contains a super block at the start
unsigned char* get_superblock(int fd);

//Write memory pointed by buf (which must be a superblock of PAGESIZE) into file fd
//do not unmap the memory for superblock
ssize_t write_superblock(int fd, unsigned char* buf);

//Return a pointer to a page of memory, which contains #whichblock'th block from 0
unsigned char* get_block(int fd, uint32_t whichblock);

//Write memory pointed by buf (which must be a storage block of PAGESIZE) into file fd
//do not unmap the memory for block
ssize_t write_block(int fd, unsigned char* buf, uint32_t whichblock);

//check the validity of a block, the block must be of size PAGESIZE
uint32_t isValid(unsigned char* block);

//return the struct representing the storage block header
//Notice this effectively construct a new object, so you need to write it into the buffer in memory
Storageblockheader readblockheader(unsigned char* block);

//read whichentry'th entry into a Requestentry struct
//Notice this effectively construct a new object, so you need to write it into the buffer in memory
Requestentry readblockentry(unsigned char* block, uint32_t whichentry);

//store Storageblockheader object into a block
void writeblockheader(unsigned char* block, const Storageblockheader& header);

//store whichentry'th Requestentry into a block
void writeblockentry(unsigned char* block, uint32_t whichentry, const Requestentry& entry);

//Calculate the checksum of a super block
uint32_t calcchecksum_super(unsigned char* superblock);

//Calculate the checksum of a storage block
uint32_t calcchecksum_storage(unsigned char* block);

//calculate and update checksum of a super block (in memory block)
void update_checksum_super(unsigned char* superblock);

//calculate and update checksum of a super block (in memory block)
void update_checksum_storage(unsigned char* block);

//write content to disk at blockstart'th block and offsetstart, using buf as buffer
//need to allocate PAGESIZE for buf before use, and call buffered_write_finish after use
void buffered_write(int fd, uint32_t& blockstart, uint32_t& offsetstart, unsigned char* buf, uint64_t content);

//write last incomplete block on disk, at blockstart'th block
//free the memory buffer
void buffered_write_finish(int fd, uint32_t blockstart, unsigned char* buf);

//Store the graph on disk
//Start from check start, stored in superblock
int storegraph(uint32_t checkstart, int fd,
  std::unordered_map<u64, std::unordered_set<u64>>& edgelist);


//read from disk at blockstart'th block, at offsetstart in block, buffered by buf
u64 buffered_read(int fd, uint32_t& blockstart, uint32_t& offsetstart, unsigned char*& buf);

//free the memory might be used as buffer when buffered_read
void buffered_read_finish(unsigned char*& buf);

//read the graph and store it in an unordered_map
std::unordered_map<u64, std::unordered_set<u64>> readgraph(uint32_t checkstart, int fd);

//update a log entry, and the header of its block, then write to disk
//return 0 on success
//return -1 on block exceeds MAXBLOCKNUM, or entry exceeds MAX_ENTRY_IN_BLOCK
int update_log_entry(int fd, uint32_t whichblock, uint32_t whichentry, const Requestentry& entry, uint32_t generation);


#endif
