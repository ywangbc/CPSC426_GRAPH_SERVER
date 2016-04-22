/*******
 * Author: Yu Wang
 *
 */

#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <cstring>
#include <vector>
#include <queue>
#include <unordered_map>
#include <unordered_set>
#include <assert.h>
#include "lib/mongoose.h"
#include "lib/disk_lib.h"
#include "lib/debug.h"

static const int num_headers = 2;
static char *s_http_port;
//static struct mg_serve_http_opts s_http_server_opts;
FILE* fp;
static std::unordered_map<u64, std::unordered_set<u64>> edge_list;
char* DISKNAME;
int fd; //file descriptor for storage disk

//next read or write happens here
uint32_t lastblock;
uint32_t lastentry;

static std::string get_method(const char* p)
{
  //printf("In get_method\n");
  std::string result = "";
  char target[] = "api/v1/";
  const char* pos = strstr(p, target);
  if(!pos)
  {
    fprintf(fp, "Invalid method string, no api/v1\n");
  }
  //printf("method splitted\n");
  pos += sizeof(target)-1;
  for(;*pos!=' '; pos++)
  {
    result.push_back(*pos);
  }
  //printf("Before get_method return\n");
  return result;
}

std::vector<u64> get_args(const char* p)
{
  std::vector<u64> result;
  struct json_token tokens[10];
  int token_size = sizeof(tokens) / sizeof(tokens[0]);
  assert(parse_json(p, strlen(p), tokens, token_size)>0);
  for(size_t i = 0 ; tokens[i].type != JSON_TYPE_EOF; i++)
  {
    //printf("In get_args current i is: %lu\n", i);
    if(tokens[i].type == JSON_TYPE_NUMBER)
    {
      //printf("current token is: %llu\n", strtoull(tokens[i].ptr, NULL, 10));
      result.push_back(strtoull(tokens[i].ptr, NULL, 10));
    }
  }
  //printf("Before get_args return\n");
  return result;
}

bool add_node(const std::vector<u64>& args)
{
  //printf("In add_node\n");
  if(edge_list.find(args[0]) != edge_list.end())
  {
    return false;
  }
  else
  {
    edge_list[args[0]];   
    return true;
  }
}
/* return 1 on success
 * return 0 if edge already exist
 * return -1 if node_a_id = node_b_id or, or node does not exist
 * */
int add_edge(const std::vector<u64>& args)
{
 // printf("In add_edge\n");
  if(args[0] == args[1])
  {
    return -1;
  }
  else if(edge_list.find(args[0]) == edge_list.end())
  {
    return -1;
  }
  else if(edge_list.find(args[1]) == edge_list.end())
  {
    return -1;
  }
  else
  {
    if(edge_list[args[0]].find(args[1]) != edge_list[args[0]].end())
    {
      return 0;
    }
    else
    {
      edge_list[args[0]].insert(args[1]);
      edge_list[args[1]].insert(args[0]);
      return 1;
    }
  }
}

//return false if remove failed, true if success
bool remove_node(const std::vector<u64>& args)
{
  //printf("In remove_node\n");
  if(edge_list.find(args[0]) == edge_list.end())
  {
    return false;
  }
  else
  {
    std::unordered_set<u64>::iterator it;
    for(it = edge_list[args[0]].begin(); it != edge_list[args[0]].end(); it++)
    {
      edge_list[*it].erase(args[0]);
    }
    edge_list.erase(args[0]);
    return true;
  }
}

/* return true on success
 * return false if edge does not exist 
 */

bool remove_edge(const std::vector<u64>& args)
{
  //printf("In remove_edge\n");
  if(edge_list.find(args[0]) == edge_list.end())
  {
    return false;
  }
  else if(edge_list.find(args[1]) == edge_list.end())
  {
    return false;
  }
  else
  {
    if(edge_list[args[0]].find(args[1]) == edge_list[args[0]].end())
    {
      return false;
    }
    else
    {
      edge_list[args[0]].erase(args[1]);
      edge_list[args[1]].erase(args[0]);
      return true;
    }
  }
}

/* return true if the node is in the graph
 * otherwise return false
 */
bool get_node(const std::vector<u64>& args)
{
  //printf("In get_node\n");
  if(edge_list.find(args[0]) == edge_list.end())
  {
    return false;
  }
  else
  {
    return true;
  }
}

/* return 1 if the edge is in the graph
 * return 0 if the edge is not in the graph
 * if any of the node is not in the graph, return -1
 */
int get_edge(const std::vector<u64>& args)
{
  //printf("In get_edge\n");
  if(edge_list.find(args[0]) == edge_list.end())
  {
    return -1;
  }
  else if(edge_list.find(args[1]) == edge_list.end())
  {
    return -1;
  }
  else if(edge_list[args[0]].find(args[1]) == edge_list[args[0]].end())
  {
    return 0;
  }
  else //The edge exist
  {
    return 1;
  }
}

/* return false if the node does not exist
 * return true and put neighbours into list if exist
 */
bool get_neighbors(const std::vector<u64>& args, std::vector<u64>& list)
{
  //printf("In get_neighbors\n");
  if(edge_list.find(args[0]) == edge_list.end())
  {
    return false;
  }
  else
  {
    list.insert(list.end(), edge_list[args[0]].begin(), edge_list[args[0]].end());
    return true;
  }
}

/* return -1 if there is no path
 * return -2 if either node does not exist
 * return the shortest distance otherwise 
 */
int shortest_path(const std::vector<u64>& args)
{
  //printf("In shortest_path\n");
  if(edge_list.find(args[0]) == edge_list.end())
  {
    return -2;
  }
  else if(edge_list.find(args[1]) == edge_list.end())
  {
    return -2;
  }
  
  std::unordered_set<u64> visited;
  std::queue<std::pair<u64, u64>> dijkq;
  dijkq.push(std::make_pair(args[0], 0));
  visited.insert(args[0]);

  while(!dijkq.empty())
  {
    std::pair<u64, u64> cur = dijkq.front();
    dijkq.pop();
    if(cur.first == args[1])
    {
      return cur.second;
    }
    std::unordered_set<u64>::iterator it;
    for(it = edge_list[cur.first].begin(); it != edge_list[cur.first].end(); it++)
    {
       if(visited.find(*it) != visited.end())
       {
         continue;
       }
       else
       {
         visited.insert(*it);
         dijkq.push(std::make_pair(*it, cur.second+1));
       }
    }
  } 
  return -1;
}

//check if there is enough space
//return -1 if there is no enough space
//return 0 if there is enough space 
int checkspace(std::unordered_map<u64, std::unordered_set<u64>>edge_list) {
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

//store the current checkpoint
//increase generation number in superblock
//update lastblock and lastentry
//return 0 on success
//return -1 indicating no enough space for checkpoint
int checkpoint(int fd) {
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

void update_tail(uint32_t& lastblock, uint32_t& lastentry) {
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
int update_log(uint32_t method, u64 node1, u64 node2) {
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

static void exec_command(const std::string& method, const std::vector<u64>& args, struct http_message* p, struct mg_connection*& nc)
{
  char buf[300];
  int result;
  if(method.compare("add_node")==0)
  {
    if(args.size() != 1)
    {
      fprintf(fp, "Number of args does not match in add_node: %lu \n", args.size());
      return;
    }
    //handle response
    unsigned int count;
    count = json_emit(buf, sizeof(buf), "{ s: i }", "node_id", (long)args[0]);
    if(count > sizeof(buf))
    {
      fprintf(fp, "add_node json exceeded buf size\n");
    }
    
    //int num_printed;

    if(add_node(args))
    {
      //handle log
      printf("adding node %llu in log\n", (unsigned long long)args[0]);
      printf("last block: %llu, last entry: %llu\n", (unsigned long long)lastblock, (unsigned long long)lastentry);
      result = update_log(ADD_NODE, args[0], 0);
      if(result == -1) {
        mg_printf(nc, "HTTP/1.1 507 No space for log\r\n\r\n");
        fprintf(fp, "HTTP/1.1 507 No space for log\r\n\r\n");
        return;
      }

      mg_printf(nc, "HTTP/1.1 200 OK\r\nContent-Length: %zu\r\nContent-Type: application/json\r\n\r\n%s\r\n", strlen(buf), buf);
      fprintf(fp, "HTTP/1.1 200 OK\r\nContent-Length: %zu\r\nContent-Type: application/json\r\n\r\n%s\r\n", strlen(buf), buf);
      fprintf(fp, "\n");
    }
    else
    {
      mg_printf(nc, "HTTP/1.1 204 OK\r\n\r\n");
      fprintf(fp, "HTTP/1.1 204 OK\r\n\r\n");
    }
    //printf("num_printed is: %d", num_printed);
  }
  else if(method.compare("add_edge")==0)
  {
    if(args.size() != 2)
    {
      fprintf(fp, "Number of args does not match in add_edge: %lu \n", args.size());
      return;
    }
    //handle response
    unsigned int count;
    count = json_emit(buf, sizeof(buf), "{ s: i, s: i }", "node_a_id", (long)args[0], "node_b_id", (long)args[1]);
    if(count > sizeof(buf))
    {
      fprintf(fp, "add_node json exceeded buf size\n");
    }
    
    int status = add_edge(args);
    if(status == 1)
    {
      //handle log
      printf("adding edge (%llu, %llu) in log\n", (unsigned long long)args[0], (unsigned long long)args[1]);
      printf("last block: %llu, last entry: %llu\n", (unsigned long long)lastblock, (unsigned long long)lastentry);
      result = update_log(ADD_EDGE, args[0], args[1]);
      if(result == -1) {
        mg_printf(nc, "HTTP/1.1 507 No space for log\r\n\r\n");
        fprintf(fp, "HTTP/1.1 507 No space for log\r\n\r\n");
        return;
      }

      mg_printf(nc, "HTTP/1.1 200 OK\r\nContent-Length: %zu\r\nContent-Type: application/json\r\n\r\n%s\r\n", strlen(buf), buf);
      fprintf(fp, "HTTP/1.1 200 OK\r\nContent-Length: %zu\r\nContent-Type: application/json\r\n\r\n%s\r\n", strlen(buf), buf);
      fprintf(fp, "\n\n");
    }
    else if(status == 0)
    {
      mg_printf(nc, "HTTP/1.1 204 OK\r\n\r\n");
      fprintf(fp, "HTTP/1.1 204 OK\r\n\r\n");
    }
    else if(status == -1)
    {
      mg_printf(nc, "HTTP/1.1 400 Bad Request\r\n\r\n");
      fprintf(fp, "HTTP/1.1 400 Bad Request\r\n\r\n");
    }
    else
    {
      fprintf(fp, "Invalid status code\n");
    }
  }
  else if(method.compare("remove_node")==0)
  {
    if(args.size() != 1)
    {
      fprintf(fp, "Number of args does not match in remove_node: %lu \n", args.size());
      return;
    }

    //handle response

    unsigned int count;
    count = json_emit(buf, sizeof(buf), "{ s: i }", "node_id", (long)args[0]);
    if(count > sizeof(buf))
    {
      fprintf(fp, "add_node json exceeded buf size\n");
    }
    
    if(remove_node(args))
    {
      //handle log
      printf("removing node %llu in log\n", (unsigned long long)args[0]);
      printf("last block: %llu, last entry: %llu\n", (unsigned long long)lastblock, (unsigned long long)lastentry);
      result = update_log(REMOVE_NODE, args[0], 0);
      if(result == -1) {
        mg_printf(nc, "HTTP/1.1 507 No space for log\r\n\r\n");
        fprintf(fp, "HTTP/1.1 507 No space for log\r\n\r\n");
        return;
      }

      mg_printf(nc, "HTTP/1.1 200 OK\r\nContent-Length: %zu\r\nContent-Type: application/json\r\n\r\n%s\r\n", strlen(buf), buf);
      fprintf(fp, "HTTP/1.1 200 OK\r\nContent-Length: %zu\r\nContent-Type: application/json\r\n\r\n%s\r\n", strlen(buf), buf);
      fprintf(fp, "\n\n");
    }
    else
    {
      mg_printf(nc, "HTTP/1.1 400 Bad Request\r\n\r\n");
      fprintf(fp, "HTTP/1.1 400 Bad Request\r\n\r\n");
    }
  }
  else if(method.compare("remove_edge")==0)
  {
    if(args.size() != 2)
    {
      fprintf(fp, "Number of args does not match in remove_edge: %lu \n", args.size());
      return;
    }


    //handle response
    unsigned int count;
    count = json_emit(buf, sizeof(buf), "{ s: i, s: i }", "node_a_id", (long)args[0], "node_b_id", (long)args[1]);
    if(count > sizeof(buf))
    {
      fprintf(fp, "add_node json exceeded buf size\n");
    }
    //fprintf(fp, "strlen of buf is: %zu", strlen(buf));
    
    int status = remove_edge(args);
    if(status)
    {
      //handle log
      printf("removing edge (%llu, %llu) in log\n", (unsigned long long)args[0], (unsigned long long)args[1]);
      printf("last block: %llu, last entry: %llu\n", (unsigned long long)lastblock, (unsigned long long)lastentry);
      result = update_log(REMOVE_EDGE, args[0], args[1]);
      if(result == -1) {
        mg_printf(nc, "HTTP/1.1 507 No space for log\r\n\r\n");
        fprintf(fp, "HTTP/1.1 507 No space for log\r\n\r\n");
        return;
      }

      mg_printf(nc, "HTTP/1.1 200 OK\r\nContent-Length: %zu\r\nContent-Type: application/json\r\n\r\n%s", strlen(buf), buf);
      fprintf(fp, "HTTP/1.1 200 OK\r\nContent-Length: %zu\r\nContent-Type: application/json\r\n\r\n%s", strlen(buf), buf);
      fprintf(fp, "\n\n");
    }
    else
    {
      mg_printf(nc, "HTTP/1.1 400 Bad Request\r\n\r\n");
      fprintf(fp, "HTTP/1.1 400 Bad Request\r\n\r\n");
    }
  }
  else if(method.compare("get_node")==0)
  {
    if(args.size() != 1)
    {
      fprintf(fp, "Number of args does not match in get_node: %lu \n", args.size());
      return;
    }
    bool status = get_node(args);
    unsigned int count;
    if(status)
    {
      count = json_emit(buf, sizeof(buf), "{ s: T }", "in_graph");
    }
    else
    {
      count = json_emit(buf, sizeof(buf), "{ s: F }", "in_graph");
    }

    if(count > sizeof(buf))
    {
      fprintf(fp, "add_node json exceeded buf size\n");
    }
    
    mg_printf(nc, "HTTP/1.1 200 OK\r\nContent-Length: %zu\r\nContent-Type: application/json\r\n\r\n%s\r\n", strlen(buf), buf);
    fprintf(fp, "HTTP/1.1 200 OK\r\nContent-Length: %zu\r\nContent-Type: application/json\r\n\r\n%s\r\n", strlen(buf), buf);
    fprintf(fp, "\n\n");
  }
  else if(method.compare("get_edge")==0)
  {
    if(args.size() != 2)
    {
      fprintf(fp, "Number of args does not match in get_edge: %lu \n", args.size());
      return;
    }
    int status = get_edge(args);
    unsigned int count;
    if(status == 1)
    {
      count = json_emit(buf, sizeof(buf), "{ s: T }", "in_graph");
    }
    else if(status == 0)
    {
      count = json_emit(buf, sizeof(buf), "{ s: F }", "in_graph");
    }

    if(count > sizeof(buf))
    {
      fprintf(fp, "add_node json exceeded buf size\n");
    }
    
    if(status == 1 || status == 0)
    {
      mg_printf(nc, "HTTP/1.1 200 OK\r\nContent-Length: %zu\r\nContent-Type: application/json\r\n\r\n%s\r\n", strlen(buf), buf);
      fprintf(fp, "HTTP/1.1 200 OK\r\nContent-Length: %zu\r\nContent-Type: application/json\r\n\r\n%s\r\n", strlen(buf), buf);
      fprintf(fp, "\n\n");
    }
    else if(status == -1)
    {
      mg_printf(nc, "HTTP/1.1 400 Bad Request\r\n\r\n");
      fprintf(fp, "HTTP/1.1 400 Bad Request\r\n\r\n");
    }
    else
    {
      fprintf(fp, "Invalid status code in get_edge\n");
    }
  }
  else if(method.compare("get_neighbors")==0)
  {
    if(args.size() != 1)
    {
      fprintf(fp, "Number of args does not match in get_neighbors: %lu \n", args.size());
      return;
    }

    std::vector<u64> list;
    bool status = get_neighbors(args, list);

    unsigned int count;
    count = json_emit(buf, sizeof(buf), "{ s: i , ", "node_id", (long)args[0]);
    char* bp = buf+count;
    count += json_emit(bp, sizeof(buf)-count, " s: [", "neighbors");
    bp = buf+count;

    for(size_t i = 0; i < list.size(); i++)
    {
      if(i != list.size()-1)
      {
        count += json_emit(bp, sizeof(buf)-count, "i, ", list[i]);
      }
      else
      {
        count += json_emit(bp, sizeof(buf)-count, "i", list[i]);
      }
      bp = buf+count;
    }
    count += json_emit(bp, sizeof(buf)-count, "] }");
    if(count > sizeof(buf))
    {
      fprintf(fp, "add_node json exceeded buf size\n");
    }

    if(status)
    {
      mg_printf(nc, "HTTP/1.1 200 OK\r\nContent-Length: %zu\r\nContent-Type: application/json\r\n\r\n%s\r\n", strlen(buf), buf);
      fprintf(fp, "HTTP/1.1 200 OK\r\nContent-Length: %zu\r\nContent-Type: application/json\r\n\r\n%s\r\n", strlen(buf), buf);
      fprintf(fp, "\n\n");
    }
    else
    {
      mg_printf(nc, "HTTP/1.1 400 Bad Request\r\n\r\n");
      fprintf(fp, "HTTP/1.1 400 Bad Request\r\n\r\n");
    }

  }
  else if(method.compare("shortest_path")==0)
  {
    if(args.size() != 2)
    {
      fprintf(fp, "Number of args does not match in shortest_path: %lu \n", args.size());
      return;
    }
    int status = shortest_path(args);
    if(status >= 0)
    {
      unsigned int count;
      count = json_emit(buf, sizeof(buf), "{ s: i, s: i } ", "node_id", (long)args[0], "distance", (long)status);
      if(count > sizeof(buf))
      {
        fprintf(fp, "shortest_path json exceeded buf size\n");
      }

      mg_printf(nc, "HTTP/1.1 200 OK\r\nContent-Length: %zu\r\nContent-Type: application/json\r\n\r\n%s\r\n", strlen(buf), buf);
      fprintf(fp, "HTTP/1.1 200 OK\r\nContent-Length: %zu\r\nContent-Type: application/json\r\n\r\n%s\r\n", strlen(buf), buf);
      fprintf(fp, "\n\n");
    }
    else if(status == -1)
    {
      mg_printf(nc, "HTTP/1.1 204 OK\r\n\r\n");
      fprintf(fp, "HTTP/1.1 204 OK\r\n\r\n");
    }
    else if(status == -2)
    {
      mg_printf(nc, "HTTP/1.1 400 Bad Request\r\n\r\n");
      fprintf(fp, "HTTP/1.1 400 Bad Request\r\n\r\n");
    }
    else
    {
      fprintf(fp, "Invalid status code\n");
    }
  }
  else if (method.compare("checkpoint") == 0) {
    printf("checkpointing...\n");
    if(checkpoint(fd) < 0) {
      mg_printf(nc, "HTTP/1.1 507 insufficient space for checkpoint\r\n\r\n");
      fprintf(fp, "HTTP/1.1 507 insufficient space for checkpoint\r\n\r\n");
    }
    else {
      mg_printf(nc, "HTTP/1.1 200 OK\r\n\r\n");
      fprintf(fp, "HTTP/1.1 200 OK\r\n\r\n");
    }
    printf("checkpoint finished\n");
  }
  else
  {
    fprintf(fp, "Invalid command: %s\n", method.c_str());
  }
  return;
}


static void parse_exec_command(struct http_message* p, struct mg_connection*& nc)
{
  printf("in parse_exec_command\n");
  std::string method = get_method(p->method.p);
  fprintf(fp, "The extracted method is: %s\n\n", method.c_str());
  printf("The extracted method is: %s\n\n", method.c_str());
  //printf("The extracted method is: %s\n\n", method.c_str());
  std::vector<u64> args;
  if(p->body.len != 0) {
    printf("Received message has body length %lu\n", p->body.len);
    args = get_args(p->body.p);
  }
  else {
    printf("Received message has body length 0\n");
  }
  //fprintf(fp, "parsed args are: ");
  printf("parsed args are: ");
  //printf("parsed args are: \n");
  printf("We have %lu args\n", args.size());
  for(size_t i = 0; i < args.size(); i++)
  {
    fprintf(fp, "%" PRIu64, args[i]);
    printf("%" PRIu64, args[i]);
  }
  fprintf(fp, "\n\n");
  printf("\n\n");
  exec_command(method, args, p, nc);
}


static void ev_handler(struct mg_connection *nc, int ev, void *oldp) {
  struct http_message *p =(struct http_message*) oldp;
  //printf("received message from client\n");
  if (ev == MG_EV_HTTP_REQUEST) {
    printf("Recceived valid request\n");
    parse_exec_command(p, nc);
  }
}

void format(int fd) {
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

void play_log(int fd, uint32_t generation)
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
        if(!(add_node(args))) {
          printf("node %" PRIu64 " is already added when playlog, generation %" PRIu32 "\n", args[0], generation);
        }
      }
      else if(request.method == ADD_EDGE) {
        args.push_back(request.node1);
        args.push_back(request.node2);
        int result = add_edge(args);
        if(result == 0) {
          printf("node %" PRIu64 ", node %" PRIu64 " edge is already added when playlog, generation %" PRIu32 "\n", args[0], args[1], generation);
        }
        else if(result == -1) {
          printf("node %" PRIu64 ", node %" PRIu64 " edge are equal or do not exist when palying log, generation %" PRIu32 "\n", args[0], args[1], generation);
        }

      }
      else if(request.method == REMOVE_NODE) {
        args.push_back(request.node1);
        if(!(remove_node(args))) {
          printf("node %" PRIu64 " remove does exist not when playlog, generation %" PRIu32 "\n", args[0], generation);
        }
      }
      else if(request.method == REMOVE_EDGE) {
        args.push_back(request.node1);
        args.push_back(request.node2);
        int result = remove_edge(args);
        if(result == 0) {
          printf("node %" PRIu64 ", node %" PRIu64 " edge does not exist playlog, generation %" PRIu32 "\n", args[0], args[1], generation);
        }
      }      
      else {
        printf("Invalid method when palying log: %" PRIu32 "\n", request.method);
      }
    }                                                                                                 
  }
  lastblock = whichblock-1;
  lastentry = whichentry;  
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

void on_start_up(int fd) {
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

int main(int argc, char* argv[]) {
  lastblock = LOGSTARTBLOCK;
  lastentry = 0;
  uint32_t needformat = 0;
  if ( argc == 3 ) {
    s_http_port = argv[1];
    DISKNAME = argv[2];
  }
  else if( argc == 4 ) {
    if(strcmp(argv[1], "-f") != 0) {
      printf("Invalid option argument %s, only support -f\n", argv[1]);
      return -1;
    }
    needformat = true;
    s_http_port = argv[2];
    DISKNAME = argv[3];
  }
  else {
    printf("Usage: file %s, argument number invalid\n, only receive -f, port number and device\n", argv[0]);
    return -1;
  }
  printf("Connecting on port %s, writing to device %s\n", s_http_port, DISKNAME);
  //open disk
  printf("trying to open disk with name: %s \n", DISKNAME);
  fd = open(DISKNAME, O_RDWR | O_DIRECT);
  if(fd < 0) {
    printf("Open disk failed, now exit\n");
    exit(-1);
  }
  //format the disk if required
  if(needformat) {
    printf("start formatting\n");
    format(fd);
  }
  //pull out the checkpoint on disk and replay the log
  on_start_up(fd);

  struct mg_mgr mgr;
  struct mg_connection *nc;

  fp = fopen("local_out", "w");
  mg_mgr_init(&mgr, NULL);
  nc = mg_bind(&mgr, s_http_port, ev_handler);

  // Set up HTTP server parameters
  mg_set_protocol_http_websocket(nc);

  printf("Starting web server on port %s\n", s_http_port);
  for (;;) {
    mg_mgr_poll(&mgr, 1000);
  }
  mg_mgr_free(&mgr);

  return 0;
}
