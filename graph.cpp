/*******
 * Author: Yu Wang
 * Date: 04/22/2016
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
#include <thread>
#include "lib/internodecommhandler.h"

static const int num_headers = 2;
static char *s_http_port;
//static struct mg_serve_http_opts s_http_server_opts;
FILE* fp;
static std::unordered_map<u64, std::unordered_set<u64>> edge_list;
const char* DISKNAME;
int fd; //file descriptor for storage disk

int partnum=-1; //which partition this server stands for
vector<char*> partlist;
vector<char*> all_addr;
vector<int> all_port;

StorageLog* storageLogp;
boost::shared_ptr<TTransport> socket_local;
boost::shared_ptr<TTransport> transport_local;
boost::shared_ptr<TProtocol> protocol_local;
boost::shared_ptr<InterNodeCommClient> clientp;
  
vector<boost::shared_ptr<TTransport>> all_socket_local;
vector<boost::shared_ptr<TTransport>> all_transport_local;
vector<boost::shared_ptr<TProtocol>> all_protocol_local;
vector<boost::shared_ptr<InterNodeCommClient>> all_clientp;

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


static void exec_command(const std::string& method, const std::vector<u64>& args, struct http_message* p, struct mg_connection*& nc)
{
  char buf[300];
  if(method.compare("add_node")==0)
  {
    if(args.size() != 1)
    {
      fprintf(fp, "Number of args does not match in add_node: %lu \n", args.size());
      return;
    }
    //handle response
    unsigned int count;
    int32_t retval;
    count = json_emit(buf, sizeof(buf), "{ s: i }", "node_id", (long)args[0]);
    if(count > sizeof(buf))
    {
      fprintf(fp, "add_node json exceeded buf size\n");
    }

    if(!add_node(edge_list, args)) {
      mg_printf(nc, "HTTP/1.1 204 OK\r\nContent-Length:0\r\n\r\n");
      fprintf(fp, "HTTP/1.1 204 OK\r\nContent-Length:0\r\n\r\n");
      return;
    }
    if(fd!=-1) {
      retval = storageLogp->update_log(ADD_NODE, args[0], 0);
      if(retval == -1) {
        mg_printf(nc, "HTTP/1.1 507 No space for log\r\nContent-Length:0\r\n\r\n");
        fprintf(fp, "HTTP/1.1 507 No space for log\r\nContent-Length:0\r\n\r\n");
        return;
      }
    }
    mg_printf(nc, "HTTP/1.1 200 OK\r\nContent-Length: %zu\r\nContent-Type: application/json\r\n\r\n%s\r\n", strlen(buf), buf);
    fprintf(fp, "HTTP/1.1 200 OK\r\nContent-Length: %zu\r\nContent-Type: application/json\r\n\r\n%s\r\n", strlen(buf), buf);
    fprintf(fp, "\n\n");
/*
    if(!clientp) {
      printf("clientp is null! This is tail and shall not receive such command!\n");
      return;
    }
    int32_t retval;
    transport_local->open();
    retval = clientp->add_node_rep((int32_t)args[0]);
    transport_local->close();
    if(retval == 1) {
      if(!add_node(edge_list, args)) {
        mg_printf(nc, "HTTP/1.1 204 OK\r\n\r\n");
        fprintf(fp, "HTTP/1.1 204 OK\r\n\r\n");
        return;
      }
      if(fd!=-1) {
        retval = storageLogp->update_log(ADD_NODE, args[0], 0);
        if(retval == -1) {
          mg_printf(nc, "HTTP/1.1 507 No space for log\r\n\r\n");
          fprintf(fp, "HTTP/1.1 507 No space for log\r\n\r\n");
          return;
        }
      }
      mg_printf(nc, "HTTP/1.1 200 OK\r\nContent-Length: %zu\r\nContent-Type: application/json\r\n\r\n%s\r\n", strlen(buf), buf);
      fprintf(fp, "HTTP/1.1 200 OK\r\nContent-Length: %zu\r\nContent-Type: application/json\r\n\r\n%s\r\n", strlen(buf), buf);
      fprintf(fp, "\n\n");
    }
    else if(retval == 0) {
      mg_printf(nc, "HTTP/1.1 204 OK\r\n\r\n");
      fprintf(fp, "HTTP/1.1 204 OK\r\n\r\n");
    }
    else if(retval == -1) {
      mg_printf(nc, "HTTP/1.1 507 No space for log\r\n\r\n");
      fprintf(fp, "HTTP/1.1 507 No space for log\r\n\r\n");
    }
*/
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

    int32_t retval;
    retval = add_edge_remote((u64)partnum, (u64)partlist.size(), all_transport_local, all_clientp, edge_list, args);
    if(retval == -1) {
      mg_printf(nc, "HTTP/1.1 400 Bad Request\r\nContent-Length:0\r\n\r\n");
      fprintf(fp, "HTTP/1.1 400 Bad Request\r\nContent-Length:0\r\n\r\n");
      return;
    }
    else if(retval == 0) {
      mg_printf(nc, "HTTP/1.1 204 OK\r\nContent-Length:0\r\n\r\n");
      fprintf(fp, "HTTP/1.1 204 OK\r\nContent-Length:0\r\n\r\n");
      return;
    }
    else if(retval == -2) {
      mg_printf(nc, "HTTP/1.1 507 No space for log\r\nContent-Length:0\r\n\r\n");
      fprintf(fp, "HTTP/1.1 507 No space for log\r\nContent-Length:0\r\n\r\n");
      return;
    }

    if(fd!=-1) {
      retval = storageLogp->update_log(ADD_EDGE, args[0], args[1]);
      if(retval == -1) {
        mg_printf(nc, "HTTP/1.1 507 No space for log\r\nContent-Length:0\r\n\r\n");
        fprintf(fp, "HTTP/1.1 507 No space for log\r\nContent-Length:0\r\n\r\n");
        return;
      }
    }

    mg_printf(nc, "HTTP/1.1 200 OK\r\nContent-Length: %zu\r\nContent-Type: application/json\r\n\r\n%s\r\n", strlen(buf), buf);
    fprintf(fp, "HTTP/1.1 200 OK\r\nContent-Length: %zu\r\nContent-Type: application/json\r\n\r\n%s\r\n", strlen(buf), buf);
    fprintf(fp, "\n\n");
/*
    transport_local->open();
    retval = clientp->add_edge_rep((int32_t)args[0], (int32_t)args[1]);
    transport_local->close();
    if(retval == 1) {
      retval = add_edge(edge_list, args);
      if(retval == -1) {
        mg_printf(nc, "HTTP/1.1 400 Bad Request\r\n\r\n");
        fprintf(fp, "HTTP/1.1 400 Bad Request\r\n\r\n");
        return;
      }
      else if(retval == 0) {
        mg_printf(nc, "HTTP/1.1 204 OK\r\n\r\n");
        fprintf(fp, "HTTP/1.1 204 OK\r\n\r\n");
        return;
      }

      if(fd!=-1) {
        retval = storageLogp->update_log(ADD_EDGE, args[0], args[1]);
        if(retval == -1) {
          mg_printf(nc, "HTTP/1.1 507 No space for log\r\n\r\n");
          fprintf(fp, "HTTP/1.1 507 No space for log\r\n\r\n");
          return;
        }
      }

      mg_printf(nc, "HTTP/1.1 200 OK\r\nContent-Length: %zu\r\nContent-Type: application/json\r\n\r\n%s\r\n", strlen(buf), buf);
      fprintf(fp, "HTTP/1.1 200 OK\r\nContent-Length: %zu\r\nContent-Type: application/json\r\n\r\n%s\r\n", strlen(buf), buf);
      fprintf(fp, "\n\n");
    }
    else if(retval == 0) {
      mg_printf(nc, "HTTP/1.1 204 OK\r\n\r\n");
      fprintf(fp, "HTTP/1.1 204 OK\r\n\r\n");
    }
    else if(retval == -1) {
      mg_printf(nc, "HTTP/1.1 400 Bad Request\r\n\r\n");
      fprintf(fp, "HTTP/1.1 400 Bad Request\r\n\r\n");
    }
    else if(retval == -2) {
      mg_printf(nc, "HTTP/1.1 507 No space for log\r\n\r\n");
      fprintf(fp, "HTTP/1.1 507 No space for log\r\n\r\n");
    }
*/
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

    int32_t retval;

    if(!remove_node(edge_list, args)) {
      mg_printf(nc, "HTTP/1.1 400 Bad Request\r\nContent-Length:0\r\n\r\n");
      fprintf(fp, "HTTP/1.1 400 Bad Request\r\nContent-Length:0\r\n\r\n");
      return;
    }
    if(fd!=-1) {
      retval = storageLogp->update_log(REMOVE_NODE, args[0], 0);
      if(retval == -1) {
        mg_printf(nc, "HTTP/1.1 507 No space for log\r\nContent-Length:0\r\n\r\n");
        fprintf(fp, "HTTP/1.1 507 No space for log\r\nContent-Length:0\r\n\r\n");
        return ;
      }
    }
    mg_printf(nc, "HTTP/1.1 200 OK\r\nContent-Length: %zu\r\nContent-Type: application/json\r\n\r\n%s\r\n", strlen(buf), buf);
    fprintf(fp, "HTTP/1.1 200 OK\r\nContent-Length: %zu\r\nContent-Type: application/json\r\n\r\n%s\r\n", strlen(buf), buf);
    fprintf(fp, "\n\n");
/*    
    transport_local->open();
    retval = clientp->remove_node_rep((int32_t)args[0]);
    transport_local->close();
    if(retval == 1) {
      if(!remove_node(edge_list, args)) {
        mg_printf(nc, "HTTP/1.1 400 Bad Request\r\n\r\n");
        fprintf(fp, "HTTP/1.1 400 Bad Request\r\n\r\n");
        return;
      }
      if(fd!=-1) {
        retval = storageLogp->update_log(REMOVE_NODE, args[0], 0);
        if(retval == -1) {
          mg_printf(nc, "HTTP/1.1 507 No space for log\r\n\r\n");
          fprintf(fp, "HTTP/1.1 507 No space for log\r\n\r\n");
          return ;
        }
      }
      mg_printf(nc, "HTTP/1.1 200 OK\r\nContent-Length: %zu\r\nContent-Type: application/json\r\n\r\n%s\r\n", strlen(buf), buf);
      fprintf(fp, "HTTP/1.1 200 OK\r\nContent-Length: %zu\r\nContent-Type: application/json\r\n\r\n%s\r\n", strlen(buf), buf);
      fprintf(fp, "\n\n");
    }
    else if(retval == 0) {
      mg_printf(nc, "HTTP/1.1 400 Bad Request\r\n\r\n");
      fprintf(fp, "HTTP/1.1 400 Bad Request\r\n\r\n");
    }
    else if(retval == -1) {
      mg_printf(nc, "HTTP/1.1 507 No space for log\r\n\r\n");
      fprintf(fp, "HTTP/1.1 507 No space for log\r\n\r\n");
    }
*/
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

    int32_t retval;
    retval = remove_edge_remote((u64)partnum, (u64)partlist.size(), all_transport_local, all_clientp, edge_list, args);
    
    if(retval == 0) {
      mg_printf(nc, "HTTP/1.1 400 Bad Request\r\nContent-Length:0\r\n\r\n");
      fprintf(fp, "HTTP/1.1 400 Bad Request\r\nContent-Length:0\r\n\r\n");
      return;
    }
    else if(retval == -1) {
      mg_printf(nc, "HTTP/1.1 507 No space for log\r\nContent-Length:0\r\n\r\n");
      fprintf(fp, "HTTP/1.1 507 No space for log\r\nContent-Length:0\r\n\r\n");
      return;
    }

    if(fd!=-1) {
      printf("Log file is on in remove_edge\n");
      retval = storageLogp->update_log(REMOVE_EDGE, args[0], args[1]);
      if(retval == -1) {
        mg_printf(nc, "HTTP/1.1 507 No space for log\r\nContent-Length:0\r\n\r\n");
        fprintf(fp, "HTTP/1.1 507 No space for log\r\nContent-Length:0\r\n\r\n");
        return;
      }
    }

    mg_printf(nc, "HTTP/1.1 200 OK\r\nContent-Length: %zu\r\nContent-Type: application/json\r\n\r\n%s", strlen(buf), buf);
    fprintf(fp, "HTTP/1.1 200 OK\r\nContent-Length: %zu\r\nContent-Type: application/json\r\n\r\n%s", strlen(buf), buf);
    fprintf(fp, "\n\n");
    
    /*
    int32_t retval;
    transport_local->open();
    retval = clientp->remove_edge_rep((int32_t)args[0], (int32_t)args[1]);
    transport_local->close();
    if(retval == 1) {
      if(!remove_edge(edge_list, args)) {
        mg_printf(nc, "HTTP/1.1 400 Bad Request\r\n\r\n");
        fprintf(fp, "HTTP/1.1 400 Bad Request\r\n\r\n");
        return;
      }
      if(fd!=-1) {
        retval = storageLogp->update_log(REMOVE_EDGE, args[0], args[1]);
        if(retval == -1) {
          mg_printf(nc, "HTTP/1.1 507 No space for log\r\n\r\n");
          fprintf(fp, "HTTP/1.1 507 No space for log\r\n\r\n");
          return;
        }
      }
      mg_printf(nc, "HTTP/1.1 200 OK\r\nContent-Length: %zu\r\nContent-Type: application/json\r\n\r\n%s", strlen(buf), buf);
      fprintf(fp, "HTTP/1.1 200 OK\r\nContent-Length: %zu\r\nContent-Type: application/json\r\n\r\n%s", strlen(buf), buf);
      fprintf(fp, "\n\n");
    }
    else if(retval == 0) {
      mg_printf(nc, "HTTP/1.1 400 Bad Request\r\n\r\n");
      fprintf(fp, "HTTP/1.1 400 Bad Request\r\n\r\n");
    }
    else if(retval == -1) {
      mg_printf(nc, "HTTP/1.1 507 No space for log\r\n\r\n");
      fprintf(fp, "HTTP/1.1 507 No space for log\r\n\r\n");
    }
    */

  }
  else if(method.compare("get_node")==0)
  {
    if(args.size() != 1)
    {
      fprintf(fp, "Number of args does not match in get_node: %lu \n", args.size());
      return;
    }
    bool status = get_node(edge_list, args);
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
    int status = get_edge(all_transport_local, all_clientp, partnum, partlist.size(), edge_list, args);
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
      mg_printf(nc, "HTTP/1.1 400 Bad Request\r\nContent-Length:0\r\n\r\n");
      fprintf(fp, "HTTP/1.1 400 Bad Request\r\nContent-Length:0\r\n\r\n");
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
    bool status = get_neighbors(edge_list, args, list);

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
      mg_printf(nc, "HTTP/1.1 400 Bad Request\r\nContent-Length:0\r\n\r\n");
      fprintf(fp, "HTTP/1.1 400 Bad Request\r\nContent-Length:0\r\n\r\n");
    }

  }
  else if(method.compare("shortest_path")==0)
  {
    if(args.size() != 2)
    {
      fprintf(fp, "Number of args does not match in shortest_path: %lu \n", args.size());
      return;
    }
    print_graph(edge_list);
    int status = shortest_path(edge_list, args);
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
      mg_printf(nc, "HTTP/1.1 204 OK\r\nContent-Length:0\r\n\r\n");
      fprintf(fp, "HTTP/1.1 204 OK\r\nContent-Length:0\r\n\r\n");
    }
    else if(status == -2)
    {
      mg_printf(nc, "HTTP/1.1 400 Bad Request\r\nContent-Length:0\r\n\r\n");
      fprintf(fp, "HTTP/1.1 400 Bad Request\r\nContent-Length:0\r\n\r\n");
    }
    else
    {
      fprintf(fp, "Invalid status code\n");
    }
  }
  else if (method.compare("checkpoint") == 0) {
    int32_t retval;
    printf("checkpointing...\n");
    transport_local->open();
    if(clientp->checkpoint_rep() < 0) {
      transport_local->close();
      mg_printf(nc, "HTTP/1.1 507 insufficient space for checkpoint on remote\r\n\r\n");
      fprintf(fp, "HTTP/1.1 507 insufficient space for checkpoint on remote\r\n\r\n");
      return;
    }
    transport_local->close();
    if(fd!=-1) {
      retval = storageLogp->checkpoint(fd); 
    }
    else {
      retval=-1;
    }
    if(retval < 0) {
      mg_printf(nc, "HTTP/1.1 507 insufficient space for checkpoint\r\nContent-Length:0\r\n\r\n");
      fprintf(fp, "HTTP/1.1 507 insufficient space for checkpoint\r\nContent-Length:0\r\n\r\n");
    }
    else {
      mg_printf(nc, "HTTP/1.1 200 OK\r\nContent-Length:0\r\n\r\n");
      fprintf(fp, "HTTP/1.1 200 OK\r\nContent-Length:0\r\n\r\n");
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

void mongoose_start() {
  struct mg_mgr mgr;
  struct mg_connection *nc;
  mg_mgr_init(&mgr, NULL);
  nc = mg_bind(&mgr, s_http_port, ev_handler);

  // Set up HTTP server parameters
  mg_set_protocol_http_websocket(nc);

  printf("Starting web server on port %s\n", s_http_port);
  for (;;) {
    mg_mgr_poll(&mgr, 1000);
  }
  mg_mgr_free(&mgr);
}

void thrift_start(char* slave_addr, int slave_port) {

  u64 i;
  for(i=0; i<partlist.size(); i++) {
    all_socket_local.push_back(boost::shared_ptr<TTransport>(new TSocket(all_addr[i], all_port[i])));
    all_transport_local.push_back(boost::shared_ptr<TTransport>(new TBufferedTransport(all_socket_local[i])));
    all_protocol_local.push_back(boost::shared_ptr<TProtocol>(new TBinaryProtocol(all_transport_local[i])));
    all_clientp.push_back(boost::shared_ptr<InterNodeCommClient>(new InterNodeCommClient(all_protocol_local[i])));
  }
  if(slave_addr != 0) {
    socket_local = all_socket_local[(partnum+1)%partlist.size()];
    transport_local = all_transport_local[(partnum+1)%partlist.size()];
    protocol_local = all_protocol_local[(partnum+1)%partlist.size()];
    clientp = all_clientp[(partnum+1)%partlist.size()];
    /*
    socket_local = boost::shared_ptr<TTransport>(new TSocket(slave_addr, slave_port));
    transport_local = boost::shared_ptr<TTransport>(new TBufferedTransport(socket_local));
    protocol_local = boost::shared_ptr<TProtocol>(new TBinaryProtocol(transport_local));
    clientp = boost::shared_ptr<InterNodeCommClient>(new InterNodeCommClient(protocol_local));
    */
    //transport->open();
  }

  TThreadedServer server(
      boost::make_shared<InterNodeCommProcessorFactory>(boost::make_shared<InterNodeCommCloneFactory>(*storageLogp, transport_local, clientp)),
      boost::make_shared<TServerSocket>(slave_port), //port to communicate with parent as a slave
      boost::make_shared<TBufferedTransportFactory>(),
      boost::make_shared<TBinaryProtocolFactory>());

  printf("Starting the master slave server on port %d...\n", slave_port);
  server.serve();

  printf("Done.\n");
}

int main(int argc, char* argv[]) {
  
  uint32_t lastblock = LOGSTARTBLOCK;
  uint32_t lastentry = 0;
  uint32_t needformat = 0;
  uint32_t writetodisk = 0;

  char* slave_addr = 0;
  int slave_port = 9090;
  
  int opt;
  int index; //used for processing partlist only
  u64 i; //iterator
  

  while((opt = getopt(argc, argv, "dfb:p:l:")) != -1) {
    switch (opt) {
      case 'p':
        partnum=atoi(optarg);
        break;
      case 'd':
        writetodisk = 1;
        break;
      case 'f':
        needformat = true;
        break;
      case 'b':
        slave_addr = optarg;
        break;
      case 'l':
        index = optind-1;
        while(index<argc) {
          if(argv[index][0]!='-') {
            partlist.push_back(argv[index]);
            index++;
          }
          else {
            optind=index;
            break;
          }
        }
        break;
      default:
        printf("Usage %s [-d] [-f] [-b slave_ip_addr] port [device], -d for enabling disk storage", argv[0]);
        exit(-1);
    }
  }

  if(optind >= argc) {
    printf("Expected port number and device file after options");
    exit(-1);
  }

  s_http_port = argv[optind];

  if(optind+1 >= argc) {
    if(writetodisk) {
      DISKNAME = "/dev/sdc";
      printf("using default disk name: %s\n", DISKNAME);
    }
    else {
      DISKNAME= NULL;
    }
  }
  else {
    DISKNAME = argv[optind+1];
  }

  printf("Connecting on port %s, writing to device %s\n", s_http_port, DISKNAME);
  if(writetodisk) {
    printf("Writing to device %s\n", DISKNAME);
  }
  else {
    printf("Not writing to device\n");
  }
  
  printf("This server is number %d among all partitions\n", partnum);
  printf("There are %lu partitions in total\n", partlist.size());

  //open disk
  if(writetodisk) {
    printf("trying to open disk with name: %s \n", DISKNAME);
    fd = open(DISKNAME, O_RDWR | O_DIRECT);
    if(fd < 0) {
      printf("Open disk failed, now exit\n");
      exit(-1);
    }
  }
  else {
    fd=-1;
  }
  
  storageLogp = new StorageLog(edge_list, fd, lastblock, lastentry);
  //format the disk if required
  if(writetodisk && needformat) {
    printf("start formatting\n");
    storageLogp->format(fd);
  }
  //pull out the checkpoint on disk and replay the log
  if(writetodisk) {
    storageLogp->on_start_up(fd);
  }

  fp = fopen("local_out", "w");

  std::thread mongoose_thread(mongoose_start);
  for(i=0; i<partlist.size(); i++) {
    all_addr.push_back(strtok(partlist[i], ":"));
    all_port.push_back(atoi(strtok(NULL, ":")));
  }
  printf("Address of all partitions are:\n");
  for(i=0; i<partlist.size(); i++) {
    printf("IP: %s, Port: %d\n", all_addr[i], all_port[i]);
  }


  //std::thread thrift_thread(thrift_start, slave_addr, slave_port);
  slave_addr = all_addr[(partnum+1)%partlist.size()];
  slave_port = all_port[(partnum+1)%partlist.size()];

  printf("Thrift is connecting to %s at port %d.\n", slave_addr, slave_port);

  thrift_start(slave_addr, slave_port);

  mongoose_thread.join();
  //thrift_thread.join();
  
  /*
  for (;;) {
    mg_mgr_poll(&mgr, 1000);
  }
  mg_mgr_free(&mgr);
  */

  return 0;
}
