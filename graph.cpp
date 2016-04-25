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
char* DISKNAME;
int fd; //file descriptor for storage disk

StorageLog* storageLogp;
InterNodeCommClient* clientp = 0;


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
    count = json_emit(buf, sizeof(buf), "{ s: i }", "node_id", (long)args[0]);
    if(count > sizeof(buf))
    {
      fprintf(fp, "add_node json exceeded buf size\n");
    }

    if(!clientp) {
      printf("clientp is null! This is tail and shall not receive such command!\n");
      return;
    }
    int32_t retval;
    retval = clientp->add_node_rep((int32_t)args[0]);
    if(retval == 1) {
      if(!add_node(edge_list, args)) {
        mg_printf(nc, "HTTP/1.1 204 OK\r\n\r\n");
        fprintf(fp, "HTTP/1.1 204 OK\r\n\r\n");
        return;
      }
      retval = storageLogp->update_log(ADD_NODE, args[0], 0);
      if(retval == -1) {
        mg_printf(nc, "HTTP/1.1 507 No space for log\r\n\r\n");
        fprintf(fp, "HTTP/1.1 507 No space for log\r\n\r\n");
        return;
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
    retval = clientp->add_edge_rep((int32_t)args[0], (int32_t)args[1]);
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

      retval = storageLogp->update_log(ADD_EDGE, args[0], args[1]);
      if(retval == -1) {
        mg_printf(nc, "HTTP/1.1 507 No space for log\r\n\r\n");
        fprintf(fp, "HTTP/1.1 507 No space for log\r\n\r\n");
        return;
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
    retval = clientp->remove_node_rep((int32_t)args[0]);
    if(retval == 1) {
      if(!remove_node(edge_list, args)) {
        mg_printf(nc, "HTTP/1.1 400 Bad Request\r\n\r\n");
        fprintf(fp, "HTTP/1.1 400 Bad Request\r\n\r\n");
        return;
      }
      retval = storageLogp->update_log(REMOVE_NODE, args[0], 0);
      if(retval == -1) {
        mg_printf(nc, "HTTP/1.1 507 No space for log\r\n\r\n");
        fprintf(fp, "HTTP/1.1 507 No space for log\r\n\r\n");
        return ;
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
    retval = clientp->remove_edge_rep((int32_t)args[0], (int32_t)args[1]);
    if(retval == 1) {
      if(!remove_edge(edge_list, args)) {
        mg_printf(nc, "HTTP/1.1 400 Bad Request\r\n\r\n");
        fprintf(fp, "HTTP/1.1 400 Bad Request\r\n\r\n");
        return;
      }
      retval = storageLogp->update_log(REMOVE_EDGE, args[0], args[1]);
      if(retval == -1) {
        mg_printf(nc, "HTTP/1.1 507 No space for log\r\n\r\n");
        fprintf(fp, "HTTP/1.1 507 No space for log\r\n\r\n");
        return;
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
    int status = get_edge(edge_list, args);
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
    if(storageLogp->checkpoint(fd) < 0) {
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
  boost::shared_ptr<TTransport> socket;
  boost::shared_ptr<TTransport> transport;
  boost::shared_ptr<TProtocol> protocol;
  
  if(slave_addr != 0) {
    socket = boost::shared_ptr<TTransport>(new TSocket(slave_addr, slave_port));
    transport = boost::shared_ptr<TTransport>(new TBufferedTransport(socket));
    protocol = boost::shared_ptr<TProtocol>(new TBinaryProtocol(transport));
    clientp = new InterNodeCommClient(protocol);

    transport->open();
  }

  TThreadedServer server(
      boost::make_shared<InterNodeCommProcessorFactory>(boost::make_shared<InterNodeCommCloneFactory>(*storageLogp, clientp)),
      boost::make_shared<TServerSocket>(slave_port), //port to communicate with parent as a slave
      boost::make_shared<TBufferedTransportFactory>(),
      boost::make_shared<TBinaryProtocolFactory>());

  cout << "Starting the master slave server...\n";
  server.serve();

  if(clientp) {
    transport->close();
    delete clientp;
    clientp = 0;
  }
  cout << "Done. \n";
}

int main(int argc, char* argv[]) {
  
  uint32_t lastblock = LOGSTARTBLOCK;
  uint32_t lastentry = 0;
  uint32_t needformat = 0;

  char* slave_addr = 0;
  int slave_port = 9090;
  
  int opt;

  while((opt = getopt(argc, argv, "fb:")) != -1) {
    switch (opt) {
      case 'f':
        needformat = true;
        break;
      case 'b':
        slave_addr = optarg;
        break;
      default:
        printf("Usage %s [-f] [-b slave_ip_addr] port", argv[0]);
        exit(-1);
    }
  }

  if(optind >= argc) {
    printf("Expected port number and device file after options");
    exit(-1);
  }

  s_http_port = argv[optind];

  if(optind+1 >= argc) {
    printf("Expected dev file after port number");
  }
  DISKNAME = argv[optind+1];

  printf("Connecting on port %s, writing to device %s\n", s_http_port, DISKNAME);


  //open disk
  printf("trying to open disk with name: %s \n", DISKNAME);
  fd = open(DISKNAME, O_RDWR | O_DIRECT);
  if(fd < 0) {
    printf("Open disk failed, now exit\n");
    exit(-1);
  }
  
  storageLogp = new StorageLog(edge_list, fd, lastblock, lastentry);
  //format the disk if required
  if(needformat) {
    printf("start formatting\n");
    storageLogp->format(fd);
  }
  //pull out the checkpoint on disk and replay the log
  storageLogp->on_start_up(fd);


  fp = fopen("local_out", "w");

  std::thread mongoose_thread(mongoose_start);
  std::thread thrift_thread(thrift_start, slave_addr, slave_port);


  mongoose_thread.join();
  thrift_thread.join();
  
  /*
  for (;;) {
    mg_mgr_poll(&mgr, 1000);
  }
  mg_mgr_free(&mgr);
  */

  return 0;
}
