// This autogenerated skeleton file illustrates how to build a server.
// You should copy it to another filename to avoid overwriting it.

#include "InterNodeComm.h"
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/server/TSimpleServer.h>
#include <thrift/transport/TServerSocket.h>
#include <thrift/transport/TBufferTransports.h>

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;

using boost::shared_ptr;

class InterNodeCommHandler : virtual public InterNodeCommIf {
 public:
  InterNodeCommHandler() {
    // Your initialization goes here
  }

  int32_t add_node_rep(const int32_t node) {
    // Your implementation goes here
    printf("add_node_rep\n");
  }

  int32_t remove_node_rep(const int32_t node) {
    // Your implementation goes here
    printf("remove_node_rep\n");
  }

  int32_t add_edge_rep(const int32_t node1, const int32_t node2) {
    // Your implementation goes here
    printf("add_edge_rep\n");
  }

  int32_t remove_edge_rep(const int32_t node1, const int32_t node2) {
    // Your implementation goes here
    printf("remove_edge_rep\n");
  }

  int32_t checkpoint_rep() {
    // Your implementation goes here
    printf("checkpoint_rep\n");
  }

};

int main(int argc, char **argv) {
  int port = 9090;
  shared_ptr<InterNodeCommHandler> handler(new InterNodeCommHandler());
  shared_ptr<TProcessor> processor(new InterNodeCommProcessor(handler));
  shared_ptr<TServerTransport> serverTransport(new TServerSocket(port));
  shared_ptr<TTransportFactory> transportFactory(new TBufferedTransportFactory());
  shared_ptr<TProtocolFactory> protocolFactory(new TBinaryProtocolFactory());

  TSimpleServer server(processor, serverTransport, transportFactory, protocolFactory);
  server.serve();
  return 0;
}

