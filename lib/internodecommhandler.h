#ifndef INTERNODESERVICE_H
#define INTERNODESERVICE_H

#include <thrift/concurrency/ThreadManager.h>
#include <thrift/concurrency/PlatformThreadFactory.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/server/TSimpleServer.h>
#include <thrift/server/TThreadPoolServer.h>
#include <thrift/server/TThreadedServer.h>
#include <thrift/transport/TServerSocket.h>
#include <thrift/transport/TSocket.h>
#include <thrift/transport/TTransportUtils.h>
#include <thrift/TToString.h>

#include <boost/make_shared.hpp>

#include <iostream>
#include <stdexcept>
#include <sstream>
#include "storagelog.h"
#include "gen-cpp/InterNodeComm.h"

using namespace std;
using namespace apache::thrift;
using namespace apache::thrift::concurrency;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using namespace apache::thrift::server;

class InterNodeCommHandler : public InterNodeCommIf{
  StorageLog& storageLog;
  boost::shared_ptr<TTransport> transport;
  boost::shared_ptr<InterNodeCommClient> clientp;
  public:
  //If this is tail node, slave_addr == 0
    InterNodeCommHandler(StorageLog& storageLog_, boost::shared_ptr<TTransport> transport_, boost::shared_ptr<InterNodeCommClient> clientp_)
      :storageLog(storageLog_)
    {
      this->transport = transport_;
      this->clientp = clientp_;
    }
    int32_t get_node_rep(int32_t node);
    int32_t add_node_rep(int32_t node);
    int32_t remove_node_rep(int32_t node);
    int32_t add_edge_rep(int32_t node1, int32_t node2);
    int32_t remove_edge_rep(int32_t node1, int32_t node2);
    int32_t checkpoint_rep();
};

class InterNodeCommCloneFactory : virtual public InterNodeCommIfFactory {
  public:
    StorageLog& storageLog;
    boost::shared_ptr<TTransport> transport;
    boost::shared_ptr<InterNodeCommClient> clientp;
    InterNodeCommCloneFactory(StorageLog& storageLog_, boost::shared_ptr<TTransport> transport_, boost::shared_ptr<InterNodeCommClient> clientp_)
     :storageLog(storageLog_) 
    {
      this->transport = transport_;
      this->clientp = clientp_;
    }

    virtual ~InterNodeCommCloneFactory() {}
    virtual InterNodeCommIf* getHandler(const ::apache::thrift::TConnectionInfo& connInfo)
    {
      boost::shared_ptr<TSocket> sock = boost::dynamic_pointer_cast<TSocket>(connInfo.transport);
      cout << "Incoming connection\n";
      cout << "\tSocketInfo: "  << sock->getSocketInfo() << "\n";
      cout << "\tPeerHost: "    << sock->getPeerHost() << "\n";
      cout << "\tPeerAddress: " << sock->getPeerAddress() << "\n";
      cout << "\tPeerPort: "    << sock->getPeerPort() << "\n";
      return new InterNodeCommHandler(storageLog, transport, clientp);
    }
    virtual void releaseHandler(InterNodeCommIf* handler) {
      delete handler;
    }
};

#endif
