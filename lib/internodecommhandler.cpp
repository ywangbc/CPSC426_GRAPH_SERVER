#include "internodecommhandler.h"

/***************
 * get_node_rep
 * return 1 if node is in graph
 * return 0 if node is not in graph
 ***********************/
int32_t InterNodeCommHandler::get_node_rep(int32_t node) 
{
  vector<u64> nodes;
  nodes.push_back((u64)node);
  return get_node(storageLog.edge_list, nodes);
}

/***************
 * add_node_rep
 * return 1 on success
 * return 0 on repetitive node at remote or local
 * return -1 on no enough storage at remote or local
 ***********************/
int32_t InterNodeCommHandler::add_node_rep(int32_t node) 
{
  int32_t retval;
  printf("Received add node from parent\n");
  if(clientp != NULL) {
    transport->open();
    printf("Passing add node to child\n");
    retval = clientp->add_node_rep(node);
    if(retval == -1) {
      printf("No enough storage (remote) in add_node_rep, add failed\n");
      transport->close();
      return -1;
    }
    else if(retval == 0) {
      printf("node already exist (remote) in add_node_rep, add failed\n");
      transport->close();
      return 0;
    }
    transport->close();
  }

  printf("Adding node at local\n");
  vector<u64> nodes;
  nodes.push_back((u64)node);
  if(!add_node(storageLog.edge_list, nodes)) {
    printf("node already exist (local) in add_node_rep, add failed\n");
    return 0;
  }
  if(storageLog.fd!=-1) {
    retval = storageLog.update_log(ADD_NODE, nodes[0], 0);
    if(retval == -1) {
      printf("No enough storage (local) in add_node_rep, add failed\n");
      return -1;
    }
  }
  return 1;
}
/***************
 * remove_node_rep
 * return 1 on success
 * return 0 on node does not exist
 * return -1 on no enough storage at remote or local
 ***********************/
int32_t InterNodeCommHandler::remove_node_rep(int32_t node) 
{
  /*
  int32_t retval;
  if(clientp != NULL) {
    transport->open();
    retval = clientp->remove_node_rep(node);
    if(retval == -1) {
      printf("No enough storage (remote) in remove_node_rep, remove failed\n");
      transport->close();
      return -1;
    }
    else if(retval == 0) {
      printf("node does not exist (remote) in remove_node_rep, remove failed\n");
      transport->close();
      return 0;
    }
    transport->close();
  }

  vector<u64> nodes;
  nodes.push_back((u64)node);
  if(!remove_node(storageLog.edge_list, nodes)) {
    printf("node does not exist (local) in remove_node_rep, remove failed\n");
    return 0;
  }
  if(storageLog.fd!=-1) 
    retval = storageLog.update_log(REMOVE_NODE, nodes[0], 0);
    if(retval == -1) {
      printf("No enough storage (local) in remove_node_rep, remove failed\n");
      return -1;
    }
  }
  */
  return 1;
}

/***************
 * add_edge_rep
 * return 1 on success
 * return 0 on edge already exist
 * return -1 on node_a_id == node_b_id or node does not exist
 * return -2 on no enough storage at remote or local
 ***********************/
int32_t InterNodeCommHandler::add_edge_rep(int32_t remotev, int32_t localu) 
{
  int32_t retval;
  retval = add_edge_rep_local(storageLog.edge_list, remotev, localu);
  if(retval!=1) {
    return retval;
  }

  if(storageLog.fd!=-1) {
    retval = storageLog.update_log(ADD_EDGE, localu, remotev);
    if(retval == -1) {
      printf("No enough storage (local) in remove_node_rep, remove failed\n");
      return -2;
    }
  }
  return 1;
}
/*
  if(clientp != NULL) {
    transport->open();
    retval = clientp->add_edge_rep(node1, node2);
    if(retval == -2) {
      printf("No enough storage (remote) in add_edge_rep, add failed\n");
      transport->close();
      return -2;
    }
    else if(retval == -1) {
      printf("node_a_id==node_b_id or node does not exist (remote) in add_edge_rep, add failed\n");
      transport->close();
      return -1;
    }
    else if(retval == 0) {
      printf("edge already exist (remote) in add_edge_rep, add failed\n");
      transport->close();
      return 0;
    }
    transport->close();
  }

  vector<u64> nodes;
  nodes.push_back((u64)node1);
  nodes.push_back((u64)node2);
  retval = add_edge(storageLog.edge_list, nodes);
  if(retval == -1) {
    printf("node_a_id==node_b_id or node does not exist (local) in add_edge_rep, add failed\n");
    return -1;
  }
  else if(retval == 0) {
    printf("edge already exist (local) in add_edge_rep, add failed\n");
    return 0;
  }

  if(storageLog.fd!=-1) {
    retval = storageLog.update_log(ADD_EDGE, nodes[0], nodes[1]);
    if(retval == -1) {
      printf("No enough storage (local) in add_edge_rep, add failed\n");
      return -2;
    }
  }
  return 1;
*/

/***************
 * remove_edge_rep
 * return 1 on success
 * return 0 on node or edge does not exist
 * return -1 on no enough storage at remote or local
 ***********************/
int32_t InterNodeCommHandler::remove_edge_rep(int32_t remotev, int32_t localu)
{
  int32_t retval;
  retval = remove_edge_rep_local(storageLog.edge_list, remotev, localu);
  if(retval!=1) {
    return retval;
  }

  if(storageLog.fd!=-1) {
    retval = storageLog.update_log(REMOVE_EDGE, localu, remotev);
    if(retval == -1) {
      printf("No enough storage (local) in remove_node_rep, remove failed\n");
      return -1;
    }
  }
  return 1;
  /*
  int32_t retval;
  if(clientp != NULL) {
    transport->open();
    retval = clientp->remove_edge_rep(node1, node2);
    if(retval == -1) {
      printf("No enough storage (remote) in remove_edge_rep, remove failed\n");
      transport->close();
      return -1;
    }
    else if(retval == 0) {
      printf("node does not exist (remote) in remove_edge_rep, remove failed\n");
      transport->close();
      return 0;
    }
    transport->close();
  }

  vector<u64> nodes;
  nodes.push_back((u64)node1);
  nodes.push_back((u64)node2);
  if(!remove_edge(storageLog.edge_list, nodes)) {
    printf("edge does not exist (local) in remove_edge_rep, remove failed\n");
    return 0;
  }
  if(storageLog.fd!=-1) {
    retval = storageLog.update_log(REMOVE_EDGE, nodes[0], nodes[1]);
    if(retval == -1) {
      printf("No enough storage (local) in remove_edge_rep, remove failed\n");
      return -1;
    }
  }
  return 1;
  */
}
/***********************************************
 * checkpoint_rep
 * return -1 if no space for checkpoint on remote or local
 * return 0 on success
 **********************************************/  
int32_t InterNodeCommHandler::checkpoint_rep() 
{
  int32_t retval;
  if(!clientp) {
    transport->open();
    retval = clientp->checkpoint_rep();
    if(retval == -1) {
      printf("No enough storage (remote) for check point in checkpoint_rep\n");
      transport->close();
      return -1;
    }
    transport->close();
  }
  if(storageLog.fd!=-1) {
    retval = storageLog.checkpoint(storageLog.fd);
    if(retval == -1) {
      printf("No enough storage (local) for check point in checkpoint_rep\n");
    }
  }
  return retval;
}





