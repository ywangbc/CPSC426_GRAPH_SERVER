#include "graph_util.h"


bool add_node(std::unordered_map<u64, std::unordered_set<u64>>& edge_list, const std::vector<u64>& args)
{
  my_graph_mutex.lock();
  //printf("In add_node\n");
  if(edge_list.find(args[0]) != edge_list.end())
  {
    my_graph_mutex.unlock();
    return false;
  }
  else
  {
    edge_list[args[0]];   
    my_graph_mutex.unlock();
    return true;
  }
}

/* return 1 on success
 * return 0 if edge already exist
 * return -1 if node_a_id = node_b_id or, or node does not exist
 * Notice this method can only be called in add_edge_remote, so it does not handle lock
 * */
int add_edge(std::unordered_map<u64, std::unordered_set<u64>>& edge_list, const std::vector<u64>& args)
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
/*********************************************
 * @param:
 *  partsize is the total number of partitions
 * @return
 *  true if vertex belongs to partnum
 *********************************************/
bool isMine(u64 partnum, u64 partsize, u64 vNum) {
  return vNum%partsize==partnum;
} 

/*********************************************
 * @param
 *  localu: a vertex known to be on local
 *  remotev: a vertex known to be on remote
 *********************************************/
void add_edge_local(std::unordered_map<u64, std::unordered_set<u64>>& edge_list, u64 localu, u64 remotev) 
{
   edge_list[localu].insert(remotev);
}

/* return 1 on success
 * return 0 if edge already exist
 * return -1 if node_a_id = node_b_id or, or node does not exist
 * return -2 if no enough storage on remote
 * */
int add_edge_remote(u64 partnum, u64 partsize, std::vector<boost::shared_ptr<apache::thrift::transport::TTransport>> all_transport_local,std::vector<boost::shared_ptr<InterNodeCommClient>> all_clientp, std::unordered_map<u64, std::unordered_set<u64>>& edge_list, const std::vector<u64>& args)
{
  int retval;
  if(isMine(partnum, partsize, args[0]) && isMine(partnum, partsize, args[1])) {
    my_graph_mutex.lock();
    retval = add_edge(edge_list, args);
    my_graph_mutex.unlock();
    return retval;
  }
  u64 localu, remotev;
  if(isMine(partnum, partsize, args[0])) {
    localu = args[0];
    remotev = args[1];
  }
  else if(isMine(partnum, partsize, args[1])) {
    localu = args[1];
    remotev = args[0];
  }
  else { //both vertices do not belong to this node, illegal operation
    return -1;
  }

  my_graph_mutex.lock();

  if(edge_list.find(localu) == edge_list.end())
  {
    my_graph_mutex.unlock();
    return -1;
  }
  else
  {
    if(edge_list[localu].find(remotev) != edge_list[localu].end())
    {
      my_graph_mutex.unlock();
      return 0;
    }
    else
    {
      u64 remote_part;
      remote_part = remotev % partsize;
      printf("In add edge, remotev %lu is in partition %lu\n", remotev, remote_part);
      all_transport_local[remote_part]->open();
      retval = all_clientp[remote_part]->add_edge_rep(localu, remotev); //always pass in order [local, remote]
      all_transport_local[remote_part]->close();
      if(retval != 1) {
        my_graph_mutex.unlock();
        return retval;
      }
      add_edge_local(edge_list, localu, remotev);
      my_graph_mutex.unlock();
      return 1;
    }
  }
}

int32_t add_edge_rep_local(std::unordered_map<u64, std::unordered_set<u64>>& edge_list, int32_t remotev, int32_t localu)
{
  my_graph_mutex.lock();

  if(edge_list.find(localu) == edge_list.end())
  {
    my_graph_mutex.unlock();
    return -1;
  }
  else
  {
    if(edge_list[localu].find(remotev) != edge_list[localu].end())
    {
      my_graph_mutex.unlock();
      return 0;
    }
    else
    {
      add_edge_local(edge_list, localu, remotev);
      my_graph_mutex.unlock();
      return 1;
    }
  }
}

//return false if remove failed, true if success
bool remove_node(std::unordered_map<u64, std::unordered_set<u64>>& edge_list, const std::vector<u64>& args)
{
  //printf("In remove_node\n");
  my_graph_mutex.lock();
  if(edge_list.find(args[0]) == edge_list.end())
  {
    my_graph_mutex.unlock();
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
    my_graph_mutex.unlock();
    return true;
  }
}

/* @return
 *  1 on success
 *  0 if edge or node does not exist 
 * Notice this method can only be called in remove_edge_remote, so it does not handle lock
 */
int32_t remove_edge(std::unordered_map<u64, std::unordered_set<u64>>& edge_list, const std::vector<u64>& args)
{
  //printf("In remove_edge\n");
  if(edge_list.find(args[0]) == edge_list.end())
  {
    return 0;
  }
  else if(edge_list.find(args[1]) == edge_list.end())
  {
    return 0;
  }
  else
  {
    if(edge_list[args[0]].find(args[1]) == edge_list[args[0]].end())
    {
      return 1;
    }
    else
    {
      edge_list[args[0]].erase(args[1]);
      edge_list[args[1]].erase(args[0]);
      return 1;
    }
  }
}
/*********************************************
 * @param
 *  localu: a vertex known to be on local
 *  remotev: a vertex known to be on remote
 *********************************************/
void remove_edge_local(std::unordered_map<u64, std::unordered_set<u64>>& edge_list, u64 localu, u64 remotev) 
{
   edge_list[localu].erase(remotev);
}

/* @return 
 *  1 on success
 *  0 if edge already exist
 *  -1 if no enough storage
 * */
int32_t remove_edge_remote(u64 partnum, u64 partsize, std::vector<boost::shared_ptr<apache::thrift::transport::TTransport>> all_transport_local, std::vector<boost::shared_ptr<InterNodeCommClient>> all_clientp, std::unordered_map<u64, std::unordered_set<u64>>& edge_list, const std::vector<u64>& args)
{
  int32_t retval;
  if(isMine(partnum, partsize, args[0]) && isMine(partnum, partsize, args[1])) {
    my_graph_mutex.lock();
    retval = remove_edge(edge_list, args);
    my_graph_mutex.unlock();
    return retval;
  }
  u64 localu, remotev;
  if(isMine(partnum, partsize, args[0])) {
    localu = args[0];
    remotev = args[1];
  }
  else if(isMine(partnum, partsize, args[1])) {
    localu = args[1];
    remotev = args[0];
  }
  else { //both vertices do not belong to this node, illegal operation
    return 0;
  }

  my_graph_mutex.lock();

  if(edge_list.find(localu) == edge_list.end())
  {
    my_graph_mutex.unlock();
    return 0;
  }
  else
  {
    u64 remote_part;
    remote_part = remotev % partsize;
    printf("In remove edge, remotev %lu is in partition %lu\n", remotev, remote_part);
    all_transport_local[remote_part]->open();
    retval = all_clientp[remote_part]->remove_edge_rep(localu, remotev); //always pass in order [local, remote]
    all_transport_local[remote_part]->close();
    if(retval != 1) {
      my_graph_mutex.unlock();
      return retval;
    }
    remove_edge_local(edge_list, localu, remotev);
    my_graph_mutex.unlock();
    return 1;
  }
}

int32_t remove_edge_rep_local(std::unordered_map<u64, std::unordered_set<u64>>& edge_list, int32_t remotev, int32_t localu)
{
  my_graph_mutex.lock();

  if(edge_list.find(localu) == edge_list.end())
  {
    my_graph_mutex.unlock();
    return 0;
  }
  else
  {
    if(edge_list[localu].find(remotev) == edge_list[localu].end())
    {
      my_graph_mutex.unlock();
      return 0;
    }
    else
    {
      remove_edge_local(edge_list, localu, remotev);
      my_graph_mutex.unlock();
      return 1;
    }
  }
}

/* return true if the node is in the graph
 * otherwise return false
 */
bool get_node(std::unordered_map<u64, std::unordered_set<u64>>& edge_list, const std::vector<u64>& args)
{
  printf("In correct get_node\n");
  my_graph_mutex.lock();
  if(edge_list.find(args[0]) == edge_list.end())
  {
    my_graph_mutex.unlock();
    return false;
  }
  else
  {
    my_graph_mutex.unlock();
    return true;
  }
}

/* return 1 if the edge is in the graph
 * return 0 if the edge is not in the graph
 * if any of the node is not in the graph, return -1
 */
int get_edge(std::vector<boost::shared_ptr<apache::thrift::transport::TTransport>> all_transport_local, std::vector<boost::shared_ptr<InterNodeCommClient>> all_clientp, u64 partnum, u64 partsize, std::unordered_map<u64, std::unordered_set<u64>>& edge_list, const std::vector<u64>& args)
{
  //Here remotev is not necessarily remote, we only need to ensure localu is local
  u64 localu, remotev;
  if(isMine(partnum, partsize, args[0])) {
    printf("Node %lu belongs to this partition %lu\n", args[0], partnum);
    localu = args[0];
    remotev = args[1]; 
  }
  else if(isMine(partnum, partsize, args[1])) {
    printf("Node %lu belongs to this partition %lu\n", args[0], partnum);
    localu = args[1];
    remotev = args[0];
  }
  else { //both vertices do not belong to this partition, illegal operation
    printf("Illegal operation: both vertices are in this partition!\n");
    return -1;
  }

  //printf("In get_edge\n");
  my_graph_mutex.lock();
  int32_t rep;
  u64 remote_part;
  remote_part=remotev%partsize;
  printf("In get edge, remote node %lu is in partition %lu\n", remotev, remote_part);
  all_transport_local[remote_part]->open();
  rep = all_clientp[remote_part]->get_node_rep(remotev);
  all_transport_local[remote_part]->close();
  if(rep == 0) {
    printf("Remote partition does not have node: %lu\n", remotev);
    my_graph_mutex.unlock();
    return -1;
  }
  if(edge_list.find(localu) == edge_list.end())
  {
    printf("Current partition does not have node: %lu\n", localu);
    my_graph_mutex.unlock();
    return -1;
  }
  else if(edge_list[localu].find(remotev) == edge_list[localu].end())
  {
    my_graph_mutex.unlock();
    return 0;
  }
  else //The edge exist
  {
    my_graph_mutex.unlock();
    return 1;
  }
}

/* return false if the node does not exist
 * return true and put neighbours into list if exist
 */
bool get_neighbors(std::unordered_map<u64, std::unordered_set<u64>>& edge_list, const std::vector<u64>& args, std::vector<u64>& list)
{
  my_graph_mutex.lock();
  //printf("In get_neighbors\n");
  if(edge_list.find(args[0]) == edge_list.end())
  {
    my_graph_mutex.unlock();
    return false;
  }
  else
  {
    list.insert(list.end(), edge_list[args[0]].begin(), edge_list[args[0]].end());
    my_graph_mutex.unlock();
    return true;
  }
}

/* return -1 if there is no path
 * return -2 if either node does not exist
 * return the shortest distance otherwise 
 */
int shortest_path(std::unordered_map<u64, std::unordered_set<u64>>& edge_list, const std::vector<u64>& args)
{
  //printf("In shortest_path\n");
  my_graph_mutex.lock();
  if(edge_list.find(args[0]) == edge_list.end())
  {
    my_graph_mutex.unlock();
    return -2;
  }
  else if(edge_list.find(args[1]) == edge_list.end())
  {
    my_graph_mutex.unlock();
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
      my_graph_mutex.unlock();
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
  my_graph_mutex.unlock();
  return -1;
}

