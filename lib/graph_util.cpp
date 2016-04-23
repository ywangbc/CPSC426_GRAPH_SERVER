#include "graph_util.h"


bool add_node(std::unordered_map<u64, std::unordered_set<u64>>& edge_list, const std::vector<u64>& args)
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

//return false if remove failed, true if success
bool remove_node(std::unordered_map<u64, std::unordered_set<u64>>& edge_list, const std::vector<u64>& args)
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
bool remove_edge(std::unordered_map<u64, std::unordered_set<u64>>& edge_list, const std::vector<u64>& args)
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
bool get_node(std::unordered_map<u64, std::unordered_set<u64>>& edge_list, const std::vector<u64>& args)
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
int get_edge(std::unordered_map<u64, std::unordered_set<u64>>& edge_list, const std::vector<u64>& args)
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
bool get_neighbors(std::unordered_map<u64, std::unordered_set<u64>>& edge_list, const std::vector<u64>& args, std::vector<u64>& list)
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
int shortest_path(std::unordered_map<u64, std::unordered_set<u64>>& edge_list, const std::vector<u64>& args)
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

