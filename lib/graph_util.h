#ifndef GRAPH_UTIL_H
#define GRAPH_UTIL_H

#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <queue>
#include <mutex>
#include "disk_lib.h"
#include <thrift/transport/TTransportUtils.h>
#include "gen-cpp/InterNodeComm.h"

static std::mutex my_graph_mutex;

bool add_node(std::unordered_map<u64, std::unordered_set<u64>>& edge_list, const std::vector<u64>& args);

/* return 1 on success
 * return 0 if edge already exist
 * return -1 if node_a_id = node_b_id or, or node does not exist
 * */
int32_t add_edge(std::unordered_map<u64, std::unordered_set<u64>>& edge_list, const std::vector<u64>& args);

/* return 1 on success
 * return 0 if edge already exist
 * return -1 if node_a_id = node_b_id or, or node does not exist
 * */
int add_edge_remote(u64 partnum, u64 partsize, boost::shared_ptr<apache::thrift::transport::TTransport> transport_local,boost::shared_ptr<InterNodeCommClient> clientp, std::unordered_map<u64, std::unordered_set<u64>>& edge_list, const std::vector<u64>& args);

/*******************************
 * Get called as by add_edge_rep
 ******************************/
int32_t add_edge_rep_local(std::unordered_map<u64, std::unordered_set<u64>>& edge_list, int32_t remotev, int32_t localu);
//return false if remove failed, true if success
bool remove_node(std::unordered_map<u64, std::unordered_set<u64>>& edge_list, const std::vector<u64>& args);

/* return true on success
 * return false if edge does not exist 
 */
int32_t remove_edge(std::unordered_map<u64, std::unordered_set<u64>>& edge_list, const std::vector<u64>& args);

/* @return 
 *  1 on success
 *  0 if edge already exist
 *  -1 if no enough storage
 * */
int32_t remove_edge_remote(u64 partnum, u64 partsize, boost::shared_ptr<apache::thrift::transport::TTransport> transport_local,boost::shared_ptr<InterNodeCommClient> clientp, std::unordered_map<u64, std::unordered_set<u64>>& edge_list, const std::vector<u64>& args);

/*******************************
 * Get called as by remove_edge_rep
 ******************************/
int32_t remove_edge_rep_local(std::unordered_map<u64, std::unordered_set<u64>>& edge_list, int32_t remotev, int32_t localu);

/* return true if the node is in the graph
 * otherwise return false
 */
bool get_node(std::unordered_map<u64, std::unordered_set<u64>>& edge_list, const std::vector<u64>& args);

/* return 1 if the edge is in the graph
 * return 0 if the edge is not in the graph
 * if any of the node is not in the graph, return -1
 */
int get_edge(std::vector<boost::shared_ptr<apache::thrift::transport::TTransport>> all_transport_local, std::vector<boost::shared_ptr<InterNodeCommClient>> all_clientp, u64 partnum, u64 partsize, std::unordered_map<u64, std::unordered_set<u64>>& edge_list, const std::vector<u64>& args);

/* return false if the node does not exist
 * return true and put neighbours into list if exist
 */
bool get_neighbors(std::unordered_map<u64, std::unordered_set<u64>>& edge_list, const std::vector<u64>& args, std::vector<u64>& list);

/* return -1 if there is no path
 * return -2 if either node does not exist
 * return the shortest distance otherwise 
 */
int shortest_path(std::unordered_map<u64, std::unordered_set<u64>>& edge_list, const std::vector<u64>& args);
#endif
