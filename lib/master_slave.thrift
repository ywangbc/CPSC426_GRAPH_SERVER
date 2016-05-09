service InterNodeComm {
  i32 get_node_rep(1:i32 node);
  i32 add_node_rep(1:i32 node);
  i32 remove_node_rep(1:i32 node);
  i32 add_edge_rep(1:i32 node1, 2:i32 node2);
  i32 remove_edge_rep(1:i32 node1, 2:i32 node2);
  i32 checkpoint_rep();
}
