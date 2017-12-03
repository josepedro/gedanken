#!/usr/bin/python

import sys
import numpy as np

if __name__ == "__main__":
    file_name_result = sys.argv[1]
    number_lines = int(sys.argv[2])
    number_rows = int(sys.argv[3])
    mesh_file = open(file_name_result, "r")
    content = mesh_file.read()
    content = content.replace('(','')
    content = content.replace(')','')
    nodes = content.split('\n')
    nodes_map = {}
    total_nodes = len(nodes)
    for node in nodes:
       node_splited = node.split(',')
       try:
            nodes_map[node_splited[0]] = node_splited[1]
       except:
           print ''
    
    nodes_density = np.array([]) 
    for node in range(1, total_nodes):
        nodes_density = np.append(nodes_density, nodes_map[str(node)])

    print nodes_density.reshape((number_lines,number_rows))
