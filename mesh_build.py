import numpy as np

mesh_file = open("mesh.txt", "r")

content = mesh_file.read()
elements_mesh = content.replace("\n", " ").split(" ")[0:-1] 
print elements_mesh
Y = content.count("\n")
X = len(elements_mesh)/Y
print X
print Y

# build Matrix
matrix = np.loadtxt('mesh.txt')
print matrix

# builds matrix ids
matrix_ids = np.copy(matrix)
number_total_elements = matrix_ids.shape[0] * matrix_ids.shape[1]
print number_total_elements
#------------------------------------------

# build cell string
element_id = 1
cells_string = "Array("
for i in range(0,matrix_ids.shape[0]):
	for j in range(0,matrix_ids.shape[1]):
		matrix_ids[i][j] = element_id
		cells_string += "(%dL,%f)" % (element_id, matrix[i][j])
		if element_id != number_total_elements:
			cells_string += ","
		element_id += 1
cells_string += ")"
print cells_string


print matrix_ids
# ---------------------------------------------------------

# locate vertices


# neighbors

# neighbors set 1 => 3 elements neighbors
string_neighbor = ""
# first element of set 1
element_id = matrix_ids[0][0]
neighbor_1 = matrix_ids[0][1]
direction = 3
string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_1,direction)
print string_neighbor
neighbor_2 = matrix_ids[1][1]
direction = 4
string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_2,direction)
print string_neighbor
neighbor_3 = matrix_ids[1][0]
direction = 5
string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_3,direction)
print string_neighbor

# second element of set 1
element_id = matrix_ids[0][matrix_ids.shape[1] - 1]
neighbor_1 = matrix_ids[1][matrix_ids.shape[1] - 1]
direction = 5
string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_1,direction)
print string_neighbor
neighbor_2 = matrix_ids[1][matrix_ids.shape[1] - 2]
direction = 6
string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_2,direction)
print string_neighbor
neighbor_3 = matrix_ids[0][matrix_ids.shape[1] - 2]
direction = 7
string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_3,direction)
print string_neighbor  

# third element of set 1
element_id = matrix_ids[matrix_ids.shape[0] - 1][matrix_ids.shape[1] - 1]
neighbor_1 = matrix_ids[matrix_ids.shape[0] - 1][matrix_ids.shape[1] - 2]
direction = 7
string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_1,direction)
print string_neighbor
neighbor_2 = matrix_ids[matrix_ids.shape[0] - 2][matrix_ids.shape[1] - 2]
direction = 8
string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_2,direction)
print string_neighbor
neighbor_3 = matrix_ids[matrix_ids.shape[0] - 2][matrix_ids.shape[1] - 1]
direction = 1
string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_3,direction)
print string_neighbor

# fourth element of set 2
element_id = matrix_ids[matrix_ids.shape[0] - 1][0]
neighbor_1 = matrix_ids[matrix_ids.shape[0] - 2][0]
direction = 1
string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_1,direction)
print string_neighbor
neighbor_2 = matrix_ids[matrix_ids.shape[0] - 2][1]
direction = 2
string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_2,direction)
print string_neighbor
neighbor_3 = matrix_ids[matrix_ids.shape[0] - 1][1]
direction = 3
string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_3,direction)
print string_neighbor

# neighbors
'''eq_1 = X*Y - (X - 1)
eq_2 = eq_1 + 1
eq_3 = 2
eq_4 = X+2
eq_5 = X+1
eq_6 = 2*X
eq_7 = X
eq_8 = X*Y
element_count = 0'''
'''while element_count < len(elements_mesh):
   print elements_mesh[element_count], "(1): ", elements_mesh[((eq_1)-1 + element_count) % len(elements_mesh)]
    print elements_mesh[element_count], "(2): ", elements_mesh[((eq_2)-1 + element_count) % len(elements_mesh)]
    print elements_mesh[element_count], "(3): ", elements_mesh[((eq_3)-1 + element_count) % len(elements_mesh)]
    print elements_mesh[element_count], "(4): ", elements_mesh[((eq_4)-1 + element_count) % len(elements_mesh)]
    print elements_mesh[element_count], "(5): ", elements_mesh[((eq_5)-1 + element_count) % len(elements_mesh)]
    print elements_mesh[element_count], "(6): ", elements_mesh[((eq_6)-1 + element_count) % len(elements_mesh)]
    print elements_mesh[element_count], "(7): ", elements_mesh[((eq_7)-1 + element_count) % len(elements_mesh)]
    print elements_mesh[element_count], "(8): ", elements_mesh[((eq_8)-1 + element_count) % len(elements_mesh)]

    element_count += 1'''
