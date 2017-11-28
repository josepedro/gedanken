import numpy as np

def get1(line, row):
	return [line - 1, row]

def get2(line, row):
	return [line - 1, row + 1]

def get3(line, row):
	return [line, row + 1]

def get4(line, row):
	return [line + 1, row + 1]

def get5(line, row):
	return [line + 1, row]

def get6(line, row):
	return [line + 1, row - 1]

def get7(line, row):
	return [line, row - 1]

def get8(line, row):
	return [line - 1, row - 1]

file_mesh_name = "mesh_2.txt" 
mesh_file = open(file_mesh_name, "r")

content = mesh_file.read()
elements_mesh = content.replace("\n", " ").split(" ")[0:-1] 
print elements_mesh
Y = content.count("\n")
X = len(elements_mesh)/Y
print X
print Y

# build Matrix
matrix = np.loadtxt(file_mesh_name)
print matrix

# builds matrix ids
matrix_ids = np.copy(matrix)
number_total_elements = matrix_ids.shape[0] * matrix_ids.shape[1]
print number_total_elements
#------------------------------------------

# build cell string
element_id = 1
cells_string = "val cells = sc.parallelize(Array("
for i in range(0,matrix_ids.shape[0]):
	for j in range(0,matrix_ids.shape[1]):
		matrix_ids[i][j] = element_id
		cells_string += "(%dL,%f)" % (element_id, matrix[i][j])
		if element_id != number_total_elements:
			cells_string += ","
		element_id += 1
cells_string += "))"
print cells_string


print matrix_ids
# ---------------------------------------------------------

# locate vertices


# neighbors

# neighbors set 1 => 3 elements neighbors
string_neighbor = "val relationshipCells = sc.parallelize(Array("
# first element of set 1
element_id = matrix_ids[0][0]
neighbor_1 = matrix_ids[0][1]
direction = 3
string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_1,direction)
neighbor_2 = matrix_ids[1][1]
direction = 4
string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_2,direction)
neighbor_3 = matrix_ids[1][0]
direction = 5
string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_3,direction)

# second element of set 1
element_id = matrix_ids[0][matrix_ids.shape[1] - 1]
neighbor_1 = matrix_ids[1][matrix_ids.shape[1] - 1]
direction = 5
string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_1,direction)
neighbor_2 = matrix_ids[1][matrix_ids.shape[1] - 2]
direction = 6
string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_2,direction)
neighbor_3 = matrix_ids[0][matrix_ids.shape[1] - 2]
direction = 7
string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_3,direction)

# third element of set 1
element_id = matrix_ids[matrix_ids.shape[0] - 1][matrix_ids.shape[1] - 1]
neighbor_1 = matrix_ids[matrix_ids.shape[0] - 1][matrix_ids.shape[1] - 2]
direction = 7
string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_1,direction)
neighbor_2 = matrix_ids[matrix_ids.shape[0] - 2][matrix_ids.shape[1] - 2]
direction = 8
string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_2,direction)
neighbor_3 = matrix_ids[matrix_ids.shape[0] - 2][matrix_ids.shape[1] - 1]
direction = 1
string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_3,direction)

# fourth element of set 1
element_id = matrix_ids[matrix_ids.shape[0] - 1][0]
neighbor_1 = matrix_ids[matrix_ids.shape[0] - 2][0]
direction = 1
string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_1,direction)
neighbor_2 = matrix_ids[matrix_ids.shape[0] - 2][1]
direction = 2
string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_2,direction)
neighbor_3 = matrix_ids[matrix_ids.shape[0] - 1][1]
direction = 3
string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_3,direction)


# neighbors neighbors set 2 => 5 elements neighbors
# up = e1
for e1 in range(1,matrix_ids.shape[1]-1):
	line = 0
	row = e1
	element_id = matrix_ids[line][row]
	neighbor_1 = matrix_ids[line][row + 1]
	direction = 3
	string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_1,direction)
	neighbor_2 = matrix_ids[line + 1][row + 1]
	direction = 4
	string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_2,direction)
	neighbor_3 = matrix_ids[line + 1][row]
	direction = 5
	string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_3,direction)
	neighbor_4 = matrix_ids[line + 1][row - 1]
	direction = 6
	string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_4,direction)
	neighbor_5 = matrix_ids[line][row - 1]
	direction = 7
	string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_5,direction)

# right = e2
for e2 in range(1,matrix_ids.shape[0]-1):
	line = e2
	row = matrix_ids.shape[1]-1
	element_id = matrix_ids[line][row]
	neighbor_1 = matrix_ids[get5(line,row)[0]][get5(line,row)[1]]
	direction = 5
	string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_1,direction)
	neighbor_2 = matrix_ids[get6(line,row)[0]][get6(line,row)[1]]
	direction = 6
	string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_2,direction)
	neighbor_3 = matrix_ids[get7(line,row)[0]][get7(line,row)[1]]
	direction = 7
	string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_3,direction)
	neighbor_4 = matrix_ids[get8(line,row)[0]][get8(line,row)[1]]
	direction = 8
	string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_4,direction)
	neighbor_5 = matrix_ids[get1(line,row)[0]][get1(line,row)[1]]
	direction = 1
	string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_5,direction)

# down = e3
for e3 in range(1,matrix_ids.shape[1]-1):
	line = matrix_ids.shape[0]-1
	row = e3
	element_id = matrix_ids[line][row]
	neighbor_1 = matrix_ids[get7(line,row)[0]][get7(line,row)[1]]
	direction = 7
	string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_1,direction)
	neighbor_2 = matrix_ids[get8(line,row)[0]][get8(line,row)[1]]
	direction = 8
	string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_2,direction)
	neighbor_3 = matrix_ids[get1(line,row)[0]][get1(line,row)[1]]
	direction = 1
	string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_3,direction)
	neighbor_4 = matrix_ids[get2(line,row)[0]][get2(line,row)[1]]
	direction = 2
	string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_4,direction)
	neighbor_5 = matrix_ids[get3(line,row)[0]][get3(line,row)[1]]
	direction = 3
	string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_5,direction)

# left = e4
for e4 in range(1,matrix_ids.shape[0]-1):
	line = e4
	row = 0
	element_id = matrix_ids[line][row]
	neighbor_1 = matrix_ids[get1(line,row)[0]][get1(line,row)[1]]
	direction = 1
	string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_1,direction)
	neighbor_2 = matrix_ids[get2(line,row)[0]][get2(line,row)[1]]
	direction = 2
	string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_2,direction)
	neighbor_3 = matrix_ids[get3(line,row)[0]][get3(line,row)[1]]
	direction = 3
	string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_3,direction)
	neighbor_4 = matrix_ids[get4(line,row)[0]][get4(line,row)[1]]
	direction = 4
	string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_4,direction)
	neighbor_5 = matrix_ids[get5(line,row)[0]][get5(line,row)[1]]
	direction = 5
	string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_5,direction)

# neighbors neighbors set 3 => 8 elements neighbors
for line in range(1,matrix_ids.shape[0] - 1):
	for row in range(1,matrix_ids.shape[1] - 1):
		element_id = matrix_ids[line][row]
		neighbor_1 = matrix_ids[get1(line,row)[0]][get1(line,row)[1]]
		direction = 1
		string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_1,direction)
		neighbor_2 = matrix_ids[get2(line,row)[0]][get2(line,row)[1]]
		direction = 2
		string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_2,direction)
		neighbor_3 = matrix_ids[get3(line,row)[0]][get3(line,row)[1]]
		direction = 3
		string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_3,direction)
		neighbor_4 = matrix_ids[get4(line,row)[0]][get4(line,row)[1]]
		direction = 4
		string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_4,direction)
		neighbor_5 = matrix_ids[get5(line,row)[0]][get5(line,row)[1]]
		direction = 5
		string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_5,direction)
		neighbor_6 = matrix_ids[get6(line,row)[0]][get6(line,row)[1]]
		direction = 6
		string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_6,direction)
		neighbor_7 = matrix_ids[get7(line,row)[0]][get7(line,row)[1]]
		direction = 7
		string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_7,direction)
		neighbor_8 = matrix_ids[get8(line,row)[0]][get8(line,row)[1]]
		direction = 8
		string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_8,direction)
		
string_neighbor = string_neighbor[0:-1]
string_neighbor += "))"
print string_neighbor

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
    print elements_mesh[element_count], "(2): ", elements_mesh[((eq_2)-1 + element_count) % len(elements_mesh)]
    print elements_mesh[element_count], "(3): ", elements_mesh[((eq_3)-1 + element_count) % len(elements_mesh)]
    print elements_mesh[element_count], "(4): ", elements_mesh[((eq_4)-1 + element_count) % len(elements_mesh)]
    print elements_mesh[element_count], "(5): ", elements_mesh[((eq_5)-1 + element_count) % len(elements_mesh)]
    print elements_mesh[element_count], "(6): ", elements_mesh[((eq_6)-1 + element_count) % len(elements_mesh)]
    print elements_mesh[element_count], "(7): ", elements_mesh[((eq_7)-1 + element_count) % len(elements_mesh)]
    print elements_mesh[element_count], "(8): ", elements_mesh[((eq_8)-1 + element_count) % len(elements_mesh)]

    element_count += 1'''
