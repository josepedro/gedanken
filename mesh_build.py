import numpy as np
import sys

def get1(line, row, number_lines, number_rows):
	if line == 0 and row == 0:
		return [number_lines - 1, row]
	if line == 0 and row == number_rows - 1:
		return [number_lines - 1, number_rows - 1]
	if line == 0 and row > 0 and row < number_rows - 1:
		return [number_lines - 1, row]
	else:
		return [line - 1, row]

def get2(line, row, number_lines, number_rows):
	if line == 0 and row == 0:
		return [number_lines - 1, row + 1]
	if line == 0 and row == number_rows - 1:
		return [number_lines - 1, 0]
	if line == number_lines - 1 and row == number_rows - 1:
		return [number_lines - 2, 0]
	if line == 0 and row > 0 and row < number_rows - 1:
		return [number_lines - 1, row + 1]
	if line > 0 and line < number_lines - 1 and row == number_rows - 1:
		return [line - 1, 0]
	else:
		return [line - 1, row + 1]

def get3(line, row, number_lines, number_rows):
	if line == 0 and row == number_rows - 1:
		return [line, 0]
	if line == number_lines - 1 and row == number_rows - 1:
		return [number_lines - 1, 0]
	if line > 0 and line < number_lines - 1 and row == number_rows - 1:
		return [line, 0]
	else:	
		return [line, row + 1]

def get4(line, row, number_lines, number_rows):
	if line == 0 and row == number_rows - 1:
		return [1, 0]
	if line == number_lines - 1 and row == number_rows - 1:
		return [0, 0]
	if line == number_lines - 1 and row == 0:
		return [0, 1]
	if line > 0 and line < number_lines - 1 and row == number_rows - 1:
		return [line + 1, 0]
	if line == number_lines - 1 and row > 0 and row < number_rows - 1:
		return [0, row + 1]  
	else:
		return [line + 1, row + 1]

def get5(line, row, number_lines, number_rows):
	if line == number_lines - 1 and row == 0:
		return [0, row]
	if line == number_lines - 1 and row == number_rows - 1:
		return [0, row]
	if line == number_lines - 1 and row > 0 and row < number_rows - 1:
		return [0, row]
	else:	
		return [line + 1, row]

def get6(line, row, number_lines, number_rows):
	if line == 0 and row == 0:
		return [line + 1, number_rows - 1]
	if line ==  number_lines - 1 and row == 0:
		return [0, number_rows - 1]
	if line == number_lines - 1 and row == number_rows - 1:
		return [0, row - 1]
	if line > 0 and line < number_lines - 1 and row == 0:
		return [line + 1, number_rows - 1]
	if line == number_lines - 1 and row > 0 and row < number_rows - 1:
		return [0, row - 1] 
	else:	
		return [line + 1, row - 1]

def get7(line, row, number_lines, number_rows):
	if line == 0 and row == 0:
		return [line, number_rows - 1]
	if line == number_lines - 1 and row == 0:
		return [line, number_rows - 1]
	if line > 0 and line < number_lines - 1 and row == 0:
		return [line, number_rows - 1]
	else:
		return [line, row - 1]

def get8(line, row, number_lines, number_rows):
	if line == 0 and row == 0:
		return [number_lines - 1, number_rows - 1]
	if line == 0 and row == number_rows - 1:
		return [number_lines - 1, row - 1]
	if line == number_lines - 1 and row == 0:
		return [line - 1, number_rows - 1]
	if line > 0 and line < number_lines - 1 and row == 0:
		return [line - 1, number_rows - 1]
	if line == 0 and row > 0 and row < number_rows - 1:
		return [number_lines - 1, row - 1]
	else:
		return [line - 1, row - 1]

file_mesh_name = sys.argv[1] 
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
number_lines = matrix_ids.shape[0]
number_rows = matrix_ids.shape[1]
print number_total_elements
#------------------------------------------

# build cell string
element_id = 1
cells_string = "val cells = sc.parallelize(Array("
for i in range(0,matrix_ids.shape[0]):
	for j in range(0,matrix_ids.shape[1]):
		matrix_ids[i][j] = element_id
		density = matrix[i][j]/9.

		cells_string += "(%dL,Map(" % (element_id)
		cells_string += "'1' -> %f," % (density)
		cells_string += "'2' -> %f," % (density)
		cells_string += "'3' -> %f," % (density)
		cells_string += "'4' -> %f," % (density)
		cells_string += "'5' -> %f," % (density)
		cells_string += "'6' -> %f," % (density)
		cells_string += "'7' -> %f," % (density)
		cells_string += "'8' -> %f," % (density)
		cells_string += "'9' -> %f))" % (density)


		if element_id != number_total_elements:
			cells_string += ","
		element_id += 1
cells_string += "))"
print cells_string

# ---------------------------------------------------------

# neighbors

# neighbors set 1 => 3 elements neighbors
string_neighbor = "val relationshipCells = sc.parallelize(Array("
# first element of set 1
line = 0
row = 0
element_id = matrix_ids[line][row]
# ---
direction = 1
neighbor_1 = matrix_ids[get1(line,row,number_lines,number_rows)[0]][get1(line,row,number_lines,number_rows)[1]]
string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_1,direction)
# ---
direction = 2
neighbor_2 = matrix_ids[get2(line,row,number_lines,number_rows)[0]][get2(line,row,number_lines,number_rows)[1]]
string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_2,direction)
# ---
direction = 3
neighbor_3 = matrix_ids[get3(line,row,number_lines,number_rows)[0]][get3(line,row,number_lines,number_rows)[1]]
string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_3,direction)
# ---
direction = 4
neighbor_4 = matrix_ids[get4(line,row,number_lines,number_rows)[0]][get4(line,row,number_lines,number_rows)[1]]
string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_4,direction)
# ---
direction = 5
neighbor_5 = matrix_ids[get5(line,row,number_lines,number_rows)[0]][get5(line,row,number_lines,number_rows)[1]]
string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_5,direction)
# ---
direction = 6
neighbor_6 = matrix_ids[get6(line,row,number_lines,number_rows)[0]][get6(line,row,number_lines,number_rows)[1]]
string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_6,direction)
# ---
direction = 7
neighbor_7 = matrix_ids[get7(line,row,number_lines,number_rows)[0]][get7(line,row,number_lines,number_rows)[1]]
string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_7,direction)
# ---
direction = 8
neighbor_8 = matrix_ids[get8(line,row,number_lines,number_rows)[0]][get8(line,row,number_lines,number_rows)[1]]
string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_8,direction)
# ---
# second element of set 1
line = 0
row = matrix_ids.shape[1] - 1
element_id = matrix_ids[line][row]
# ---
# ---
direction = 1
neighbor_1 = matrix_ids[get1(line,row,number_lines,number_rows)[0]][get1(line,row,number_lines,number_rows)[1]]
string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_1,direction)
# ---
direction = 2
neighbor_2 = matrix_ids[get2(line,row,number_lines,number_rows)[0]][get2(line,row,number_lines,number_rows)[1]]
string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_2,direction)
# ---
direction = 3
neighbor_3 = matrix_ids[get3(line,row,number_lines,number_rows)[0]][get3(line,row,number_lines,number_rows)[1]]
string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_3,direction)
# ---
direction = 4
neighbor_4 = matrix_ids[get4(line,row,number_lines,number_rows)[0]][get4(line,row,number_lines,number_rows)[1]]
string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_4,direction)
# ---
direction = 5
neighbor_5 = matrix_ids[get5(line,row,number_lines,number_rows)[0]][get5(line,row,number_lines,number_rows)[1]]
string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_5,direction)
# ---
direction = 6
neighbor_6 = matrix_ids[get6(line,row,number_lines,number_rows)[0]][get6(line,row,number_lines,number_rows)[1]]
string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_6,direction)
# ---
direction = 7
neighbor_7 = matrix_ids[get7(line,row,number_lines,number_rows)[0]][get7(line,row,number_lines,number_rows)[1]]
string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_7,direction)
# ---
direction = 8
neighbor_8 = matrix_ids[get8(line,row,number_lines,number_rows)[0]][get8(line,row,number_lines,number_rows)[1]]
string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_8,direction)
# ---
# third element of set 1
line = matrix_ids.shape[0] - 1
row = matrix_ids.shape[1] - 1
element_id = matrix_ids[line][row]
direction = 1
neighbor_1 = matrix_ids[get1(line,row,number_lines,number_rows)[0]][get1(line,row,number_lines,number_rows)[1]]
string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_1,direction)
# ---
direction = 2
neighbor_2 = matrix_ids[get2(line,row,number_lines,number_rows)[0]][get2(line,row,number_lines,number_rows)[1]]
string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_2,direction)
# ---
direction = 3
neighbor_3 = matrix_ids[get3(line,row,number_lines,number_rows)[0]][get3(line,row,number_lines,number_rows)[1]]
string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_3,direction)
# ---
direction = 4
neighbor_4 = matrix_ids[get4(line,row,number_lines,number_rows)[0]][get4(line,row,number_lines,number_rows)[1]]
string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_4,direction)
# ---
direction = 5
neighbor_5 = matrix_ids[get5(line,row,number_lines,number_rows)[0]][get5(line,row,number_lines,number_rows)[1]]
string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_5,direction)
# ---
direction = 6
neighbor_6 = matrix_ids[get6(line,row,number_lines,number_rows)[0]][get6(line,row,number_lines,number_rows)[1]]
string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_6,direction)
# ---
direction = 7
neighbor_7 = matrix_ids[get7(line,row,number_lines,number_rows)[0]][get7(line,row,number_lines,number_rows)[1]]
string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_7,direction)
# ---
direction = 8
neighbor_8 = matrix_ids[get8(line,row,number_lines,number_rows)[0]][get8(line,row,number_lines,number_rows)[1]]
string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_8,direction)
# ---

# fourth element of set 1
line = matrix_ids.shape[0] - 1
row = 0
element_id = matrix_ids[line][row]
direction = 1
neighbor_1 = matrix_ids[get1(line,row,number_lines,number_rows)[0]][get1(line,row,number_lines,number_rows)[1]]
string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_1,direction)
# ---
direction = 2
neighbor_2 = matrix_ids[get2(line,row,number_lines,number_rows)[0]][get2(line,row,number_lines,number_rows)[1]]
string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_2,direction)
# ---
direction = 3
neighbor_3 = matrix_ids[get3(line,row,number_lines,number_rows)[0]][get3(line,row,number_lines,number_rows)[1]]
string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_3,direction)
# ---
direction = 4
neighbor_4 = matrix_ids[get4(line,row,number_lines,number_rows)[0]][get4(line,row,number_lines,number_rows)[1]]
string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_4,direction)
# ---
direction = 5
neighbor_5 = matrix_ids[get5(line,row,number_lines,number_rows)[0]][get5(line,row,number_lines,number_rows)[1]]
string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_5,direction)
# ---
direction = 6
neighbor_6 = matrix_ids[get6(line,row,number_lines,number_rows)[0]][get6(line,row,number_lines,number_rows)[1]]
string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_6,direction)
# ---
direction = 7
neighbor_7 = matrix_ids[get7(line,row,number_lines,number_rows)[0]][get7(line,row,number_lines,number_rows)[1]]
string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_7,direction)
# ---
direction = 8
neighbor_8 = matrix_ids[get8(line,row,number_lines,number_rows)[0]][get8(line,row,number_lines,number_rows)[1]]
string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_8,direction)
# ---


# neighbors neighbors set 2 => 5 elements neighbors
# up = e1
for e1 in range(1,matrix_ids.shape[1]-1):
	line = 0
	row = e1
	element_id = matrix_ids[line][row]
	direction = 1
	neighbor_1 = matrix_ids[get1(line,row,number_lines,number_rows)[0]][get1(line,row,number_lines,number_rows)[1]]
	string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_1,direction)
	# ---
	direction = 2
	neighbor_2 = matrix_ids[get2(line,row,number_lines,number_rows)[0]][get2(line,row,number_lines,number_rows)[1]]
	string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_2,direction)
	# ---
	direction = 3
	neighbor_3 = matrix_ids[get3(line,row,number_lines,number_rows)[0]][get3(line,row,number_lines,number_rows)[1]]
	string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_3,direction)
	# ---
	direction = 4
	neighbor_4 = matrix_ids[get4(line,row,number_lines,number_rows)[0]][get4(line,row,number_lines,number_rows)[1]]
	string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_4,direction)
	# ---
	direction = 5
	neighbor_5 = matrix_ids[get5(line,row,number_lines,number_rows)[0]][get5(line,row,number_lines,number_rows)[1]]
	string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_5,direction)
	# ---
	direction = 6
	neighbor_6 = matrix_ids[get6(line,row,number_lines,number_rows)[0]][get6(line,row,number_lines,number_rows)[1]]
	string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_6,direction)
	# ---
	direction = 7
	neighbor_7 = matrix_ids[get7(line,row,number_lines,number_rows)[0]][get7(line,row,number_lines,number_rows)[1]]
	string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_7,direction)
	# ---
	direction = 8
	neighbor_8 = matrix_ids[get8(line,row,number_lines,number_rows)[0]][get8(line,row,number_lines,number_rows)[1]]
	string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_8,direction)
	# ---

# right = e2
for e2 in range(1,matrix_ids.shape[0]-1):
	line = e2
	row = matrix_ids.shape[1]-1
	element_id = matrix_ids[line][row]
	direction = 1
	neighbor_1 = matrix_ids[get1(line,row,number_lines,number_rows)[0]][get1(line,row,number_lines,number_rows)[1]]
	string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_1,direction)
	# ---
	direction = 2
	neighbor_2 = matrix_ids[get2(line,row,number_lines,number_rows)[0]][get2(line,row,number_lines,number_rows)[1]]
	string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_2,direction)
	# ---
	direction = 3
	neighbor_3 = matrix_ids[get3(line,row,number_lines,number_rows)[0]][get3(line,row,number_lines,number_rows)[1]]
	string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_3,direction)
	# ---
	direction = 4
	neighbor_4 = matrix_ids[get4(line,row,number_lines,number_rows)[0]][get4(line,row,number_lines,number_rows)[1]]
	string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_4,direction)
	# ---
	direction = 5
	neighbor_5 = matrix_ids[get5(line,row,number_lines,number_rows)[0]][get5(line,row,number_lines,number_rows)[1]]
	string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_5,direction)
	# ---
	direction = 6
	neighbor_6 = matrix_ids[get6(line,row,number_lines,number_rows)[0]][get6(line,row,number_lines,number_rows)[1]]
	string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_6,direction)
	# ---
	direction = 7
	neighbor_7 = matrix_ids[get7(line,row,number_lines,number_rows)[0]][get7(line,row,number_lines,number_rows)[1]]
	string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_7,direction)
	# ---
	direction = 8
	neighbor_8 = matrix_ids[get8(line,row,number_lines,number_rows)[0]][get8(line,row,number_lines,number_rows)[1]]
	string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_8,direction)
	# ---

# down = e3
for e3 in range(1,matrix_ids.shape[1]-1):
	line = matrix_ids.shape[0]-1
	row = e3
	element_id = matrix_ids[line][row]
	direction = 1
	neighbor_1 = matrix_ids[get1(line,row,number_lines,number_rows)[0]][get1(line,row,number_lines,number_rows)[1]]
	string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_1,direction)
	# ---
	direction = 2
	neighbor_2 = matrix_ids[get2(line,row,number_lines,number_rows)[0]][get2(line,row,number_lines,number_rows)[1]]
	string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_2,direction)
	# ---
	direction = 3
	neighbor_3 = matrix_ids[get3(line,row,number_lines,number_rows)[0]][get3(line,row,number_lines,number_rows)[1]]
	string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_3,direction)
	# ---
	direction = 4
	neighbor_4 = matrix_ids[get4(line,row,number_lines,number_rows)[0]][get4(line,row,number_lines,number_rows)[1]]
	string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_4,direction)
	# ---
	direction = 5
	neighbor_5 = matrix_ids[get5(line,row,number_lines,number_rows)[0]][get5(line,row,number_lines,number_rows)[1]]
	string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_5,direction)
	# ---
	direction = 6
	neighbor_6 = matrix_ids[get6(line,row,number_lines,number_rows)[0]][get6(line,row,number_lines,number_rows)[1]]
	string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_6,direction)
	# ---
	direction = 7
	neighbor_7 = matrix_ids[get7(line,row,number_lines,number_rows)[0]][get7(line,row,number_lines,number_rows)[1]]
	string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_7,direction)
	# ---
	direction = 8
	neighbor_8 = matrix_ids[get8(line,row,number_lines,number_rows)[0]][get8(line,row,number_lines,number_rows)[1]]
	string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_8,direction)
	# ---

# left = e4
for e4 in range(1,matrix_ids.shape[0]-1):
	line = e4
	row = 0
	element_id = matrix_ids[line][row]
	direction = 1
	neighbor_1 = matrix_ids[get1(line,row,number_lines,number_rows)[0]][get1(line,row,number_lines,number_rows)[1]]
	string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_1,direction)
	# ---
	direction = 2
	neighbor_2 = matrix_ids[get2(line,row,number_lines,number_rows)[0]][get2(line,row,number_lines,number_rows)[1]]
	string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_2,direction)
	# ---
	direction = 3
	neighbor_3 = matrix_ids[get3(line,row,number_lines,number_rows)[0]][get3(line,row,number_lines,number_rows)[1]]
	string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_3,direction)
	# ---
	direction = 4
	neighbor_4 = matrix_ids[get4(line,row,number_lines,number_rows)[0]][get4(line,row,number_lines,number_rows)[1]]
	string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_4,direction)
	# ---
	direction = 5
	neighbor_5 = matrix_ids[get5(line,row,number_lines,number_rows)[0]][get5(line,row,number_lines,number_rows)[1]]
	string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_5,direction)
	# ---
	direction = 6
	neighbor_6 = matrix_ids[get6(line,row,number_lines,number_rows)[0]][get6(line,row,number_lines,number_rows)[1]]
	string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_6,direction)
	# ---
	direction = 7
	neighbor_7 = matrix_ids[get7(line,row,number_lines,number_rows)[0]][get7(line,row,number_lines,number_rows)[1]]
	string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_7,direction)
	# ---
	direction = 8
	neighbor_8 = matrix_ids[get8(line,row,number_lines,number_rows)[0]][get8(line,row,number_lines,number_rows)[1]]
	string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_8,direction)
	# ---

# neighbors neighbors set 3 => 8 elements neighbors
for line in range(1,matrix_ids.shape[0] - 1):
	for row in range(1,matrix_ids.shape[1] - 1):
		element_id = matrix_ids[line][row]
		direction = 1
		neighbor_1 = matrix_ids[get1(line,row,number_lines,number_rows)[0]][get1(line,row,number_lines,number_rows)[1]]
		string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_1,direction)
		# ---
		direction = 2
		neighbor_2 = matrix_ids[get2(line,row,number_lines,number_rows)[0]][get2(line,row,number_lines,number_rows)[1]]
		string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_2,direction)
		# ---
		direction = 3
		neighbor_3 = matrix_ids[get3(line,row,number_lines,number_rows)[0]][get3(line,row,number_lines,number_rows)[1]]
		string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_3,direction)
		# ---
		direction = 4
		neighbor_4 = matrix_ids[get4(line,row,number_lines,number_rows)[0]][get4(line,row,number_lines,number_rows)[1]]
		string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_4,direction)
		# ---
		direction = 5
		neighbor_5 = matrix_ids[get5(line,row,number_lines,number_rows)[0]][get5(line,row,number_lines,number_rows)[1]]
		string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_5,direction)
		# ---
		direction = 6
		neighbor_6 = matrix_ids[get6(line,row,number_lines,number_rows)[0]][get6(line,row,number_lines,number_rows)[1]]
		string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_6,direction)
		# ---
		direction = 7
		neighbor_7 = matrix_ids[get7(line,row,number_lines,number_rows)[0]][get7(line,row,number_lines,number_rows)[1]]
		string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_7,direction)
		# ---
		direction = 8
		neighbor_8 = matrix_ids[get8(line,row,number_lines,number_rows)[0]][get8(line,row,number_lines,number_rows)[1]]
		string_neighbor += "Edge(%dL,%dL,'%d')," % (element_id,neighbor_8,direction)
		# ---

string_neighbor = string_neighbor[0:-1]
string_neighbor += "))"
print string_neighbor