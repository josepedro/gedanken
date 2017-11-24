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

# neighbors
eq_1 = X*Y - (X - 1)
eq_2 = eq_1 + 1
eq_3 = 2
eq_4 = X+2
eq_5 = X+1
eq_6 = 2*X
eq_7 = X
eq_8 = X*Y
element_count = 0
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
