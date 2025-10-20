from controller import Supervisor

supervisor = Supervisor()
timestep = int(supervisor.getBasicTimeStep())

robot_node = supervisor.getFromDef('MY_ROBOT')
while 1==1:
    print('...............')
if robot_node is None:
    print("Không tìm thấy robot với DEF 'MY_ROBOT'")
    exit(1)

root = supervisor.getRoot()
children_field = root.getField('children')

line_def = """
Shape {
  DEF PATH_LINE
  appearance Appearance {
    material Material { diffuseColor 1 0 0 }
  }
  geometry IndexedLineSet {
    coord Coordinate {
      point [ 0 0 0 ]
    }
    coordIndex [ 0 -1 ]
  }
}
"""
children_field.importMFNodeFromString(-1, line_def)
line_node = supervisor.getFromDef('PATH_LINE')
geometry = line_node.getField('geometry')
coordinate = geometry.getSFNode().getField('coord')
points_field = coordinate.getMFField('point')
index_field = geometry.getSFNode().getField('coordIndex')

path_points = []

while supervisor.step(timestep) != -1:
    position = robot_node.getPosition()
    path_points.append(position)

    points_field.clear()
    for p in path_points:
        points_field.appendMFVector3f(p)

    index_field.clear()
    for i in range(len(path_points)):
        index_field.appendMFInt32(i)
    index_field.appendMFInt32(-1)
