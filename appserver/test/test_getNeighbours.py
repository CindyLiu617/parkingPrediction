from knearest import _yield_neighbours

neighbours = _yield_neighbours([-1, 0, 0, 0, 0], 2)
for neighbour in neighbours:
    print neighbour
