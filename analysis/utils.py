
def execute_file(cursor, filename):
    contents = " ".join([line for line in open(filename, 'r') if line[0:2] != '--'])
    statements = contents.split(';')[:-1] # split on semi-colon. Last element will be trailing whitespace

    for statement in statements:
        cursor.execute(statement)



def binary_search(a, x):
    """Given a sorted (decreasing) list, return the first element that is less than the target value."""
    
    left = 0
    right = len(a)
    
    while (left < right):
        mid = (right + left) / 2
        
        if a[mid] >= x:
            left = mid + 1
        else:
            right = mid
    
    return right
            