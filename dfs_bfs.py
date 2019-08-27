adj = {1:[2,3],2:[4],3:[4],4:[5],5:[]}

def dfs(item, visited):
    visited.append(item)
    print(item)
    for i in adj[item]:
        if(i not in visited):
            dfs(i,visited)
        


def bfs(queue, visited):
    if(not queue):
        return
    
    item = queue.pop(0)
    
    if(item not in visited):
        visited.append(item)
        print(item)
    
    for i in adj[item]:
       if(i not in visited): 
          queue.append(i)
    
    bfs(queue,visited)

visited_dfs = []
print("Depth Breadth Traversal")
dfs(1,visited_dfs)

queue = []
queue.append(1)
visited = []
print("Breadth First Traversal")
bfs(queue,visited)
