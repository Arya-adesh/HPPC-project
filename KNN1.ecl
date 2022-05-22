IMPORT Python3 AS Python;
anomaly:= $.File_dlof.File;
dummy_rec:=RECORD
   
    $.File_dlof.Layout.field1;
    $.File_dlof.Layout.field2;
    $.File_dlof.Layout.field3;
    $.File_dlof.Layout.field4;
    $.File_dlof.Layout.field5;
    $.File_dlof.Layout.field6;
    $.File_dlof.Layout.field7;
    $.File_dlof.Layout.field8;
    $.File_dlof.Layout.field9;
    $.File_dlof.Layout.field10;
    $.File_dlof.Layout.field11;
    INTEGER numfactorial;
END;
dummy_rec1:=RECORD
   
  INTEGER numfactorial
   
    
    
   
END;
dummy_rec change(anomaly L, INTEGER C):=TRANSFORM
    

    SELF.numfactorial:=0;
    SELF:=L;
END;




// Python Subsystem.  We store the "factorialMgr" object in shared memory, and then access it
// with a separate method. This would only be useful if the factorialMgr had significant data
// or initialization cost which we wouldn't want to bear on each invocation.
// Note that there's no input data, but we need some kind of STREAMED DATASET for input or
// it wont run on all nodes, so we just pass a bogus handleRec dataset in.  Usually you would
// have real input data here.
handleRec := RECORD
  UNSIGNED handle;
END;
STREAMED DATASET(handleRec) fmInit(STREAMED DATASET(dummy_rec) recs) :=
           EMBED(Python: globalscope('facScope'), persist('query'), activity)
  
    global OBJECT, OBJ_NUM
    import math

    if 'OBJECT' not in globals():
        # This is your one-time initializer code.  It will only be executed once on each node.
        # All global initialization goes here.
        OBJECT = {}  # Dictionary of objects.  This allows multiple active objects.
        OBJ_NUM = 1 # Object counter
        class kdtree_create():
            
            def __init__(self,points):
                self.all_points=list(points)
                self.k=len(points[0])
                self.root_node={}

            
            def distance_squared(point1, point2):
                return sum((x-y)**2 for x, y in zip(point1, point2))
            
           


            def build_kdtree(self,points, depth=0):
                n = len(points)

                if n <= 0:
                    return None

                axis = depth % self.k

                sorted_points = sorted(points, key=lambda point: point[axis])

                return {
                    'point': sorted_points[n // 2],
                    'left': self.build_kdtree(sorted_points[:n // 2], depth + 1),
                    'right': self.build_kdtree(sorted_points[n // 2 + 1:], depth + 1)}


            
            
            def fit(self):
                tree=self.build_kdtree(self.all_points)
                self.root_node=tree
                

                



            def closer_distance(pivot, p1, p2):
                if p1 is None:
                    return p2

                if p2 is None:
                    return p1

                d1 = distance_squared(pivot, p1)
                d2 = distance_squared(pivot, p2)

                if d1 < d2:
                    return p1
                else:
                    return p2


            def kdtree_closest_point(root, point, k,depth=0):
                if root is None:
                    return None

                axis = depth % k

                next_branch = None
                opposite_branch = None

                if point[axis] < root['point'][axis]:
                    next_branch = root['left']
                    opposite_branch = root['right']
                else:
                    next_branch = root['right']
                    opposite_branch = root['left']

                best = closer_distance(point,
                                        kdtree_closest_point(next_branch,
                                                            point,k,
                                                            depth + 1),
                                        root['point'])

                if distance_squared(point, best) > (point[axis] - root['point'][axis]) ** 2:
                    best = closer_distance(point,
                                            kdtree_closest_point(opposite_branch,
                                                                point,k,
                                                                depth + 1),
                                            best)

                return best  
        l=[]
        for recTuple in recs:
            inter_list=[]
            for x in recTuple:
                inter_list.append(recTuple)
            l.append(inter_list)     
    # Now instantiate the object that we want to use repeatedly
        myObject =kdtree_create(l)
        myObject.fit()
    # Add it to our OBJECTS cache
        handle = OBJ_NUM # The handle is the index into the OBJECTS dict.
        OBJECT[handle] = myObject

    # Increment the OBJ_NUM for next time we're called.
        OBJ_NUM += 1

    # We return a single dummy record with the object handle inside.
    return[(handle,)]
ENDEMBED;

// Here's a routine that uses the shared object from Init.
// Notice that it must receive handle even though it's not used.
// Otherwise, we can't guarantee that fmInit will be called first.
rec2:=RECORD
    REAL4 field1;
    REAL4 field2;
     REAL4 field3;
      REAL4 field4;
       REAL4 field5;
END;
STREAMED DATASET(rec2) nearesn(STREAMED DATASET(dummy_rec) recs, UNSIGNED handle) :=
           EMBED(Python: globalscope('facScope'), persist('query'), activity)
    myLR = OBJECT[handle] 
   
    
    for recTuple in recs:
            
        # Extract the fields from the record. In this case
        # we only care about the first field 'num'.
        index=[]
        for x in range(0,sizeof(recTuple)):
            index.append(recTuple[x])
        y=myLR.nearest_neighbor(list(index))
        yield (tuple(y))
        
    
ENDEMBED;
dummy_ds:=project(anomaly, change(LEFT,COUNTER));
// Geneate a distributed dataset of MyRec, with just the num field populated with numbers 1-20:
//MyDS0 := DATASET(20, TRANSFORM(MyRec, SELF.num := COUNTER), DISTRIBUTED);
// Let's distribute it by num.  This will cause record 1 to be on node 1, record 2
// to be on node2, etc.
MyDS := DISTRIBUTE(dummy_ds, ALL);
// We'll output the original dataset for reference.
OUTPUT(MyDS, NAMED('InputDataset'));


// Create a dummy dataset of handles, at least one record on each node.
//dummy0 := DATASET([{0}], handleRec);
//dummy := DISTRIBUTE(dummy0, ALL);
handles:=fmInit(MyDS);
// Now we can call the fmInit, and get back a handle on from each node.
//handles := fmInit(dummy);
// We output the handles just to show how they appear.
OUTPUT(handles, NAMED('handles'));
// Now we reduce to a single handle using MAX
handle := MAX(handles, handle);
// Output the single handle.
OUTPUT(handle, NAMED('handle'));

MyDS2:=DISTRIBUTE(MyDS);
OUTPUT(MyDS2, NAMED('MyDS2'));
// And now we call the doFactorials method, using the handle.
MyDSComplete := nearesn(MyDS2, handle);

// Output the results.  Note that, while the dataset has been completed,
// the results are not sorted (if run on a multi node thor).
//output(MyDS);
OUTPUT(MyDSComplete, NAMED('Dataset_Complete'));

// You may want to sort the final results after doing distributed operations.
//Complete_Sorted := SORT(MyDSComplete, num);
//OUTPUT(Complete_Sorted, NAMED('Complete_Sorted'));