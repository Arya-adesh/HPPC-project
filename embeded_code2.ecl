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
   
END;
dummy_rec change(anomaly L, INTEGER C):=TRANSFORM
    

    SELF.numfactorial:=0;
    SELF:=L;
END;
dummy_ds:=project(anomaly, change(LEFT,COUNTER));
// Geneate a distributed dataset of MyRec, with just the num field populated with numbers 1-20:
//MyDS0 := DATASET(20, TRANSFORM(MyRec, SELF.num := COUNTER), DISTRIBUTED);
// Let's distribute it by num.  This will cause record 1 to be on node 1, record 2
// to be on node2, etc.
MyDS := DISTRIBUTE(dummy_ds);
// We'll output the original dataset for reference.
OUTPUT(MyDS, NAMED('InputDataset'));



// Python Subsystem.  We store the "factorialMgr" object in shared memory, and then access it
// with a separate method. This would only be useful if the factorialMgr had significant data
// or initialization cost which we wouldn't want to bear on each invocation.
// Note that there's no input data, but we need some kind of STREAMED DATASET for input or
// it wont run on all nodes, so we just pass a bogus handleRec dataset in.  Usually you would
// have real input data here.
handleRec := RECORD
  UNSIGNED handle;
END;
STREAMED DATASET(handleRec) fmInit(STREAMED DATASET(handleRec) recs) :=
           EMBED(Python: globalscope('facScope'), persist('query'), activity)
    import math
    global OBJECT
    # Let's create and store
    # an exception formatting function so we can use it
    # anywhere.
    global FORMAT_EXC
    def _format_exc(func=''):
       import traceback as tb
       exc = tb.format_exc(limit=2)
       if len(exc) < 100000:
           return func + ': ' + exc
       else:
           return func + ': ' + exc[:200] + ' ... ' + exc[-200:]
    FORMAT_EXC = _format_exc
    # We wrap everything we can in try ... except to make debugging easier.
    try:
        # We're only supporting one factorialMgr.  If called again will use the original.
        if 'OBJECT' not in globals():
            # Define the class. Typlically we would import another module and use a
            # class from that module.
            class factorialMgr:
                def calcFactorial(self, num):
                   return math.factorial(num)
            # Instantiate factorialMgr and store in global memory
            OBJECT = factorialMgr()
        # Now we just return an arbitrary handle record, since we're only handling one
        # instance.
        return [(1,)]
    except:
        # Use our stored exception formatting function
        exc = FORMAT_EXC('facModule.doFactorials')
        assert False, exc

ENDEMBED;

// Here's a routine that uses the shared object from Init.
// Notice that it must receive handle even though it's not used.
// Otherwise, we can't guarantee that fmInit will be called first.
STREAMED DATASET(dummy_rec1) doFactorials(STREAMED DATASET(dummy_rec) recs, UNSIGNED handle) :=
           EMBED(Python: globalscope('facScope'), persist('query'), activity)
    # Check your input data and stored state and use assert to indicate errors.
    assert 'OBJECT' in globals(), 'facModule.doFactorial -- ERROR Expected OBJECT not defined.'
    try:
        i=1;
        inputs=[]
        for recTuple in recs:
            inputs.append(recTuple)
            # Extract the fields from the record. In this case
            # we only care about the first field 'num'.
        for i in range(0,len(inputs)):
            rec=inputs[i]     
            #t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11=rec
            
            # Yield a new record with the factorial included.
            # We use the stored factorialMgr to do the work.
            # yield (t1,t2,t3,t4,t5,t6,t7,t8,t9,t10)
    except:
        exc = FORMAT_EXC('facModule.doFactorials')
        assert False, exc
ENDEMBED;

// Create a dummy dataset of handles, at least one record on each node.
dummy0 := DATASET([{0}], handleRec);
dummy := DISTRIBUTE(dummy0, ALL);
// Now we can call the fmInit, and get back a handle on from each node.
handles := fmInit(dummy);
// We output the handles just to show how they appear.
OUTPUT(handles, NAMED('handles'));
// Now we reduce to a single handle using MAX
handle := MAX(handles, handle);
// Output the single handle.
OUTPUT(handle, NAMED('handle'));
// And now we call the doFactorials method, using the handle.
MyDSComplete := doFactorials(MyDS, handle);

// Output the results.  Note that, while the dataset has been completed,
// the results are not sorted (if run on a multi node thor).
//output(MyDS);
OUTPUT(MyDSComplete, NAMED('Dataset_Complete'));

// You may want to sort the final results after doing distributed operations.
//Complete_Sorted := SORT(MyDSComplete, num);
//OUTPUT(Complete_Sorted, NAMED('Complete_Sorted'));