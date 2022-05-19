IMPORT Python3 AS Python;

// Embed method to calculate one factorial
INTEGER doFactorial(INTEGER input) := EMBED(Python)
    import math
    return math.factorial(input)
ENDEMBED;

// Use above function to generate a single factorial.  Note that this
// can only run on one node, because it receives and returns a basic (local)
// data type.
fac17 := doFactorial(17);
OUTPUT(fac17, NAMED('factorial17'));

// Now we'll call it from within a TRANSFORM of a distributed dataset.
// This runs in parallel on all nodes, because each record is transformed
// with a different input value, and results in dataset records distributed
// across nodes.

MyRec := RECORD
    UNSIGNED num;
    UNSIGNED numFactorial := 0;
END;

// Geneate a distributed dataset of MyRec, with just the num field populated with numbers 1-20:
MyDS0 := DATASET(20, TRANSFORM(MyRec, SELF.num := COUNTER), DISTRIBUTED);
// Let's distribute it by num.  This will cause record 1 to be on node 1, record 2
// to be on node2, etc.  
MyDS := DISTRIBUTE(MyDS0, num);
// We'll output the original dataset for reference.
OUTPUT(MyDS, NAMED('InputDataset'));

// Now we do a PROJECT with a TRANSFORM that uses our python factorial function.
// The records on each node will be processed in parallel, resulting in the results
// being on each node.
MyDSComplete := PROJECT(MyDS, TRANSFORM(MyRec, 
                             SELF.numFactorial := doFactorial(LEFT.num),
                             SELF := LEFT));

// Output the results.  Note that, while the dataset has been completed,
// the results are not sorted (if run on a multi node thor).
OUTPUT(MyDSComplete, NAMED('Dataset_Complete'));

// You may want to sort the final results after doing distributed operations.
Complete_Sorted := SORT(MyDSComplete, num);
OUTPUT(Complete_Sorted, NAMED('Complete_Sorted'));