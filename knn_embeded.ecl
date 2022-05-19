EXPORT STREAMED DATASET(MyRec) pyEng2Span(STREAMED DATASET(MyRec) eng_sentences) :=       
                                                            EMBED(Python: activity)
    import eng2span # Fictional python module
    # Iterate over the input records
    for item in eng_sentences:
        id, sentence = item # Remember, each input item is a record tuple with 2 fields
        # Do the translation.
        spanish = ent2Span.Translate(sentence)
        # Since we are processing one record at a time, we use the Python generator semantic "yield"
        # (see Python documentation).  This allows us to emit records from deep inside a function.
        yield (id, spanish)  # We emit a single record as a tuple.
ENDEMBED;


MyRec := RECORD
    UNSIGNED id;
    STRING Sentence;
END;

// Make a dataset of 500 English Sentences (or input from a file).
Sentences := DATASET([{1, 'Hello, my name is Roger'},
                      {2, 'I live in Colorado'}, 
                      {500, 'This is my last sentence'}],MyRec);

// Distribute the sentences round-robin by id.
Sentences_Dist := DISTRIBUTE(Sentences, id);

// Now call the Python embed function.  Because our pyEng2Span embedded function
// is an 'activity', this one line will cause the subset of records on each node to be passed to
// the python code, and all of the results to form a single distributed dataset.
Spanish := pyEng2Span(Sentences_Dist);

// At this point, the order of the sentences is scrambled.  We'll probably want to sort it.
Spanish_Sorted := SORT(Spanish, Id);

OUTPUT(Spanish_Sorted);

// Here's what the embedded function would look like.
// Note the 'activity' keyword in the EMBED statement, and STREAMED DATASET output and first input.
