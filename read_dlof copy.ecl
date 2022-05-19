import $;
IMPORT Std.System.Thorlib;
anomaly:= $.File_dlof.File;

child_rec:=RECORD
    $.File_dlof.Layout;
    INTEGER1 Node_num;
END;

EXPORT child_rec proces(anomaly L):=TRANSFORM
    SELF:=L;
    SELF.Node_num:=Thorlib.node();
    
END;
INTEGER a:=0;
ds:=distribute(anomaly, field1);
with_node_num:=project(anomaly, proces(LEFT));

funct(integer1 x):= FUNCTION
ds3:=with_node_num(Node_num=x);
a=a+count(ds3);
return a;
END; 


s:=set(with_node_num,Node_num);
setDS := dataset(s,{INTEGER1 Node_Num});
sort_ds:=sort(setDS,Node_num);
Dedup_ds:=dedup(sort_ds,Node_num);
dummy_rec:= RECORD
    INTEGER1 dummy;
    Dedup_ds.Node_num;
    
END;
dummy_rec produce(Dedup_ds L,INTEGER C):=TRANSFORM
    SELF.dummy:=funct(c);
    SELF := L;
END;


recs:=project(Dedup_ds, produce(Left, counter));
real_set:=set(Dedup_ds,Node_num);
//output(ds3);

ds2:=with_Node_num(Node_num=2);
output(ds2);