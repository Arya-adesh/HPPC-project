import $;
IMPORT Std.System.Thorlib;
anomaly:= $.File_dlof.File;

child_rec:=RECORD
    $.File_dlof.Layout;
    INTEGER1 Node_num;
END;

child_rec change(anomaly L):=TRANSFORM
    SELF:=L;
    SELF.Node_num:=Thorlib.node();
    
END;


ds:=distribute(anomaly, field1);
OUTPUT(ds);
with_node_num:=project(anomaly, change(LEFT));
ds2:=with_node_num(Node_num=2);
OUTPUT(ds2);
OUTPUT(ds2,,OVERWRITE,'~thor::outdata.csv');
ds3:=with_node_num(Node_num=3);
OUTPUT(ds3);
OUTPUT(ds3,,OVERWRITE,'~thor::outdata.csv');