# Customizing the Overlay Network 
Skyplane can be modified to support customizeable overlay networks. 

## Gateway Program 
The gateway program defines how data is processed by a single gateway (cloud VM). The gateway program defines what object stores to read and write from and what other gateways to send and recieve data between.  

Currently, gateway programs can compose the following operators: 

For example, we can write a gateway program for a single VM that reads from bucket `src_bucket` and writes data to both `dst_bucket_1` and `dst_bucket_2` with the following code: 

```
program = GatewayProgram()

# read from object store
read_op = program.add_operator(ObjectStoreRead(bucket="src_bucket"))

# send read data to both child operators (write1_op, write2_op)
and_op = program.add_operator(MuxAnd(parent=read_op))

# write to object stores
write1_op = program.add_operator(ObjectStoreWrite(bucket="dst_bucket_1"), parent=and_op) 
write2_op = program.add_operator(ObjectStoreRead(bucket="dst_bucket_2"), parent=and_op)

print(program.to_json()) # JSON representation of program
```

## Planners 
Skyplane's planner determines the structure of the overlay network and how data should be transferred through the overlay network. 





