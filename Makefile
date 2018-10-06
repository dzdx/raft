protobuf:
	protoc --go_out=plugins=grpc:raftpb/ *.proto

fast_protobuf:
	protoc --gofast_out=plugins=grpc:raftpb/ *.proto
	
