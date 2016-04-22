package com.cfets.door.rpc;

public class RpcServer {

	public static void main(String[] args) throws Exception {
		HelloService service = new HelloServiceImpl();  
		//Start the RPC service, listening on port 1234
        RpcFramework.startService(service, 1234);
	}

}
