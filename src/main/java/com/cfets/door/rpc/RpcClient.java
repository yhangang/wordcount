package com.cfets.door.rpc;

public class RpcClient {

	public static void main(String[] args) throws Exception {
		HelloService service = RpcFramework.getService(HelloService.class, "127.0.0.1", 1234); 
            System.out.println(service.hello("World"));  
            System.out.println(service.hi("World"));
            
	}

}
