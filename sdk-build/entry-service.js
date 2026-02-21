// Entry point for esbuild — exports Connect SDK classes to a global
import { AmazonConnectService } from "@amazon-connect/app";
import { AgentClient } from "@amazon-connect/contact";

window.ConnectSDK = { AmazonConnectService, AgentClient };
