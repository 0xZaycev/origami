import {Script} from "../utils/Script";

import {headerScript} from "./header.script";

export const response_ackScript = new Script();

response_ackScript.loadScript('response_ack.lua');

response_ackScript.loadDep(headerScript);
