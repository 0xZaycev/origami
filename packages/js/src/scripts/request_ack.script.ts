import {Script} from "../utils/Script";

import {headerScript} from "./header.script";

export const request_ackScript = new Script();

request_ackScript.loadScript('../../scripts/request_ack.lua');

request_ackScript.loadDep(headerScript);