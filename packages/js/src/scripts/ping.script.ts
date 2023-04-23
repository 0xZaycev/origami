import {Script} from "../utils/Script";

import {headerScript} from "./header.script";

export const pingScript = new Script();

pingScript.loadScript('ping.lua');

pingScript.loadDep(headerScript);
