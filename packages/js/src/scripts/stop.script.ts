import {Script} from "../utils/Script";

import {headerScript} from "./header.script";

export const stopScript = new Script();

stopScript.loadScript('stop.lua');

stopScript.loadDep(headerScript);
