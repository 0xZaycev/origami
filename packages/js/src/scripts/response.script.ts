import {Script} from "../utils/Script";

import {headerScript} from "./header.script";

export const responseScript = new Script();

responseScript.loadScript('response.lua');

responseScript.loadDep(headerScript);
