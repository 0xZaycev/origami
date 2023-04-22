import {Script} from "../utils/Script";

import {headerScript} from "./header.script";

export const requestScript = new Script();

requestScript.loadScript('../../scripts/request.lua');

requestScript.loadDep(headerScript);