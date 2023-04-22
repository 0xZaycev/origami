import {Script} from "../utils/Script";

import {headerScript} from "./header.script";

export const initScript = new Script();

initScript.loadScript('../../scripts/init.lua');

initScript.loadDep(headerScript);