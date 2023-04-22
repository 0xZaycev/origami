import {Script} from "../utils/Script";

import {tick_functionScript} from "./tick_function.script";

export const headerScript = new Script();

headerScript.loadScript('../../scripts/header.lua');

headerScript.loadDep(tick_functionScript);