import {Script} from "../utils/Script";

import {log_functionScript} from "./log_function.script";
import {tick_functionScript} from "./tick_function.script";

export const headerScript = new Script();

headerScript.loadScript('header.lua');

headerScript.loadDep(log_functionScript);

headerScript.loadDep(tick_functionScript);
