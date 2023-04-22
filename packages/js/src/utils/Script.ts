import * as fs from "fs";
import path from "path";

export class Script {
    private deps: Script[] = [];

    private script = '';

    loadDep(depScript: Script) {
        this.deps.push(depScript);
    }

    loadScript(scriptPath: string) {
        const script = fs.readFileSync(path.resolve(__dirname, scriptPath));

        this.script = script.toString();
    }

    getScript(): string {
        const fullScript = this.deps
            .map(d => d.getScript())

        fullScript.push(this.script);

        return fullScript
            .join('\n\n\n');
    }
}