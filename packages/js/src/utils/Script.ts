import * as fs from "fs";
import path from "path";

export class Script {
    private deps: Script[] = [];

    private script = '';

    loadDep(depScript: Script) {
        this.deps.push(depScript);
    }

    loadScript(scriptPath: string) {
        const dir = /*process.env.NODE_ENV === 'test' ? 'test-scripts' : */'scripts';

        const script = fs.readFileSync(path.resolve(__dirname, '../../', dir, scriptPath));

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
