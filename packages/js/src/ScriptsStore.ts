export class ScriptsStore {
    private sources = new Map<EScriptName, string>();
    private hashes = new Map<EScriptName, string>();

    registerSource(script: EScriptName, source: string) {
        this.sources.set(script, source);
    }

    registerHash(script: EScriptName, hash: string) {
        this.hashes.set(script, hash);
    }

    getHash(script: EScriptName) {
        return this.hashes.get(script);
    }
}

export enum EScriptName {
    INIT,
    PING,
    REQUEST,
    REQUEST_ACK,
    RESPONSE,
    RESPONSE_ACK,
    STOP,
}
