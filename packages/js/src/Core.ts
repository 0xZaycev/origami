import {Redis} from "./Redis";
import {Consumer} from "./Consumer";
import {Producer} from "./Producer";
import {EScriptName, ScriptsStore} from "./ScriptsStore";

import {pingScript, stopScript} from "./scripts";

import {Resp} from "./utils/Resp";

export class Core {
    private readonly nodeId: string;

    private readonly pingConn: Redis;
    private readonly listenerConn: Redis;
    private readonly publisherConn: Redis;

    private readonly scriptsStore: ScriptsStore;

    private readonly restartHandler: () => void;

    public readonly listeners: Record<string, IMessageHandler> = {};

    public readonly consumer: Consumer;
    public readonly producer: Producer;

    private started = false;

    private pingState: (0 | number) = 0;
    private pingTimer = 0;

    private stopState: (0 | number) = 0;
    private stopPromise = Promise.resolve();
    private stopResolver: () => void = () => undefined;

    private cleanerTimer = 0;



    constructor(options: ICoreOptions) {
        this.nodeId = options.nodeId;

        this.pingConn = options.pingConn;
        this.listenerConn = options.listenerConn;
        this.publisherConn = options.publisherConn;

        this.scriptsStore = options.scriptsStore;

        this.restartHandler = options.restart;

        this.consumer = new Consumer({
            nodeId: this.nodeId,
            publisherConn: this.publisherConn,
            scriptsStore: this.scriptsStore,
        });

        this.producer = new Producer({
            nodeId: this.nodeId,
            publisherConn: this.publisherConn,
            scriptsStore: this.scriptsStore,
        });

        this.registerScripts();
    }


    start() {
        this.started = true;

        const now = Date.now();

        this.consumer.restart(now);
        this.producer.restart(now);

        this.ping();
        this.cleaner();
    }
    stop() {
        this.started = false;

        clearTimeout(this.pingTimer);
        clearTimeout(this.cleanerTimer);
    }


    fillListeners() {
        this.listeners['origami.a' + this.nodeId] = this.onPing.bind(this);
        this.listeners['origami.h' + this.nodeId] = this.onStop.bind(this);

        for(const listener of this.consumer.listeners()) {
            this.listeners[listener[0]] = listener[1];
        }

        for(const listener of this.producer.listeners()) {
            this.listeners[listener[0]] = listener[1];
        }
    }


    stopRequestsAccepting() {
        if(this.stopState) {
            return this.stopPromise;
        }

        this.stopPromise = new Promise<void>((resolve) => {
            this.stopResolver = resolve;

            const scriptName = this.scriptsStore.getHash(EScriptName.STOP);

            if(!scriptName) {
                // wtf?

                resolve();

                return;
            }

            this.stopState = Date.now();

            this.publisherConn.send(Resp.encode(['evalsha', scriptName, '0', this.nodeId]));
        });

        return this.stopPromise;
    }


    private ping() {
        if(!this.started) {
            return;
        }

        clearTimeout(this.pingTimer);

        this.pingTimer = setTimeout(() => {
            const scriptHash = this.scriptsStore.getHash(EScriptName.PING);

            if(!scriptHash) {
                this.ping();

                return;
            }

            this.pingState = Date.now();

            this.pingConn.send(Resp.encode(['evalsha', scriptHash, '0', this.nodeId]));
        }, 1000) as any as number;
    }
    private cleaner() {
        if(!this.started) {
            return;
        }

        clearTimeout(this.cleanerTimer);

        this.cleanerTimer = setTimeout(() => {
            const now = Date.now();

            let needRestart;

            needRestart = this.pingCleaner(now, 2000, 7000);

            if(needRestart) {
                this.pingState = 0;

                this.restartHandler();

                return;
            }

            needRestart = this.stopCleaner(now, 2000, 7000);

            if(needRestart) {
                this.stopState = 0;

                this.restartHandler();

                return;
            }

            needRestart = this.consumer.cleaner(now, 2000, 10000);

            if(needRestart) {
                this.restartHandler();

                return;
            }

            needRestart = this.producer.cleaner(now, 2000, 10000);

            if(needRestart) {
                this.restartHandler();

                return;
            }

            this.cleaner();
        }, 2000) as any as number;
    }


    private pingCleaner(now: number, ack: number, restart: number) {
        if(!this.pingState) {
            return false
        }

        const t = now - this.pingState;

        if(t > restart) {
            return true;
        } else if(t > ack) {
            const scriptHash = this.scriptsStore.getHash(EScriptName.PING);

            if(!scriptHash) {
                return false;
            }

            this.pingConn.send(Resp.encode(['evalsha', scriptHash, '0', this.nodeId]));
        }

        return false;
    }
    private stopCleaner(now: number, ack: number, restart: number) {
        if(!this.stopState) {
            return false
        }

        const t = now - this.stopState;

        if(t > restart) {
            return true;
        } else if(t > ack) {
            const scriptHash = this.scriptsStore.getHash(EScriptName.STOP);

            if(!scriptHash) {
                return false;
            }

            this.pingConn.send(Resp.encode(['evalsha', scriptHash, '0', this.nodeId]));
        }

        return false;
    }


    private async onPing() {
        this.pingState = 0;

        this.ping();
    }
    private async onStop() {
        this.stopState = 0;

        this.stopResolver();
    }


    public messagesHandler(message: [string, string, string]) {
        if(message[0] !== 'message') {
            return;
        }

        if(!this.listeners[message[1]]) {
            return;
        }

        this.listeners[message[1]](message[2]);
    }


    private registerScripts() {
        this.scriptsStore.registerSource(EScriptName.PING, pingScript.getScript());
        this.scriptsStore.registerSource(EScriptName.STOP, stopScript.getScript());
    }
}

export interface ICoreOptions {
    nodeId: string;

    pingConn: Redis;
    listenerConn: Redis;
    publisherConn: Redis;

    scriptsStore: ScriptsStore;

    restart: () => void;
}

interface IMessageHandler {
    (message: string): Promise<void>;
}
