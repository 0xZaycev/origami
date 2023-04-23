import * as os from "os";
import crypto from "crypto";

import {Core} from "./Core";
import {EScriptName, ScriptsStore} from "./ScriptsStore";
import {IChannelHandler, IChannelOptions} from "./Consumer";
import {IRequestOptions, TRequestResponse} from "./Producer";
import {ESendCode, IConnectionOptions, Redis, TRequest} from "./Redis";

import {initScript} from "./scripts";

import {Resp} from "./utils/Resp";
import {BaseResult} from "./utils/BaseResult";


let client = 'NodeJS';
let clientVersion = '0';

try {
    const packageJson = require('../package.json');

    clientVersion = packageJson.version;
} catch (e) {
    //
}


export class Origami {
    private nodeId = crypto.randomUUID();

    private options: IOrigamiOptions;

    private state = EOrigamiState.CLOSED;
    private initialized = false;
    private stopped = false;

    private readonly core: Core;

    private readonly pingConn: Redis;
    private readonly listenerConn: Redis;
    private readonly publisherConn: Redis;

    private readonly scriptsStore: ScriptsStore;

    private startingLoop = false;
    private startingPromise: Promise<void> = Promise.resolve();

    private stoppingPromise: Promise<void> = Promise.resolve();

    private lastErrMsg = '';

    constructor(options: IOrigamiOptions) {
        this.options = options;

        this.pingConn = new Redis('pingConn', options.connection);
        this.listenerConn = new Redis('listenerConn', options.connection);
        this.publisherConn = new Redis('publisherConn', options.connection);

        this.scriptsStore = new ScriptsStore();

        this.core = new Core({
            nodeId: this.nodeId,

            pingConn: this.pingConn,
            listenerConn: this.listenerConn,
            publisherConn: this.publisherConn,

            scriptsStore: this.scriptsStore,

            restart: () => {
                this.closeConns().then();
            },
        });

        this.setListenersOnConn(this.pingConn);
        this.setListenersOnConn(this.listenerConn);
        this.setListenersOnConn(this.publisherConn);

        this.scriptsStore.registerSource(EScriptName.INIT, initScript.getScript());
    }


    getState() {
        return this.state;
    }

    isStopping() {
        return this.stopped;
    }


    channel<PARAMS = any, RESPONSE = any>(channelName: string, options: Partial<IChannelOptions>, handler: IChannelHandler<PARAMS, RESPONSE>) {
        return this.core.consumer.channel<PARAMS, RESPONSE>(channelName, options, handler);
    }


    request<RESPONSE = any, PARAMS = any>(channel: string, params: PARAMS, options?: Partial<IRequestOptions>): TRequestResponse<RESPONSE> {
        return this.core.producer.request<RESPONSE, PARAMS>(channel, params, options);
    }


    async start() {
        if(this.state !== EOrigamiState.CLOSED) {
            return this.startingPromise;
        }

        this.startingPromise = this.startingPromise
            .then(() => this._start());

        return this.startingPromise;
    }
    async stop() {
        this.stoppingPromise = this.stoppingPromise
            .then(() => this._stop());

        return this.stoppingPromise;
    }


    private async _start(): Promise<void> {
        if(this.isStopping()) {
            return;
        }

        if(this.state !== EOrigamiState.RECONNECTING) {
            this.state = EOrigamiState.CONNECTING;
        }

        this.pingConn.setSendOnly(false);
        this.publisherConn.setSendOnly(false);

        const connectResult = await this.connectAndAuthConns();

        if(connectResult === EDoStart.RESTART) {
            return this._restart();
        } else if(connectResult === EDoStart.STOP) {
            return;
        }

        if(this.state === EOrigamiState.CONNECTING && this.options.onConnect) {
            this.options.onConnect();
        } else if(this.state === EOrigamiState.RECONNECTING && this.options.onReconnect) {
            this.options.onReconnect();
        }

        this.startingLoop = false;

        const failLoadScripts = await this.loadScripts();

        if(failLoadScripts) {
            return this._restart();
        }

        const failSubscribe = await this.subscribeOnTopics();

        if(failSubscribe) {
            return this._restart();
        }

        const failInit = await this.init();

        if(failInit) {
            return this._restart();
        }

        this.pingConn.setSendOnly(true);
        this.publisherConn.setSendOnly(true);

        this.state = EOrigamiState.CONNECTED;
        this.stopped = false;

        this.lastErrMsg = '';

        this.core.start();
    }
    private async _stop(): Promise<void> {
        if(this.stopped) {
            return;
        }

        this.state = EOrigamiState.CLOSING;
        this.stopped = true;

        await this.startingPromise;

        await this.core.stopRequestsAccepting();

        await this.core.consumer.stop();

        await this.core.producer.stop();

        await this.closeConns();

        this.state = EOrigamiState.CLOSED;
    }
    private async _restart(): Promise<void> {
        if(this.isStopping()) {
            return;
        }

        this.state = EOrigamiState.RECONNECTING;
        this.startingLoop = true;

        await new Promise(r => setTimeout(r, this.options.reconnectTimeout || 2000));

        return this._start();
    }


    private async connectAndAuthConns() {
        const conns = [
            this.pingConn,
            this.listenerConn,
            this.publisherConn,
        ];

        for(const conn of conns) {
            const connectResult = await this.connectAndAuthConn(conn);

            if(connectResult !== EDoStart.CONTINUE) {
                return connectResult;
            }
        }

        return EDoStart.CONTINUE;
    }
    private async connectAndAuthConn(conn: Redis): Promise<EDoStart> {
        const connectResult = await conn.connect();

        if(!connectResult.ok) {
            await conn.close();

            return EDoStart.RESTART;
        }

        if(!this.options.password) {
            return EDoStart.CONTINUE;
        }

        const authResult = await this.timeoutRequest<string>(conn, 'auth ' + this.options.password + '\r\n', 10000);

        if(!authResult.ok) {
            if(authResult.code === ESendCode.TIMEOUT) {
                return EDoStart.RESTART;
            }

            if(this.options.onError) {
                this.options.onError(
                    new Error('Wrong password'),
                );
            }

            return EDoStart.RESTART;
        }

        if(authResult.result !== 'OK') {
            if(this.options.onError) {
                this.options.onError(
                    new Error('Wrong password'),
                );
            }

            return EDoStart.RESTART;
        }

        return EDoStart.CONTINUE;
    }


    private async closeConns() {
        const conns = [
            this.pingConn,
            this.listenerConn,
            this.publisherConn,
        ];

        for(const conn of conns) {
            conn.onMessage(undefined);

            await conn.close();
        }
    }


    private async init() {
        if(this.initialized) {
            return false;
        }

        const commandArgs: string[] = [];

        let channelsLen = 0;

        for(const [, channel] of this.core.consumer.getChannels()) {
            channelsLen += 5;

            commandArgs.push(
                channel.channel,
                channel.options.concurrent + '',
                channel.options.reservoir.enable + '',
                channel.options.reservoir.size + '',
                channel.options.reservoir.interval + '',
            );
        }

        commandArgs.push(
            this.nodeId,
            this.options.nodeName || 'none',

            this.options.appName || 'none',
            this.options.appVersion || 'none',

            process.pid + '',
            os.hostname() || 'none',
            os.platform() || 'unknown',

            client,
            clientVersion,
        );

        const scriptHash = this.scriptsStore.getHash(EScriptName.INIT);

        if(!scriptHash) {
            // wft?

            throw new Error('FUCK YOU!!! WHAT ARE YOU DOING? REVERT ALL YOUR CHANGES AND GO OUT FROM SOURCE CODE, LITTLE STUPID KID!!!!');
        }

        commandArgs.unshift('evalsha', scriptHash, channelsLen + '');

        const initResult = await this.timeoutRequest<number>(this.publisherConn, Resp.encode(commandArgs), 2000);

        if(!initResult.ok) {
            return true;
        }

        if(!initResult.result) {
            // wft?

            throw new Error('FUCK YOU!!! WHAT ARE YOU DOING? REVERT ALL YOUR CHANGES AND GO OUT FROM SOURCE CODE, LITTLE STUPID KID!!!!');
        }

        this.initialized = true;

        return false;
    }
    private async loadScripts() {
        for(const [scriptName, scriptSource] of this.scriptsStore.getScripts()) {
            if(scriptName === EScriptName.PING) {
                const loadResult = await this.timeoutRequest<string>(this.pingConn, Resp.encode(['script', 'load', scriptSource]), 2000);

                if(!loadResult.ok) {
                    return true;
                }

                this.scriptsStore.registerHash(scriptName, loadResult.result);
            } else {
                const loadResult = await this.timeoutRequest<string>(this.publisherConn, Resp.encode(['script', 'load', scriptSource]), 2000);

                if(!loadResult.ok) {
                    return true;
                }

                this.scriptsStore.registerHash(scriptName, loadResult.result);
            }
        }

        return false;
    }
    private async subscribeOnTopics() {
        this.core.fillListeners();

        const commandArgs = Object.keys(this.core.listeners);

        commandArgs.unshift('subscribe');

        const subscribeResult = await this.timeoutRequest(this.listenerConn, Resp.encode(commandArgs), 2000);

        if(!subscribeResult.ok) {
            return true;
        }

        this.listenerConn.onMessage(this.core.messagesHandler.bind(this.core));

        return false;
    }


    private setListenersOnConn(conn: Redis) {
        conn.onClose(this.onCloseConn.bind(this));
        conn.onError(this.onErrorConn.bind(this));
    }


    private async timeoutRequest<RESULT>(conn: Redis, payload: string, timeout: number): Promise<TRequest<RESULT>> {
        return new Promise<TRequest<RESULT>>(async resolve => {
            const timer = setTimeout(() => {
                resolve(BaseResult.fail(ESendCode.TIMEOUT, null));
            }, timeout);

            const result = await conn.request<RESULT>(payload);

            clearTimeout(timer);

            resolve(result);
        });
    }


    private onCloseConn() {
        if(this.startingLoop) {
            return;
        }

        this.startingLoop = true;

        this.core.stop();

        if(this.state === EOrigamiState.CONNECTING || this.state === EOrigamiState.RECONNECTING || this.state === EOrigamiState.CLOSING) {
            return;
        }

        this.state = EOrigamiState.RECONNECTING;

        this._start().then();

        if(this.options.onClose) {
            this.options.onClose();
        }
    }
    private onErrorConn(error: Error) {
        if(error.message === this.lastErrMsg) {
            return;
        }

        this.lastErrMsg = error.message;

        if(this.options.onError) {
            this.options.onError(error);
        }
    }
}

export interface IOrigamiOptions {
    nodeName?: string;
    appName?: string;
    appVersion?: string;

    password?: string;
    connection: IConnectionOptions;

    onConnect?: () => void;
    onReconnect?: () => void;
    onClose?: () => void;
    onError?: (error: Error) => void;

    reconnectTimeout?: number;
}

export enum EOrigamiState {
    RECONNECTING,
    CONNECTING,
    CONNECTED,
    CLOSING,
    CLOSED,
}

enum EDoStart {
    CONTINUE,
    RESTART,
    STOP,
}
