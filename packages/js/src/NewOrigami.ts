import crypto from "crypto";

import {Consumer} from "./Consumer";
import {Producer} from "./Producer";

import {ScriptsStore} from "./ScriptsStore";
import {ESendCode, IConnectionOptions, Redis, TRequest} from "./Redis";

import {BaseResult} from "./utils/BaseResult";

export class NewOrigami {
    private nodeId = crypto.randomUUID();
    private options: IOrigamiOptions;

    private state = EOrigamiState.CLOSED;
    private stopped = false;

    private readonly pingConn: Redis;
    private readonly listenerConn: Redis;
    private readonly publisherConn: Redis;

    private readonly consumer: Consumer;
    private readonly producer: Producer;

    private readonly scriptsStore: ScriptsStore;

    private startingLoop = false;
    private startingPromise: Promise<void> = Promise.resolve();

    private stoppingPromise: Promise<void> = Promise.resolve();

    private lastErrMsg = '';

    constructor(options: IOrigamiOptions) {
        this.options = options;

        this.pingConn = new Redis(options.connection);
        this.listenerConn = new Redis(options.connection);
        this.publisherConn = new Redis(options.connection);

        this.scriptsStore = new ScriptsStore();

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

        this.setListenersOnConn(this.pingConn);
        this.setListenersOnConn(this.listenerConn);
        this.setListenersOnConn(this.publisherConn);
    }

    getState() {
        return this.state;
    }

    isStopping() {
        return this.stopped;
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

        const connectResult = await this.connectAndAuthConns();

        if(connectResult === EDoStart.RESTART) {
            if(this.isStopping()) {
                return;
            }

            this.state = EOrigamiState.RECONNECTING;
            this.startingLoop = true;

            await new Promise(r => setTimeout(r, this.options.reconnectTimeout || 2000));

            return this._start();
        } else if(connectResult === EDoStart.STOP) {
            return;
        }

        if(this.state === EOrigamiState.CONNECTING && this.options.onConnect) {
            this.options.onConnect();
        } else if(this.state === EOrigamiState.RECONNECTING && this.options.onReconnect) {
            this.options.onReconnect();
        }

        this.startingLoop = false;

        this.state = EOrigamiState.CONNECTED;
        this.stopped = false;

        this.lastErrMsg = '';

        // todo: do sent requests
    }

    private async _stop(): Promise<void> {
        if(this.stopped) {
            return;
        }

        this.state = EOrigamiState.CLOSING;
        this.stopped = true;

        await this.startingPromise;

        // todo: do stop accepting request

        // todo: do wait pending requests

        await this.closeConns();

        this.state = EOrigamiState.CLOSED;
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

        const authResult = await this.timeoutRequest<string>(conn, 'auth ' + this.options.password + '\r\n', 1000);

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
            await conn.close()
        }
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

        if(this.state === EOrigamiState.CONNECTING || this.state === EOrigamiState.RECONNECTING || this.state === EOrigamiState.CLOSING) {
            return;
        }

        this.state = EOrigamiState.RECONNECTING;

        this._start();

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
