import {Socket} from 'net';

import {List} from "./utils/List";
import {Listeners} from "./utils/Listeners";
import {Reply, RespDecoder} from "./utils/RespDecodder";
import {BaseResult, TBaseResult} from "./utils/BaseResult";

export class Redis {
    public readonly name: string;

    // Connection options
    private options: IConnectionOptions;

    // Connection state
    private state: EConnectionState = EConnectionState.CLOSED;
    private stateChangeQueue = Promise.resolve<TBaseResult>(BaseResult.ok(null));

    // TCP socket
    private socket: Socket | undefined;

    private decoder: RespDecoder;

    // aka events-emitter
    private listeners: Listeners = new Listeners();

    private sendOnly = false;

    private pendingCalls = new List<(data?: any) => void>();

    private messageHandler: IMessageHandler | undefined;
    private errorHandler: IErrorHandler | undefined;
    private closeHandler: ICloseHandler | undefined;

    private _onClose = this.onSocketClose.bind(this);
    private _onError = this.onSocketError.bind(this);
    private _onReady = this.onSocketReady.bind(this);
    private _onData = this.onSocketData.bind(this);

    private dataBuffer: Buffer = Buffer.from([]);


    constructor(name: string, options: IConnectionOptions) {
        this.name = name;

        this.options = options;

        this.decoder = new RespDecoder({
            returnStringsAsBuffers: false,
            onReply: this.onParseSocketMessage.bind(this),
        });
    }


    private makeSocket() {
        this.socket = new Socket({
            allowHalfOpen: false,
            readable: true,
            writable: true,
        });

        this.socket.on('ready', this._onReady);

        if(!this.sendOnly) {
            this.socket.on('data', this._onData);
        }

        this.socket.on('error', this._onError);

        this.socket.on('end', this._onClose);
        this.socket.on('close', this._onClose);
    }
    private destroySocket() {
        if(!this.socket) {
            return;
        }

        this.dataBuffer = Buffer.from([]);

        this.socket.off('ready', this._onReady);
        this.socket.off('data', this._onData);

        this.socket.off('error', this._onError);

        this.socket.off('end', this._onClose);
        this.socket.off('close', this._onClose);

        try {
            this.socket.end();
            this.socket.destroy();
        } catch (e) {
            //
        }

        delete this.socket;

        this.socket = undefined;

        this.dropPendingCalls();

        this.decoder.reset();
    }


    connect(): Promise<TConnect> {
        this.stateChangeQueue = this.stateChangeQueue
            .then(() => this._connect());

        return this.stateChangeQueue as Promise<TConnect>;
    }
    private async _connect(): Promise<TConnect> {
        this.listeners.clear();

        this.changeState(EConnectionState.CONNECTING);

        const promise = new Promise<TConnect>((resolve) => {
            const timer = setTimeout(() => {
                removeListeners();

                this.destroySocket();

                this.changeState(EConnectionState.CLOSED);

                this.emitClose();

                resolve(
                    BaseResult
                        .fail(EConnectionCode.CONNECTION_CLOSED),
                );
            }, 5000);

            const removeListeners = () => {
                this.listeners.delete('socket:error');
                this.listeners.delete('socket:close');
                this.listeners.delete('socket:ready');

                clearTimeout(timer);
            };

            this.listeners.set('socket:error', (error: Error & {code?: string}) => {
                removeListeners();

                resolve(
                    BaseResult
                        .fail(EConnectionCode.CONNECTION_ERROR, error),
                );
            });
            this.listeners.set('socket:close', () => {
                removeListeners();

                resolve(
                    BaseResult
                        .fail(EConnectionCode.CONNECTION_CLOSED),
                );
            });
            this.listeners.set('socket:ready', async () => {
                removeListeners();

                resolve(
                    BaseResult
                        .ok(null),
                );
            });
        });

        this.makeSocket();

        this.socket!.connect({
            noDelay: true,
            keepAlive: true,
            host: this.options.host,
            port: this.options.port,
        });

        this.socket!.setKeepAlive(true, 5000);

        return promise;
    }

    close(): Promise<TClose> {
        this.stateChangeQueue = this.stateChangeQueue
            .then(() => this._close());

        return this.stateChangeQueue as Promise<TClose>;
    }
    private async _close(): Promise<TClose> {
        this.listeners.clear();

        this.changeState(EConnectionState.CLOSING);

        const promise = new Promise<TClose>((resolve) => {
            const waitingFuse = setTimeout(() => {
                removeListeners();

                this.changeState(EConnectionState.CLOSED);

                this.emitClose();

                resolve(
                    BaseResult
                        .ok(null),
                );
            }, 1000) as any as number;

            const removeListeners = () => {
                clearTimeout(waitingFuse);

                this.listeners.delete('socket:error');
                this.listeners.delete('socket:close');

                this.changeState(EConnectionState.CLOSED);
            };

            this.listeners.set('socket:error', (error: Error) => {
                removeListeners();

                resolve(
                    BaseResult
                        .fail(EConnectionCode.CONNECTION_ERROR, error),
                );
            });
            this.listeners.set('socket:close', () => {
                removeListeners();

                resolve(
                    BaseResult
                        .ok(null),
                );
            });
        });

        this.destroySocket();

        return promise;
    }


    request<RESULT = any>(data: string): Promise<TRequest<RESULT>> {
        if(this.sendOnly) {
            return Promise.resolve(
                BaseResult.fail(ESendCode.SEND_ONLY_ENABLED, null),
            );
        }

        return new Promise((resolve) => {
            if(!this.socket) {
                return resolve(BaseResult.fail(ESendCode.UNEXPECTED_ERROR, null));
            }

            this.pendingCalls.push(resolve);

            this._send(data);
        });
    }

    send(data: string): TSend {
        if(!this.sendOnly) {
            return BaseResult.fail(ESendCode.SEND_ONLY_DISABLED, null);
        }

        this._send(data);

        return BaseResult.ok(undefined);
    }

    private _send(data: string) {
        if(!this.socket) {
            return;
        }

        if(this.state !== EConnectionState.CONNECTED) {
            return;
        }

        try {
            this.socket.write(data);
        } catch (e: any) {
            this.emitError(e);
        }
    }


    setSendOnly(sendOnly: boolean) {
        if(this.sendOnly === sendOnly) {
            return;
        }

        this.sendOnly = sendOnly;

        if(!this.socket) {
            return;
        }

        if(this.sendOnly) {
            this.socket.off('data', this._onData);
        } else {
            this.socket.on('data', this._onData);
        }
    }


    onMessage(handler?: IMessageHandler) {
        this.messageHandler = handler;
    }
    onError(handler: IErrorHandler) {
        this.errorHandler = handler;
    }
    onClose(handler: ICloseHandler) {
        this.closeHandler = handler;
    }


    private emitMessage(data: [string, string, string]) {
        if(this.messageHandler) {
            this.messageHandler(data);
        }
    }
    private emitError(error: Error) {
        if(this.errorHandler) {
            this.errorHandler(error);
        }
    }
    private emitClose() {
        if(this.closeHandler) {
            this.closeHandler();
        }
    }


    // socket listeners
    private onSocketReady() {
        this.listeners.call('socket:ready');

        this.changeState(EConnectionState.CONNECTED);
    }
    private onSocketData(data: Buffer) {
        this.decoder.write(data);
    }
    private onSocketError(error: Error) {
        this.listeners.call('socket:error', error);

        this.emitError(error)

        this.destroySocket();

        this.changeState(EConnectionState.CLOSED);

        this.emitClose();
    }
    private onSocketClose(errored: boolean | undefined) {
        this.listeners.call('socket:close', !!errored);

        this.destroySocket();

        this.changeState(EConnectionState.CLOSED);

        this.emitClose();
    }

    private onParseSocketMessage(message: Reply) {
        if(!this.sendOnly) {
            this.emitMessage(message as [string, string, string]);

            const resolver = this.pendingCalls.shift();

            if(resolver) {
                resolver(BaseResult.ok(message));
            }
        }
    }


    // change connection state
    private changeState(state: EConnectionState) {
        if(this.state === state) {
            return;
        }

        this.state = state;

        if(state === EConnectionState.CLOSED) {
            this.dropPendingCalls();
        }
    }


    private dropPendingCalls() {
        const payload = BaseResult.fail(ESendCode.CONNECTION_LOST, null);

        this.pendingCalls
            .forEach((resolver) => {
                resolver(payload);
            });

        this.pendingCalls = new List<(data?: any) => void>();
    }
}

export enum EConnectionCode {
    WRONG_STATE = 'WRONG_STATE',
    CONNECTION_ERROR = 'CONNECTION_ERROR',
    CONNECTION_CLOSED = 'CONNECTION_CLOSED',
}

export enum ESendCode {
    UNEXPECTED_ERROR,
    CONNECTION_LOST,
    TIMEOUT,
    SEND_ONLY_ENABLED,
    SEND_ONLY_DISABLED,
}

export enum EConnectionState {
    CONNECTING,
    CONNECTED,
    CLOSING,
    CLOSED,
}

export interface IConnectionOptions {
    host: string;
    port: number;

    username?: string;
    password?: string;

    onStateChange?: (state: EConnectionState) => void;
}

export interface IMessageHandler {
    (message: [string, string, string]): void;
}
export interface IErrorHandler {
    (error: Error): void;
}
export interface ICloseHandler {
    (): void;
}

// responses
type TConnect = TBaseResult<null, any, EConnectionCode>;
type TClose = TBaseResult<null, any, EConnectionCode>;
type TSend = TBaseResult<void, Error, ESendCode>;
export type TRequest<RESULT = any> = TBaseResult<RESULT, Error, ESendCode>;
