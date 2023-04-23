import {Socket} from 'net';

import {List} from "./utils/List";
import {Resp} from "./utils/Resp";
import {Listeners} from "./utils/Listeners";
import {BaseResult, TBaseResult} from "./utils/BaseResult";

export class Redis {
    // Connection options
    private options: IConnectionOptions;

    // Connection state
    private state: EConnectionState = EConnectionState.CREATED;
    private stateChangeQueue = Promise.resolve<TBaseResult>(BaseResult.ok(null));

    // TCP socket
    private socket: Socket = new Socket();

    // aka events-emitter
    private listeners: Listeners = new Listeners();

    private sendOnly = false;

    private pendingCalls = new List<(data?: any) => void>();

    private messageHandler: IMessageHandler | undefined;
    private errorHandler: IErrorHandler | undefined;
    private closeHandler: ICloseHandler | undefined;

    private _onData = this.onSocketData.bind(this);

    private dataBuffer: Buffer = Buffer.from([]);

    constructor(options: IConnectionOptions) {
        this.options = options;

        this.prepareSocket();
    }



    connect(): Promise<TConnect> {
        this.stateChangeQueue = this.stateChangeQueue
            .then(() => this._connect());

        return this.stateChangeQueue as Promise<TConnect>;
    }
    private async _connect(): Promise<TConnect> {
        if(!(this.state === EConnectionState.CREATED || this.state === EConnectionState.CLOSED)) {
            return BaseResult
                .fail(EConnectionCode.WRONG_STATE, 'in connect if state');
        }

        this.listeners.clear();

        this.changeState(EConnectionState.CONNECTING);

        const promise = new Promise<TConnect>((resolve) => {
            const timer = setTimeout(() => {
                removeListeners();

                this.changeState(EConnectionState.CLOSED);

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

            this.listeners.set('socket:error', (error: Error) => {
                removeListeners();

                this.changeState(EConnectionState.CLOSED);

                resolve(
                    BaseResult
                        .fail(EConnectionCode.CONNECTION_ERROR, error),
                );
            });
            this.listeners.set('socket:close', () => {
                removeListeners();

                this.changeState(EConnectionState.CLOSED);

                resolve(
                    BaseResult
                        .fail(EConnectionCode.CONNECTION_CLOSED),
                );
            });
            this.listeners.set('socket:ready', async () => {
                removeListeners();

                this.changeState(EConnectionState.CONNECTED);

                resolve(
                    BaseResult
                        .ok(null),
                );
            });
        });

        this.socket.connect({
            noDelay: true,
            keepAlive: true,
            host: this.options.host,
            port: this.options.port,
        });

        this.socket.setKeepAlive(true, 5000);

        return promise;
    }

    close(): Promise<TClose> {
        this.stateChangeQueue = this.stateChangeQueue
            .then(() => this._close());

        return this.stateChangeQueue as Promise<TClose>;
    }
    private async _close(): Promise<TClose> {
        // if(this.state !== EConnectionState.CONNECTED) {
        //     this.changeState(EConnectionState.CLOSED);
        //
        //     return BaseResult
        //         .fail(EConnectionCode.WRONG_STATE, 'in close if state');
        // }

        this.listeners.clear();

        this.changeState(EConnectionState.CLOSING);

        const promise = new Promise<TClose>((resolve) => {
            const waitingFuse = setTimeout(() => {
                removeListeners();

                resolve(
                    BaseResult
                        .ok(null),
                );
            }, 500) as any as number;

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

        try {
            this.socket.end();
            this.socket.destroy();
        } catch (e) {
            console.log('Error when closing connection', e);
        }

        return promise;
    }



    request<RESULT = any>(data: string): Promise<TRequest<RESULT>> {
        if(this.state !== EConnectionState.CONNECTED) {
            return Promise.resolve(
                BaseResult.fail(ESendCode.CONNECTION_LOST, null),
            );
        }

        if(this.sendOnly) {
            return Promise.resolve(
                BaseResult.fail(ESendCode.SEND_ONLY_ENABLED, null),
            );
        }

        return new Promise((resolve) => {
            try {
                this.socket.write(data);
                this.pendingCalls.push(resolve);
            } catch (e) {
                resolve(BaseResult.fail(ESendCode.UNEXPECTED_ERROR, e as Error));
            }
        });
    }

    send(data: string): TSend {
        if(this.state !== EConnectionState.CONNECTED) {
            return BaseResult.fail(ESendCode.CONNECTION_LOST, null);
        }

        if(!this.sendOnly) {
            return BaseResult.fail(ESendCode.SEND_ONLY_DISABLED, null);
        }

        try {
            this.socket.write(data);

            return BaseResult.ok(undefined);
        } catch (e) {
            return BaseResult.fail(ESendCode.UNEXPECTED_ERROR, e as Error);
        }
    }

    setSendOnly(sendOnly: boolean) {
        if(this.sendOnly === sendOnly) {
            return;
        }

        this.sendOnly = sendOnly;

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



    // binding socket events
    private prepareSocket() {
        this.socket.on('ready', this.onSocketReady.bind(this));
        this.socket.on('data', this._onData);

        this.socket.on('error', this.onSocketError.bind(this));

        this.socket.on('end', this.onSocketClose.bind(this));
        this.socket.on('close', this.onSocketClose.bind(this));
    }

    // socket listeners
    private onSocketReady() {
        this.listeners.call('socket:ready');
    }
    private onSocketData(data: Buffer) {
        if(this.dataBuffer.length) {
            data = Buffer.concat([this.dataBuffer, data]);
            this.dataBuffer = Buffer.from([]);
        }

        while(true) {
            if(!data.length) {
                return;
            }

            const parsedData = Resp.decode(data);

            if(parsedData === undefined) {
                this.dataBuffer = data;

                return;
            }

            data = data.subarray(parsedData[1]);

            if(parsedData[0] === undefined) {
                if(data.length) {

                    this.dataBuffer = data;

                    return;
                }

                return;
            }

            if(!this.sendOnly) {
                if(this.messageHandler) {
                    this.messageHandler(parsedData[0]);

                    return;
                }

                const resolver = this.pendingCalls.shift();

                if(!resolver) {
                    continue;
                }

                resolver(BaseResult.ok(parsedData[0]));
            }
        }
    }
    private onSocketError(error: Error) {
        this.listeners.call('socket:error', error);

        if(this.errorHandler) {
            this.errorHandler(error);
        }
    }
    private onSocketClose(errored: boolean | undefined) {
        this.listeners.call('socket:close', !!errored);

        this.changeState(EConnectionState.CLOSED);
    }

    // change connection state
    private changeState(state: EConnectionState) {
        if(this.state === state) {
            return;
        }

        this.state = state;

        if(this.options.onStateChange) {
            try {
                this.options.onStateChange(state);
            } catch (e) {
                console.log('Unexpected error in onStateChange handler', e);
            }
        }

        if(state === EConnectionState.CLOSED) {
            this.dropPendingCalls();

            if(this.closeHandler) {
                this.closeHandler();
            }
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
    CREATED,
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
