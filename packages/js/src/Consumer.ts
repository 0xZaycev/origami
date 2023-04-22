import {Redis} from "./Redis";
import {Resp} from "./utils/Resp";

import {EScriptName, ScriptsStore} from "./ScriptsStore";
import {request_ackScript, responseScript} from "./scripts";


export class Consumer {
    private readonly nodeId: string;

    private readonly publisherConn: Redis;

    private readonly scriptsStore: ScriptsStore;

    private readonly channels = new Map<string, IChannel>();
    private readonly requests = new Map<string, IRequest>();

    private stoppingPromise: Promise<void> | undefined;
    private stoppingResolver = (): void | PromiseLike<void> => undefined;

    private activeRequests = 0;


    constructor(options: IConsumerOptions) {
        this.nodeId = options.nodeId;

        this.publisherConn = options.publisherConn;

        this.scriptsStore = options.scriptsStore;

        this.registerScripts();
    }


    getChannels() {
        return this.channels.entries();
    }


    listeners() {
        const listeners: [string, IMessageHandler][] = [];

        for(const [channel, {handler}] of this.getChannels()) {
            listeners.push([channel, handler]);
        }

        listeners.push(['origami.d' + this.nodeId, this.requestAck.bind(this)]);
        listeners.push(['origami.g' + this.nodeId, this.responseAck.bind(this)]);

        return listeners;
    }


    cleaner(now: number, ack: number, restart: number) {
        for(const request of this.requests.values()) {
            if(request.state === ERequestState.SENDING_REQUEST_ACK) {
                const t = now - request.timestamp;

                if(t > restart) {
                    return true;
                } else if(t > ack) {
                    this.sendRequestAck(request.id);
                }
            } else if(request.state === ERequestState.SENDING_RESPONSE) {
                const t = now - request.timestamp;

                if(t > restart) {
                    return true;
                } else if(t > ack) {
                    this.sendResponse(request.id, request.errored, request.response);
                }
            }
        }

        return false;
    }

    restart(now: number) {
        for(const request of this.requests.values()) {
            if(request.state === ERequestState.SENDING_REQUEST_ACK) {
                request.timestamp = now;

                this.sendRequestAck(request.id);
            } else if(request.state === ERequestState.SENDING_RESPONSE) {
                request.timestamp = now;

                this.sendResponse(request.id, request.errored, request.response);
            }
        }
    }


    channel<PARAMS = any, RESPONSE = any>(channelName: string, options: Partial<IChannelOptions>, handler: IChannelHandler<PARAMS, RESPONSE>) {
        const _channelName = 'origami.b' + this.nodeId + channelName;

        const channel: IChannel = {
            channel: channelName,
            options: {
                concurrent: options.concurrent || 0,
                reservoir: {
                    enable: options?.reservoir?.enable || EChannelReservoir.DISABLE,
                    size: options?.reservoir?.size || 0,
                    interval: options?.reservoir?.interval || 0,
                },
            },
            handler: this.handlerWrapper(handler),
        };

        this.channels.set(_channelName, channel);
    }


    stop() {
        if(this.stoppingPromise) {
            return this.stoppingPromise;
        }

        if(!this.activeRequests) {
            return Promise.resolve();
        }

        this.stoppingPromise = new Promise<void>(resolve => {
            this.stoppingResolver = resolve;
        });

        return this.stoppingPromise;
    }


    private async requestAck(message: string) {
        // 0-1 - result
        // 1-37 - requestId
        const requestId = message.substring(1, 37);

        const request = this.requests.get(requestId);

        if(!request) {
            // wtf?

            return;
        }

        if(request.state !== ERequestState.SENDING_REQUEST_ACK) {
            return;
        }

        if(message[0] === '0') {
            this.requests.delete(requestId);

            this.activeRequests--;

            if(!this.activeRequests && this.stoppingPromise) {
                this.stoppingResolver();
            }

            return;
        }

        request.state = ERequestState.PROCESSING_REQUEST;

        try {
            const response = await request.handler(JSON.parse(request.params), requestId, request.senderId);

            request.response = JSON.stringify(response === undefined ? null : response);
        } catch (e: any) {
            request.errored = true;
            request.response = JSON.stringify({
                name: e.name || null,
                message: e.message || null,
                stack: e.stack || null,
                code: e.code || null,
                cause: e.cause || null,
            });
        }

        request.state = ERequestState.SENDING_RESPONSE;

        this.sendResponse(requestId, request.errored, request.response);
    }
    private async responseAck(message: string) {
        // message === requestId

        const request = this.requests.get(message);

        if(!request) {
            // wtf?

            return;
        }

        request.state = ERequestState.REQUEST_PROCESSED;

        this.requests.delete(message);

        this.activeRequests--;

        if(!this.activeRequests && this.stoppingPromise) {
            this.stoppingResolver();
        }
    }


    private sendRequestAck(requestId: string) {
        const scriptHash = this.scriptsStore.getHash(EScriptName.REQUEST_ACK);

        if(!scriptHash) {
            return;
        }

        this.publisherConn.send(Resp.encode(['evalsha', scriptHash, '0', this.nodeId, requestId]));
    }
    private sendResponse(requestId: string, errored: boolean, payload: string) {
        const scriptHash = this.scriptsStore.getHash(EScriptName.RESPONSE);

        if(!scriptHash) {
            return;
        }

        this.publisherConn.send(Resp.encode(['evalsha', scriptHash, '0', this.nodeId, requestId, errored ? '1' : '0', payload]));
    }


    private handlerWrapper(handler: IChannelHandler): IMessageHandler {
        return async (message: string) => {
            // 0-36 - requestId
            // 36-72 - senderId
            // 72 - params

            if(this.stoppingPromise) {
                return;
            }

            const requestId = message.substring(0, 36);

            let request = this.requests.get(requestId);

            if(request) {
                return;
            }

            const senderId = message.substring(36, 72);
            const params = message.substring(72);

            request = {
                id: requestId,
                senderId: senderId,

                state: ERequestState.SENDING_REQUEST_ACK,

                params: params,
                response: '',
                errored: false,

                timestamp: Date.now(),

                handler: handler,
            };

            this.requests.set(requestId, request);

            this.sendRequestAck(requestId);

            this.activeRequests++;
        };
    }


    private registerScripts() {
        this.scriptsStore.registerSource(EScriptName.REQUEST_ACK, request_ackScript.getScript());
        this.scriptsStore.registerSource(EScriptName.RESPONSE, responseScript.getScript());
    }
}


export interface IConsumerOptions {
    nodeId: string;

    publisherConn: Redis;

    scriptsStore: ScriptsStore;
}


interface IMessageHandler {
    (message: string): Promise<void>;
}


export interface IChannel {
    channel: string;
    options: IChannelOptions;
    handler: IMessageHandler;
}
export interface IChannelOptions {
    concurrent: number;
    reservoir: IChannelReservoirOptions;
}
export enum EChannelReservoir {
    DISABLE,
    ENABLE,
    GROUPED,
}
export interface IChannelReservoirOptions {
    enable: EChannelReservoir;
    size: number;
    interval: number;
}
export interface IChannelHandler<PARAMS = any, RESPONSE = any> {
    (params: PARAMS, requestId: string, senderId: string): RESPONSE;
}


interface IRequest {
    id: string;
    senderId: string;

    state: ERequestState;

    params: string,
    response: string,

    errored: boolean,

    timestamp: number,

    handler: IChannelHandler;
}
enum ERequestState {
    SENDING_REQUEST_ACK,
    PROCESSING_REQUEST,
    SENDING_RESPONSE,
    REQUEST_PROCESSED,
}
