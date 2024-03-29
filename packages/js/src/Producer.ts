import crypto from "crypto";

import {Redis} from "./Redis";
import {Resp} from "./utils/Resp";

import {EScriptName, ScriptsStore} from "./ScriptsStore";
import {requestScript, response_ackScript} from "./scripts";

import {BaseResult, TBaseResult} from "./utils/BaseResult";


export class Producer {
    private readonly nodeId: string;

    private readonly publisherConn: Redis;

    private readonly scriptsStore: ScriptsStore;

    private readonly requests = new Map<string, IRequest>();

    private stoppingPromise: Promise<void> | undefined;
    private stoppingResolver = (): void | PromiseLike<void> => undefined;

    private activeRequests = 0;


    constructor(options: IProducerOptions) {
        this.nodeId = options.nodeId;

        this.publisherConn = options.publisherConn;

        this.scriptsStore = options.scriptsStore;

        this.registerScripts();
    }


    listeners() {
        const listeners: [string, IMessageHandler][] = [];

        listeners.push(['origami.c' + this.nodeId, this.requestAck.bind(this)]);
        listeners.push(['origami.e' + this.nodeId, this.requestResponse.bind(this)]);
        listeners.push(['origami.f' + this.nodeId, this.responseAck.bind(this)]);

        return listeners;
    }


    cleaner(now: number, ack: number, restart: number) {
        for(const request of this.requests.values()) {
            if(request.state === ERequestState.SENDING_REQUEST) {
                const t = now - request.timestamp;

                if(t > restart) {
                    return true;
                } else if(t > ack) {
                    this.sendRequest(request);
                }
            } else if(request.state === ERequestState.SENDING_RESPONSE_ACK) {
                const t = now - request.timestamp;

                if(t > restart) {
                    return true;
                } else if(t > ack) {
                    this.sendResponseAck(request.id);
                }
            }
        }

        return false;
    }

    restart(now: number) {
        for(const request of this.requests.values()) {
            if(request.state === ERequestState.SENDING_REQUEST) {
                request.timestamp = now;

                this.sendRequest(request);
            } else if(request.state === ERequestState.SENDING_RESPONSE_ACK) {
                request.timestamp = now;

                this.sendResponseAck(request.id);
            }
        }

        return false;
    }


    request<RESPONSE = any, PARAMS = any>(channel: string, params: PARAMS, options?: Partial<IRequestOptions>): TRequestResponse<RESPONSE> {
        return new Promise(resolve => {
            const request: IRequest = {
                id: crypto.randomUUID(),

                state: ERequestState.SENDING_REQUEST,

                channel: channel,
                group: options?.group || 'default',
                weight: options?.weight || 0,

                tryAfter: options?.tryAfter || 0,
                timeout: options?.timeout || 0,

                noResponse: !!options?.noResponse,
                noResponseAck: !!options?.noResponseAck,

                params: JSON.stringify(params === undefined ? null : params),
                response: null,

                errored: false,

                timestamp: Date.now(),

                resolver: resolve,
            };

            this.requests.set(request.id, request);

            this.sendRequest(request);

            this.activeRequests++;
        });
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
        // message === requestId

        const request = this.requests.get(message);

        if(!request) {
            // wtf?

            return;
        }

        if(request.noResponse) {
            this.requests.delete(message);

            this.activeRequests--;

            if(!this.activeRequests && this.stoppingPromise) {
                this.stoppingResolver();
            }

            return;
        }

        request.state = ERequestState.PROCESSING_REQUEST;
    }
    private async requestResponse(message: string) {
        // message === requestId

        const request = this.requests.get(message);

        if(!request) {
            // wtf?

            return;
        }

        request.state = ERequestState.SENDING_RESPONSE_ACK;
        request.timestamp = Date.now();

        this.sendResponseAck(message);
    }
    private async responseAck(message: string) {
        // 0-1 - is error
        // 1-37 - requestId
        // 37-73 - executor node id (optional, if timeout executor node id length is 0)
        // 37/73 - response

        const requestId = message.substring(1, 37);

        const request = this.requests.get(requestId);

        if(!request) {
            // wtf?

            return;
        }

        request.state = ERequestState.REQUEST_PROCESSED;

        if(message[0] === '0') {
            const executorId = message.substring(37, 73);
            const response = message.substring(73);

            try {
                request.resolver(BaseResult.ok(JSON.parse(response), executorId));
            } catch (e) {
                request.resolver(BaseResult.fail(EResponseCode.UNEXPECTED_ERROR, e, executorId));
            }
        } else if(message[0] === '1') {
            const executorId = message.substring(37, 73);
            const response = message.substring(73);

            request.resolver(BaseResult.fail(EResponseCode.UNEXPECTED_ERROR, JSON.parse(response), executorId));
        } else {
            request.resolver(BaseResult.fail(EResponseCode.TIMEOUT_ERROR, null));
        }

        this.requests.delete(requestId);

        this.activeRequests--;

        if(!this.activeRequests && this.stoppingPromise) {
            this.stoppingResolver();
        }
    }


    private sendRequest(request: IRequest) {
        const scriptHash = this.scriptsStore.getHash(EScriptName.REQUEST);

        if(!scriptHash) {
            return;
        }

        this.publisherConn.send(Resp.encode([
            'evalsha', scriptHash, '0',

            this.nodeId,

            request.id,
            request.channel,
            request.params,
            request.group,
            request.noResponse ? '1' : '0',
            request.timeout + '',
            request.tryAfter + '',
            request.weight + '',
        ]));
    }
    private sendResponseAck(requestId: string) {
        const scriptHash = this.scriptsStore.getHash(EScriptName.RESPONSE_ACK);

        if(!scriptHash) {
            return;
        }

        this.publisherConn.send(Resp.encode(['evalsha', scriptHash, '0', this.nodeId, requestId]));
    }


    private registerScripts() {
        this.scriptsStore.registerSource(EScriptName.REQUEST, requestScript.getScript());
        this.scriptsStore.registerSource(EScriptName.RESPONSE_ACK, response_ackScript.getScript());
    }
}

export interface IProducerOptions {
    nodeId: string;

    publisherConn: Redis;

    scriptsStore: ScriptsStore;
}


interface IMessageHandler {
    (message: string): Promise<void>;
}


export interface IRequestOptions {
    group: string;
    weight: number;

    tryAfter: number;
    timeout: number;

    noResponse: boolean;
    noResponseAck: boolean;
}
export enum EResponseCode {
    UNEXPECTED_ERROR,
    TIMEOUT_ERROR,
}

export type TRequestResponse<RESPONSE = any> = Promise<TBaseResult<RESPONSE, any, EResponseCode>>;

interface IRequest {
    id: string;

    state: ERequestState;

    channel: string;
    group: string;
    weight: number;

    tryAfter: number;
    timeout: number;

    noResponse: boolean;
    noResponseAck: boolean;

    params: string;
    response: any;

    errored: boolean;

    timestamp: number;

    resolver: (response: any) => any;
}
enum ERequestState {
    SENDING_REQUEST,
    PROCESSING_REQUEST,
    SENDING_RESPONSE_ACK,
    REQUEST_PROCESSED,
}
