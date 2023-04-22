type TEventName = string;
type THandler = any;

export class Listeners {
    // private store = new Map<TEventName, THandler>();
    private store: Record<TEventName, THandler> = {};

    set(event: TEventName, handler: THandler) {
        // this.store.set(event, handler);
        this.store[event] = handler;
    }
    delete(event: TEventName) {
        // this.store.delete(event);
        delete this.store[event];
    }
    clear() {
        // this.store.clear();
        this.store = {};
    }

    call(event: TEventName, params?: any) {
        const h = this.store[event];

        // if(!this.store.has(event)) {
        if(!h) {
            return;
        }

        // this.store.get(event)(params);
        h(params);
    }
}