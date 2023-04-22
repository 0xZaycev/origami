export class HashList<TYPE = never> {
    private first: IListNode<TYPE> | null = null;
    private last: IListNode<TYPE> | null = null;

    list: Record<string, IListNode<TYPE>> = {};

    length: number = 0;

    constructor() {}

    add(key: string, item: TYPE) {
        if(this.list[key]) {
            return;
        }

        this.last = {
            value: item,
            prev: this.last,
            next: null,
        };

        if(!this.first) {
            this.first = this.last;
        }

        if (this.last.prev) {
            this.last.prev.next = this.last;
        }

        this.length++;

        return this.length;
    }

    get(key: string): TYPE | undefined {
        return this.list[key]?.value;
    }

    delete(key: string) {
        if(!this.list[key]) {
            return;
        }

        const node = this.list[key];

        if(node.next) {
            if(node.prev) {
                node.next.prev = node.prev;
                node.prev.next = node.next;
            } else {
                node.next.prev = null;
                this.first = node.next;
            }
        } else if(node.prev) {
            node.prev.next = null;
            this.last = node.prev;
        } else {
            this.last = null;
            this.first = null;
        }

        this.length--;

        delete this.list[key];
    }

    *[Symbol.iterator]() {
        for(let node = this.first; node; node = node.next) {
            yield node.value;
        }
    }
}

interface IListNode<TYPE> {
    value: TYPE;
    prev: IListNode<TYPE> | null;
    next: IListNode<TYPE> | null;
}