export class List<TYPE = never> {
    private first: IListNode<TYPE> | null = null;
    private last: IListNode<TYPE> | null = null;

    length: number = 0;

    constructor(items?: List<TYPE> | TYPE[]) {
        if(items) {
            items.forEach((item) => {
                this.push(item);
            });
        }
    }

    push(item: TYPE): number {
        this.last = {
            value: item,
            prev: this.last,
            next: null,
        };

        if(!this.first) {
            this.first = this.last;
        }

        if(this.last.prev) {
            this.last.prev.next = this.last;
        }

        this.length++;

        return this.length;
    }

    shift(): TYPE | undefined {
        if(!this.first) {
            return undefined;
        }

        const val = this.first.value;

        this.first = this.first.next;

        if(this.first) {
            this.first.prev = null;
        } else {
            this.last = null;
        }

        this.length--;

        return val;
    }

    forEach(cb: (item: TYPE, index: number) => void) {
        for(let node = this.first, i = 0; node !== null; i++) {
            cb(node.value, i);
            node = node.next;
        }
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

class ListNode<TYPE> implements IListNode<TYPE> {
    // value: TYPE;
    // prev: ListNode<TYPE> | null;
    // next: ListNode<TYPE> | null;

    constructor(
        public value: TYPE,
        public prev: ListNode<TYPE> | null,
        public next: ListNode<TYPE> | null
    ) {
    }
}