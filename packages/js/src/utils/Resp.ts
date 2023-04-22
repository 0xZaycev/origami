const CRLF = '\r\n';

export class Resp {
    static encode(args: string[]): string {
        let strings = '*' + args.length + CRLF;

        for (let i = 0; i < args.length; i++) {
            const arg = args[i];

            strings += '$' + Buffer.byteLength(arg) + CRLF + arg + CRLF;
        }

        return strings;
    }

    static decode(data: Buffer) {
        if(data[0] === 10) {
            return this.parseType(data, data[1], 2);
        }

        return this.parseType(data, data[0], 1);
    }

    private static parseType(data: Buffer, type: Types, cursor: number) {
        switch (type) {
            case Types.SIMPLE_STRING:
                return this.parseSimpleString(data, cursor);

            case Types.ERROR:
                return this.parseSimpleString(data, cursor);

            case Types.INTEGER:
                return this.parseInteger(data, cursor);

            case Types.BULK_STRING:
                return this.parseBulkString(data, cursor);

            case Types.ARRAY:
                return this.parseArray(data, cursor);

            // default:
            //     return undefined;
        }
    }

    private static parseSimpleString(data: Buffer, cursor: number): [string | undefined, number] {
        let crIndex = 0;

        while(true) {
            const i = cursor + crIndex;

            if(data[i] !== ASCII.CR) {
                if(i > data.length) {
                    return [undefined, cursor - 1];
                }

                crIndex++;

                continue;
            }

            const string = data.subarray(cursor, i).toString();

            return [string, i + 2];
        }
    }

    private static parseInteger(data: Buffer, cursor: number): [number | undefined, number] {
        const result = this.parseSimpleString(data, cursor);

        if(result[0] === undefined) {
            return result as [undefined, number];
        }

        return [+result[0], result[1]];
    }

    private static parseBulkString(data: Buffer, cursor: number): [string | null | undefined, number] {
        const result = this.parseInteger(data, cursor);

        if(result[0] === undefined) {
            return result as [undefined, number];
        }

        if(result[0] === -1) {
            return [null, result[1] + 4];
        }

        const string = data.subarray(result[1], result[1] + result[0]).toString();

        return [string, result[1] + result[0] + 2];
    }

    private static parseArray(data: Buffer, cursor: number): [any, number] {
        let result = this.parseInteger(data, cursor);

        if(result[0] === undefined) {
            return result;
        }

        if((result[0]) === -1) {
            return [null, result[1] + 4];
        }

        const startCursor = cursor - 1;

        const array = [];

        cursor = result[1];

        while((result[0])--) {
            const [item, _cursor] = this.parseType(data, data[cursor], cursor + 1);

            if(item === undefined) {
                return [undefined, startCursor];
            }

            array.push(item);

            cursor = _cursor;
        }

        return [array, cursor];
    }
}

enum Types {
    SIMPLE_STRING = 43, // +
    ERROR = 45, // -
    INTEGER = 58, // :
    BULK_STRING = 36, // $
    ARRAY = 42 // *
}

enum ASCII {
    CR = 13, // \r
    ZERO = 48,
    MINUS = 45
}