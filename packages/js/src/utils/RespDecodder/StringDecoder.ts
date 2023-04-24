export class StringDecoder {
    private readonly encoding: BufferEncoding;

    private lastNeed = 0;
    private lastTotal = 0;
    private readonly lastChar: Buffer = Buffer.allocUnsafe(0);

    public readonly write: IWrite = this.defaultWrite.bind(this);
    public readonly fillLast: IFillLast = this.defaultFillLast.bind(this);
    public readonly text: IText = this.defaultText.bind(this);
    public readonly end: IEnd = this.defaultEnd.bind(this);

    constructor(encoding?: BufferEncoding) {
        this.encoding = StringDecoder.normalizeEncoding(encoding);

        switch (this.encoding) {
            case 'utf16le':
                this.end = this.utf16End.bind(this);
                this.text = this.utf16Text.bind(this);
                this.lastChar = Buffer.allocUnsafe(4);

                break;
            case 'utf8':
                this.lastChar = Buffer.allocUnsafe(4);
                this.fillLast = this.utf8FillLast.bind(this);

                break;
            case 'base64':
                this.end = this.base64End.bind(this);
                this.text = this.base64Text.bind(this);
                this.lastChar = Buffer.allocUnsafe(3);

                break;
            default:
                this.end = this.simpleEnd.bind(this);
                this.write = this.simpleWrite.bind(this);

                break;
        }
    }

    private defaultWrite(data: Buffer) {
        if(data.length === 0) {
            return '';
        }

        let result;
        let cursor = this.lastNeed;

        if(cursor) {
            result = this.fillLast(data);

            if(result === undefined) {
                return '';
            }

            this.lastNeed = 0;
        } else {
            cursor = 0;
        }

        if(cursor < data.length) {
            return result ? result + this.text(data, cursor) : this.text(data, cursor);
        }

        return result || '';
    }
    private simpleWrite(data: Buffer) {
        return data.toString(this.encoding);
    }

    private defaultFillLast(data: Buffer) {
        if(this.lastNeed <= data.length) {
            data.copy(this.lastChar, this.lastTotal - this.lastNeed, 0, this.lastNeed);

            return this.lastChar.toString(this.encoding, 0, this.lastTotal);
        }

        data.copy(this.lastChar, this.lastTotal - this.lastNeed, 0, data.length);

        this.lastNeed -= data.length;
    }
    private utf8FillLast(data: Buffer) {
        const result = this.utf8CheckExtraBytes(data);

        if(result !== undefined) {
            return result;
        }

        const p = this.lastTotal - this.lastNeed;

        if (this.lastNeed <= data.length) {
            data.copy(this.lastChar, p, 0, this.lastNeed);

            return this.lastChar.toString(this.encoding, 0, this.lastTotal);
        }

        data.copy(this.lastChar, p, 0, data.length);

        this.lastNeed -= data.length;
    }

    private defaultText(data: Buffer, cursor: number) {
        let total = this.utf8CheckIncomplete(data, cursor);

        if (!this.lastNeed) {
            return data.toString('utf8', cursor);
        }

        this.lastTotal = total;

        let end = data.length - (total - this.lastNeed);

        data.copy(this.lastChar, 0, end);

        return data.toString('utf8', cursor, end);
    }
    private utf16Text(data: Buffer, cursor: number) {
        if ((data.length - cursor) % 2 === 0) {
            let r = data.toString('utf16le', cursor);

            if (r) {
                let c = r.charCodeAt(r.length - 1);

                if (c >= 0xD800 && c <= 0xDBFF) {
                    this.lastNeed = 2;
                    this.lastTotal = 4;
                    this.lastChar[0] = data[data.length - 2];
                    this.lastChar[1] = data[data.length - 1];

                    return r.slice(0, -1);
                }
            }

            return r;
        }

        this.lastNeed = 1;
        this.lastTotal = 2;
        this.lastChar[0] = data[data.length - 1];

        return data.toString('utf16le', cursor, data.length - 1);
    }
    private base64Text(data: Buffer, cursor: number) {
        let n = (data.length - cursor) % 3;

        if (n === 0) {
            return data.toString('base64', cursor);
        }

        this.lastNeed = 3 - n;
        this.lastTotal = 3;

        if (n === 1) {
            this.lastChar[0] = data[data.length - 1];
        } else {
            this.lastChar[0] = data[data.length - 2];
            this.lastChar[1] = data[data.length - 1];
        }

        return data.toString('base64', cursor, data.length - n);
    }

    private defaultEnd(data: Buffer) {
        let r = data && data.length ? this.write(data) : '';

        if (this.lastNeed) {
            return r + '\ufffd';
        }

        return r;
    }
    private utf16End(data: Buffer) {
        let r = data && data.length ? this.write(data) : '';

        if (this.lastNeed) {
            let end = this.lastTotal - this.lastNeed;

            return r + this.lastChar.toString('utf16le', 0, end);
        }

        return r;
    }
    private base64End(data: Buffer) {
        let r = data && data.length ? this.write(data) : '';

        if (this.lastNeed) {
            return r + this.lastChar.toString('base64', 0, 3 - this.lastNeed);
        }

        return r;
    }
    private simpleEnd(data: Buffer) {
        return data && data.length ? this.write(data) : '';
    }

    private utf8CheckByte(byte: number) {
        if(byte <= 0x7F) {
            return 0;
        } else if(byte >> 5 === 0x06) {
            return 2;
        } else if(byte >> 4 === 0x0E) {
            return 3;
        } else if(byte >> 3 === 0x1E) {
            return 4;
        }

        return byte >> 6 === 0x02 ? -1 : -2;
    }
    private utf8CheckExtraBytes(data: Buffer) {
        if((data[0] & 0xC0) !== 0x80) {
            this.lastNeed = 0;

            return '\ufffd';
        }

        if(this.lastNeed > 1 && data.length > 1) {
            if((data[1] & 0xC0) !== 0x80) {
                this.lastNeed = 1;

                return '\ufffd';
            }

            if(this.lastNeed > 2 && data.length > 2) {
                if ((data[2] & 0xC0) !== 0x80) {
                    this.lastNeed = 2;

                    return '\ufffd';
                }
            }
        }
    }
    private utf8CheckIncomplete(data: Buffer, cursor: number) {
        let len = data.length - 1;

        if (len < cursor) {
            return 0;
        }

        let nb = this.utf8CheckByte(data[len]);

        if (nb >= 0) {
            if (nb > 0) {
                this.lastNeed = nb - 1;
            }

            return nb;
        }

        if (--len < cursor || nb === -2) {
            return 0;
        }

        nb = this.utf8CheckByte(data[len]);

        if (nb >= 0) {
            if (nb > 0) {
                this.lastNeed = nb - 2;
            }

            return nb;
        }

        if (--len < cursor || nb === -2) {
            return 0;
        }

        nb = this.utf8CheckByte(data[len]);

        if (nb >= 0) {
            if (nb > 0) {
                if (nb === 2) {
                    nb = 0;
                } else {
                    this.lastNeed = nb - 3;
                }
            }

            return nb;
        }

        return 0;
    }

    private static normalizeEncoding(encoding?: BufferEncoding): BufferEncoding {
        if(!encoding) {
            encoding = 'utf8';
        } else {
            switch (encoding.toLowerCase()) {
                case 'utf8':
                case 'utf-8':
                    encoding = 'utf8';
                    break;
                case 'ucs2':
                case 'ucs-2':
                case 'utf16le':
                case 'utf-16le':
                    encoding = 'utf16le';
                    break;
                case 'latin1':
                case 'binary':
                    encoding = 'latin1';
                    break;
                case 'base64':
                case 'ascii':
                case 'hex':
                    break;
                default:
                    throw new Error('Unknown encoding: ' + encoding);
            }
        }

        if(!Buffer.isEncoding(encoding)) {
            throw new Error('Unknown encoding: ' + encoding);
        }

        return encoding;
    }
}

interface IWrite {
    (data: Buffer): string;
}
interface IFillLast {
    (data: Buffer): string | undefined;
}
interface IText {
    (data: Buffer, cursor: number): string
}
interface IEnd {
    (data: Buffer): string
}
