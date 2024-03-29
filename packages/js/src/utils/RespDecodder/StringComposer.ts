import {Composer} from "./Composer";
import {StringDecoder} from "./StringDecoder";

export default class StringComposer implements Composer<string> {
    private decoder = new StringDecoder();

    private string = '';

    write(buffer: Buffer): void {
        this.string += this.decoder.write(buffer);
    }

    end(buffer: Buffer): string {
        const string = this.string + this.decoder.end(buffer);

        this.string = '';

        return string;
    }

    reset() {
        this.string = '';
    }
}
