interface IOkResult<RESULT = any> {
    ok: true;
    code: null,
    result: RESULT;
}
interface IFailResult<CODE = string, RESULT = null> {
    ok: false;
    code: CODE;
    result?: RESULT | null;
}

export type TBaseResult<OK_RESULT = null, FAIL_RESULT = null, CODE = string> = IOkResult<OK_RESULT> | IFailResult<CODE, FAIL_RESULT>;

export const BaseResult = {
    ok<RESULT = any>(result: RESULT): IOkResult<RESULT> {
        return {
            ok: true,
            code: null,
            result: result,
        };
    },
    fail<CODE = string, RESULT = any>(code: CODE, result?: RESULT): IFailResult<CODE, RESULT> {
        return {
            ok: false,
            code: code,
            result: result,
        };
    },
};