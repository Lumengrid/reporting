import { configureMockExpressHttpContext, MockExpressHttpContext } from '../../utils';
import { validateSetting } from '../../../src/routes/validator';

describe('Validator', () => {

    let mockExpressHttpContext: MockExpressHttpContext;
    beforeEach(() => {
        mockExpressHttpContext = configureMockExpressHttpContext();
    });

    afterEach(() => {
        mockExpressHttpContext.afterEachRestoreAllMocks();
    });

    it('Should see an error with invalid datalakeV2ExpirationTime', async () => {
        const invalidInput = ['aaaa_Test_aaa', '1', '7200', '0', 1.2, 50];

        for (const input of invalidInput) {
            const mockReq = {
                method: 'put',
                body: {datalakeV2ExpirationTime: input}
            };
            const next = jest.fn();
            const res = getMockRes();

            await testValidatorSetting(validateSetting, mockReq, res, next);
            expect(res.getStatus()).toEqual(400);
            expect(res.getJson().success).toEqual(false);
            expect(next).toBeCalledTimes(0);

        }
    });

    it('Shouldn\'t see an error with a valid datalakeV2ExpirationTime value', async () => {

        const validInput = [7200, 21600, 86400];

        for (const input of validInput) {
            const mockReq = {
                method: 'put',
                body: {datalakeV2ExpirationTime: input}
            };
            const next = jest.fn();
            const res = getMockRes();

            await testValidatorSetting(validateSetting, mockReq, res, next);
            expect(next).toBeCalled();
        }
    });

    const testValidatorSetting = async (validatorArr, mockReq, mockRes, mockNext) => {
        const next = jest.fn();
        for (let i = 0; i < validatorArr.length - 1; i++) {
            await validatorArr[i](mockReq, mockRes, next);
        }

        await validatorArr[validatorArr.length - 1](mockReq, mockRes, mockNext);

        return [mockReq, mockRes, mockNext];
    };

    function getMockRes() {
        return {
            json(jsonResponse) {
                this.json = jsonResponse;
            },
            status(status) {
                this.status = status;
                return this;
            },
            getStatus() {
                return this.status;
            },
            getJson(): any {
                return this.json;
            }
        };
    }
});
