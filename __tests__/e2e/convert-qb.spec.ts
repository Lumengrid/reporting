import { E2ETestUtils } from './utils';
import axios, { AxiosResponse } from 'axios';
import { bodyArray } from './convert-qb-bodies.js';
import { bodyArray as bodyArrayFails} from './convert-qb-bodies_fails.js';

jest.setTimeout(300000);

// @ts-ignore
describe.each(bodyArray)('Query builder conversion suite', (query) => {
    let adminToken: string;
    beforeAll(async () => {
        E2ETestUtils.loadTestEnv();
        if (!adminToken) {
            adminToken = await E2ETestUtils.loginAsGodAdmin();
        }
    });

    it(`Should convert queries ${bodyArray.indexOf(query) + 1}`, async () => {
            const testReportCreationResponse = await axios.post(
                'https://hydra.docebosaas.com/analytics/v1/convert-qb',
                query,
                {headers: {Authorization: `Bearer ${adminToken}`}, validateStatus: () => true}
            );
            expect(testReportCreationResponse.data).toBeDefined();
            expect(testReportCreationResponse.data.success).toEqual(true);
    });
});

describe('Test all TR queries', () => {
    let adminToken: string;
    beforeAll(async () => {
        E2ETestUtils.loadTestEnv();
        if (!adminToken) {
            adminToken = await E2ETestUtils.loginAsGodAdmin();
        }
    });

    it('Should convert at least 50% of the queries', async () => {
        const allBodyRequests = [...bodyArray, ...bodyArrayFails];
        let countSuccess = 0;
        for (const body of allBodyRequests) {
            const testReportCreationResponse = await axios.post(
                'https://hydra.docebosaas.com/analytics/v1/convert-qb',
                body,
                {headers: {Authorization: `Bearer ${adminToken}`}, validateStatus: () => true}
            );
            if (testReportCreationResponse.data.success) countSuccess++;
        }
        expect(countSuccess).toBeGreaterThanOrEqual(allBodyRequests.length / 2);
        console.log(`Success rate is ${Math.floor((100 / allBodyRequests.length) * countSuccess)}%`);
    });

});

