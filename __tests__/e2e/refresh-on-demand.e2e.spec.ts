import { E2ETestUtils } from './utils';
import Axios from 'axios';
import { ErrorsCode } from '../../src/models/base';

jest.setTimeout(50000);

describe('Refresh on demand functionality', () => {
    const bjParameters = {bjName: 'Refresh in progress'};
    let adminToken: string;

    beforeAll(async () => {
        E2ETestUtils.loadTestEnv();
        if (!adminToken) {
            adminToken = await E2ETestUtils.loginAsGodAdmin();
        }
    });

    it('Should be able as ERP admin to change the datalake settings', async () => {
        const response = await Axios.put(
            'https://hydra.docebosaas.com/analytics/v1/reports/settings',
            {
                monthlyRefreshTokens: 1000,
                dailyRefreshTokens: 1000
            },
            {headers: { Authorization: `Bearer ${adminToken}`}},
        );

        expect(response.data.success).toBeTruthy();

    });

    it('Should start the refresh on demand procedure', async () => {
        if (await E2ETestUtils.checkToggleDatalakeV2(adminToken)) {
            console.warn('[skipped] Datalake v2 is active');
            return;
        }

        const response = await Axios.post(
            'https://hydra.docebosaas.com/analytics/v1/reports/refresh-on-demand',
            bjParameters,
            {headers: { Authorization: `Bearer ${adminToken}`}},
        );
        if (response.data.errorCode === ErrorsCode.DataLakeRefreshInProgress) {
            console.warn('[skipped] Datalake is already in progress..');
        } else {
            expect(response?.data?.success).toBeTruthy();
        }

    });

    it('Should notify the user that the refresh is terminated', async () => {
        if (!process.env.AAMON_PORT) {
            console.warn('[skipped] Missing key AAMON_PORT in .env-e2e file..');
            return;
        }
        if (!process.env.AAMON_HOST) {
            console.warn('[skipped] Missing key AAMON_HOST in .env-e2e file..');
            return;
        }

        // this is an internal api, so it does not pass through nginx
        const aamonPort = process.env.AAMON_PORT;
        const aamonHost = process.env.AAMON_HOST;

        const response = await Axios.post(
            `http://${aamonHost}:${aamonPort}/aamon/reports/scheduled/export`,
            {
                platforms: ['hydra.docebosaas.com'],
                isRefreshOnDemand: true
            },
        );
        expect(response?.data?.status).toBeTruthy();
    });
});
