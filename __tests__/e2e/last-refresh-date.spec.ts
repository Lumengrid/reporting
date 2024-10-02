import Axios from 'axios';
import { E2ETestUtils } from './utils';
import { DataLakeRefreshStatus } from '../../src/models/base';

jest.setTimeout(30000);

describe('Last Refresh Date', () => {

    beforeAll(() => {
        E2ETestUtils.loadTestEnv();
    });

    it('Should be a properly formatted date time', async () => {
        const adminToken: string = await E2ETestUtils.loginAsGodAdmin();

        const response: { data: { refreshDate: string, refreshStatus: string } } = (await Axios.get(
            'https://hydra.docebosaas.com/analytics/v1/reports/last-refresh-date',
            {headers: { Authorization: `Bearer ${adminToken}`}},
        )).data;

        if (response?.data.refreshStatus === DataLakeRefreshStatus.RefreshInProgress) {
            console.warn('[skipped] Data refresh is in progress');
            return;
        } else {
            // The current date/time format is exactly 19 chars
            expect(response?.data?.refreshDate?.length === 19 || response?.data?.refreshDate?.length === 0).toBeTruthy();
        }
    });

});
