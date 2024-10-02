import Axios from 'axios';
import { E2ETestUtils } from './utils';
import { MigrationResponse } from '../../src/models/migration-component';

jest.setTimeout(60000);

describe('Migration Service', () => {

    beforeAll(() => {
        E2ETestUtils.loadTestEnv();
    });

    it('Should be able to migrate the reports', async () => {
        const adminToken: string = await E2ETestUtils.loginAsGodAdmin();

        const reports: MigrationResponse = (await Axios.post(
            'https://hydra.docebosaas.com/analytics/v1/reports/migrations',
            {types: [1]},
            {headers: { Authorization: `Bearer ${adminToken}`}},
        )).data;

        expect(reports).toBeDefined();
    });

});
