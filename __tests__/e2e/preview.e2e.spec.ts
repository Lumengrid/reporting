import Axios from 'axios';
import { E2ETestUtils } from './utils';
import { ErrorsCode } from '../../src/models/base';

// Timeout is long since the preview might take a few seconds
jest.setTimeout(100000);

describe('Reports Service', () => {
    let adminToken: string;
    const testReportMetadata = {
        type: 'Users - Courses',
        name: 'E2E test report',
        description: 'This report is created automatically by a e2e test! You should never see this!'
    };

    beforeAll(async () => {
        E2ETestUtils.loadTestEnv();
        adminToken = await E2ETestUtils.loginAsGodAdmin();
    });


    it('Should be able to preview a report', async () => {
        const testReportCreationResponse = await Axios.post(
            'https://hydra.docebosaas.com/analytics/v1/reports',
            testReportMetadata,
            {headers: {Authorization: `Bearer ${adminToken}`}}
        );
        const testReportId = testReportCreationResponse.data.data.idReport;

        expect(testReportCreationResponse.status).toBe(200);
        expect(testReportId).toBeDefined();

        const testReportPreviewResponse = await Axios.get(
            `https://hydra.docebosaas.com/analytics/v1/reports/${testReportId}/preview`,
            {headers: {Authorization: `Bearer ${adminToken}`}}
        );

        expect(testReportPreviewResponse.status).toBe(200);
        if (testReportPreviewResponse.data.errorCode === ErrorsCode.DataLakeRefreshInProgress) {
            console.warn('Data lake refresh in progress for this platform, skipping tests');
        } else {
            expect(testReportPreviewResponse.data.success).toBeTruthy();
            expect(testReportPreviewResponse.data.data.length).toBeDefined();
        }

        const testReportDeletionResponse = await Axios.delete(
            `https://hydra.docebosaas.com/analytics/v1/reports/${testReportId}`,
            {headers: {Authorization: `Bearer ${adminToken}`}}
        );
        expect(testReportDeletionResponse.status).toBe(200);

    });

});
