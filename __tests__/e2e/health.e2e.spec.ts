import { E2ETestUtils } from './utils';
import Axios from 'axios';

jest.setTimeout(30000);

describe('Health', () => {

    beforeAll((done: Function) => {
        E2ETestUtils.loadTestEnv();

        done();
    });

    it('Should respond 200 OK if the service is up and running', async () => {
        const healthCheck = await Axios.get(
            'https://hydra.docebosaas.com/analytics/v1/health'
        );

        expect(healthCheck).toBeDefined();

    });

    it('Should check if hydra is up and running', async () => {
        const token: string = await E2ETestUtils.loginAsGodAdmin();

        expect(token).toBeDefined();
        expect(typeof token).toBe('string');
        expect(token).toHaveLength(40);

    });
});
