import { HealthComponent } from '../../../src/routes/get/health.component';

describe('Health Component', () => {
    it('Should be able to set up dependencies and pong back', async () => {
        const component = new HealthComponent();
        expect(await component.execute()).toBe('OK');
    });
});
