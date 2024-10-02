import { PlatformSettingsRedis } from '../../../../src/services/session/platform-manager.session';
import { redisFactory } from '../../../../src/services/redis/RedisFactory';

describe('Redis Client', () => {
    afterAll(async () => {
        redisFactory.drainPools();
    });

    it('Should be able to read all settings', async () => {
        const configs: PlatformSettingsRedis = await await redisFactory.getRedis().getConfigs('hydra.docebosaas.com');

        expect(configs).toBeDefined();
        expect(Object.keys(configs)).toHaveLength(40);
    });
});
