import { Redis } from '../../../src/services/redis/redis';
import Config from '../../../src/config';
import PlatformManager from '../../../src/services/session/platform-manager.session';
import { SidekiqScheduler } from '../../../src/models/sidekiq-scheduler';
import SessionManager from '../../../src/services/session/session-manager.session';
import { SidekiqSchedulerItem } from '../../../src/reports/interfaces/extraction.interface';
import { redisFactory } from '../../../src/services/redis/RedisFactory';
import { SidekiqManagerService } from '../../../src/services/sidekiq-manager-service';

jest.setTimeout(90000);

describe('Sidekiq scheduler', () => {

  it('Check address for SidekiqScheduler', async () => {
    const mock = jest.fn();
    let args;
    const redis = new mock() as Redis;
    const config = new mock() as Config;
    config.internalUrlPrefix = 'aamon';
    redis.getAPIGatewaySSLVerify = (platform: string): Promise<number> => {
      return new Promise(async (resolve) => {
        resolve(0);
      });
    };
    const session = new mock() as SessionManager;

    const platformManager = new mock() as PlatformManager;

    platformManager.getPlatformBaseUrl = jest.fn((): string => 'hydra.docebosaas.com');
    redisFactory.getRedis = jest.fn((): Redis => redis);
    session.platform = platformManager;

    const fakeSidekiqManagerService = new mock() as SidekiqManagerService;

    fakeSidekiqManagerService.storeSidekiqSchedulerItem = (itemScheduler: SidekiqSchedulerItem): Promise<void> => {
      return new Promise(async (resolve) => {
        args = itemScheduler.args;
        resolve(undefined);
      });
    };

    redisFactory.getSidekiqClient = jest.fn(async (): Promise<SidekiqManagerService> => fakeSidekiqManagerService);

    const sidekiqScheduler = new SidekiqScheduler(session.platform.getPlatformBaseUrl());

    const planningOption = {
      scheduleFrom: '2001-01-01',
      startHour: '10:00',
      timezone: 'Europe/Rome'
    };

    redis.getAPIGatewaySSLVerify = (platform: string): Promise<number> => {
      return new Promise(async (resolve) => {
        resolve(0);
      });
    };

    redis.getAPIGatewayPortParam = (platform: string): Promise<number> => {
      return new Promise(async (resolve) => {
        resolve(80);
      });
    };

    redis.getAPIGatewayParam = (platform: string): Promise<string> => {
      return new Promise(async (resolve) => {
        resolve('foo.internal.it');
      });
    };

    await sidekiqScheduler.activateScheduling('123', planningOption);

    expect(args[2]).toEqual("/usr/bin/wget --header='Host: hydra.docebosaas.com' --spider --no-check-certificate -q -t 1 'http://foo.internal.it/aamon/reports/123/sidekiq-schedulation/hydra.docebosaas.com'");

    redis.getAPIGatewaySSLVerify = (platform: string): Promise<number> => {
      return new Promise(async (resolve) => {
        resolve(1);
      });
    };

    redis.getAPIGatewayPortParam = (platform: string): Promise<number> => {
      return new Promise(async (resolve) => {
        resolve(80);
      });
    };

    redis.getAPIGatewayParam = (platform: string): Promise<string> => {
      return new Promise(async (resolve) => {
        resolve('foo.internal.it');
      });
    };
    await sidekiqScheduler.activateScheduling('123', planningOption);
    expect(args[2]).toEqual("/usr/bin/wget --header='Host: hydra.docebosaas.com' --spider  -q -t 1 'http://foo.internal.it/aamon/reports/123/sidekiq-schedulation/hydra.docebosaas.com'");

    redis.getAPIGatewaySSLVerify = (platform: string): Promise<number> => {
      return new Promise(async (resolve) => {
        resolve(1);
      });
    };

    redis.getAPIGatewayPortParam = (platform: string): Promise<number> => {
      return new Promise(async (resolve) => {
        resolve(443);
      });
    };

    redis.getAPIGatewayParam = (platform: string): Promise<string> => {
      return new Promise(async (resolve) => {
        resolve('foo.internal.it');
      });
    };
    await sidekiqScheduler.activateScheduling('123', planningOption);
    expect(args[2]).toEqual("/usr/bin/wget --header='Host: hydra.docebosaas.com' --spider  -q -t 1 'https://foo.internal.it/aamon/reports/123/sidekiq-schedulation/hydra.docebosaas.com'");

    redis.getAPIGatewaySSLVerify = (platform: string): Promise<number> => {
      return new Promise(async (resolve) => {
        resolve(0);
      });
    };

    redis.getAPIGatewayPortParam = (platform: string): Promise<number> => {
      return new Promise(async (resolve) => {
        resolve(443);
      });
    };

    redis.getAPIGatewayParam = (platform: string): Promise<string> => {
      return new Promise(async (resolve) => {
        resolve('foo.internal.it');
      });
    };
    await sidekiqScheduler.activateScheduling('123', planningOption);
    expect(args[2]).toEqual("/usr/bin/wget --header='Host: hydra.docebosaas.com' --spider --no-check-certificate -q -t 1 'https://foo.internal.it/aamon/reports/123/sidekiq-schedulation/hydra.docebosaas.com'");

  });
});
