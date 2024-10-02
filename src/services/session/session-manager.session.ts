import { Athena } from '../athena';
import { Dynamo } from '../dynamo';
import Hydra from '../hydra';
import { Redis } from '../redis/redis';
import { S3 } from '../s3';
import PermissionManager from './permission-manager.session';
import PlatformManager from './platform-manager.session';
import UserManager from './user-manager.session';
import { ReportsSettings } from '../../models/base';
import CacheService from '../cache/cache';
import { NotFoundException } from '../../exceptions/not-found.exception';
import { StepFunction } from '../step-function';
import { SessionLoggerService } from '../logger/session-logger.service';
import httpContext from 'express-http-context';
import { SQS } from '../sqs';
import { Snowflake } from '../snowflake/snowflake';
import { redisFactory } from '../redis/RedisFactory';

export default class SessionManager {
    private readonly dynamo: Dynamo;
    private readonly athena: Athena;
    private readonly s3: S3;
    private readonly stepFunction: StepFunction;
    private readonly sqs: SQS;
    private readonly snowflake: Snowflake;

    public static async init(hydra: Hydra, cache: CacheService, isAnonymous = false, logger?: SessionLoggerService): Promise<SessionManager> {
        logger = logger ?? httpContext.get('logger');

        try {
            const redis = redisFactory.getRedis();
            const validDomain = await redis.checkValidDomain(hydra.getHostname());

            if (!validDomain) {
                throw new NotFoundException(`Invalid domain ${hydra.getHostname()}`);
            }

            const session = await hydra.session();

            const user = session.data.user
              ? new UserManager(session)
              : new UserManager();

            const platform = new PlatformManager(session);
            platform.loadSettings(await redis.getConfigs(platform.getPlatformBaseUrl()));
            const permission = new PermissionManager();
            const sessionManager = new SessionManager(hydra, redis, user, platform, permission, logger);
            await sessionManager.loadDynamoSettings(cache, isAnonymous, logger);

            return sessionManager;
        } catch (err: any) {
            logger.errorWithStack(`Error during init session.`, err);
            throw (err);
        }
    }

    private constructor(
      public readonly hydra: Hydra,
      public readonly redis: Redis,
      public readonly user: UserManager,
      public platform: PlatformManager,
      public readonly permission: PermissionManager,
      logger?: SessionLoggerService
    ) {
        this.dynamo = new Dynamo(this.platform.getDynamoDbRegion(), this.platform.getPlatformBaseUrl(), this.platform.getDynamoDbPlatform(), this.platform);
        this.athena = new Athena(this.platform.getAthenaRegion(), this.platform.getAthenaS3Path(), this.platform.getAthenaS3ExportPath(), this.platform.getAthenaSchemaName(), this.platform.getAthenaSchemaNameOverride());
        this.stepFunction = new StepFunction(this.platform.getAthenaRegion());
        this.sqs = new SQS(this.platform.getAthenaRegion());
        this.s3 = new S3(this.platform.getS3Region(), this.platform.getS3Bucket(), this.platform.getAthenaS3Path(), this.platform.getAthenaS3ExportPath(), this.platform.getSnowflakeStorageIntegrationBucket());
        if (this.platform.isDatalakeV3ToggleActive()) {
            this.snowflake = new Snowflake(this, logger ?? httpContext.get('logger'));
        }
    }

    protected async loadDynamoSettings(cache: CacheService, isAnonymous = false, logger?: SessionLoggerService) {
        logger = logger ?? httpContext.get('logger');
        let settings: ReportsSettings;
        if (cache.get(this.platform.getPlatformBaseUrl())) {
            settings = cache.get(this.platform.getPlatformBaseUrl()) as ReportsSettings;
        } else {
            settings = await this.getDynamo().getSettings() as ReportsSettings;

            // Adding additional check to the Datalake V2 toggle in settings to be sure to have it set in the proper way for anonymous calls
            if (!isAnonymous) {
                let haveChanges = false;
                const isV2Active = this.platform.isDatalakeV2Active();
                const isV3Active = this.platform.isDatalakeV3ToggleActive();
                const isHydraMinimalActive = this.platform.isHydraMinimalVersionToggleActive();

                if (isV3Active && !settings.toggleDatalakeV3) {
                    haveChanges = true;
                    settings.toggleDatalakeV3 = true;
                    if (settings.toggleDatalakeV2) {
                        delete settings.toggleDatalakeV2;
                    }
                } else {
                    if (!isV3Active && settings.toggleDatalakeV3) {
                        haveChanges = true;
                        delete settings.toggleDatalakeV3;
                    }

                    if (isV2Active && !settings.toggleDatalakeV2) {
                        haveChanges = true;
                        settings.toggleDatalakeV2 = true;
                    } else if (!isV2Active && settings.toggleDatalakeV2) {
                        haveChanges = true;
                        delete settings.toggleDatalakeV2;
                    }
                }

                if (isHydraMinimalActive && !settings.toggleHydraMinimalVersion) {
                    haveChanges = true;
                    settings.toggleHydraMinimalVersion = true;
                } else if (!isHydraMinimalActive && settings.toggleHydraMinimalVersion) {
                    haveChanges = true;
                    delete settings.toggleHydraMinimalVersion;
                }

                if (haveChanges) {
                    await this.getDynamo().createOrEditSettings(settings);
                }
            }

            cache.set(this.platform.getPlatformBaseUrl(), settings, 60 * 60);
            logger.debug(`Settings for platform ${this.platform.getPlatformBaseUrl()} loaded from DynamoDB`);
        }
        this.platform.loadDynamoSettings(settings);
    }

    public getHydra(): Hydra {
        return this.hydra;
    }

    public getDynamo(): Dynamo {
        return this.dynamo;
    }

    public getAthena(): Athena {
        return this.athena;
    }

    public getSnowflake(): Snowflake {
        return this.snowflake;
    }

    public getS3(): S3 {
        return this.s3;
    }

    public getStepFunction(): StepFunction {
        return this.stepFunction;
    }

    public getSQS(): SQS {
        return this.sqs;
    }
}
