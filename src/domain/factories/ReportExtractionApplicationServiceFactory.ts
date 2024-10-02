import { ReportExtractionApplicationService } from '../application_services/ReportExtractionApplicationService';
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import Config from '../../config';
import { ScheduledReportsRepository } from '../repositories/ScheduledReportsRepository';
import { ExtractionsRepository } from '../repositories/ExtractionsRepository';
import { SessionProvider } from '../domain_services/SessionProvider';
import CacheService from '../../services/cache/cache';
import { SQSClient } from '@aws-sdk/client-sqs';
import { SessionLoggerService } from '../../services/logger/session-logger.service';
import { BackgroundJobManager } from '../domain_services/BackgroundJobManager';
import { HTTPFactory } from '../../services/http/HTTPFactory';
import { Redis } from '../../services/redis/redis';
import { PlatformSettingsRepository } from '../repositories/PlatformSettingsRepository';
import { loggerFactory } from '../../services/logger/logger-factory';
import { ConcreteS3FileSystem } from '../domain_services/S3/ConcreteS3FileSystem';
import { S3Client } from '@aws-sdk/client-s3';
import { S3FileSystemLogger } from '../domain_services/S3/S3FileSystemLogger';

export class ReportExtractionApplicationServiceFactory {
  public constructor(
    private readonly config: Config,
    private readonly redis: Redis,
  ) {}

  public async getReportExtractionApplicationService(domain = ''): Promise<ReportExtractionApplicationService> {
    const region = this.config.getAwsRegion();

    const documentDb = DynamoDBDocumentClient.from(
      new DynamoDBClient({ region })
    );

    const scheduledReportsRepository = new ScheduledReportsRepository(documentDb, this.config.getReportsTableName());
    const extractionsRepository = new ExtractionsRepository(documentDb, this.config.getReportExtractionsTableName());

    const platformSettingsRepository = new PlatformSettingsRepository(
      documentDb,
      this.config.getReportsSettingsTableName(),
      loggerFactory.buildLogger('[PlatformSettingsRepository]', domain),
    );

    const logger = loggerFactory.buildLogger('[ReportExtractionApplicationService]', domain);

    const sessionProvider = new SessionProvider(
      new SessionLoggerService(`ReportExtractionApplicationService`),
      HTTPFactory.getHTTPService(),
      new CacheService(0),
      this.redis,
    );

    const s3FileSystem = new S3FileSystemLogger(
      new ConcreteS3FileSystem(
        new S3Client({ region }),
        'snowflake-exports',
      ),
      loggerFactory.buildLogger('[S3FileSystem]', domain),
    );

    return new ReportExtractionApplicationService(
      platformSettingsRepository,
      scheduledReportsRepository,
      extractionsRepository,
      new SQSClient({ region }),
      this.config.getDatalakeV3MessagingQueueUrl(),
      logger,
      sessionProvider,
      new BackgroundJobManager(sessionProvider, HTTPFactory.getHTTPService(), logger),
      s3FileSystem,
      domain
    );
  }
}

