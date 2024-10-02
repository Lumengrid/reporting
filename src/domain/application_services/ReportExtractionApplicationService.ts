import { ScheduledReportsRepository } from '../repositories/ScheduledReportsRepository';
import { ScheduledReportId } from '../value_objects/ScheduledReportId';
import { ExtractionId } from '../value_objects/ExtractionId';
import { SendMessageCommand, SQSClient, SendMessageCommandOutput } from '@aws-sdk/client-sqs';
import { ExtractionsRepository } from '../repositories/ExtractionsRepository';
import { SessionProvider } from '../domain_services/SessionProvider';
import { Extraction } from '../entities/Extraction';
import { EventName } from '../../handlers/impl/base-message-handler';
import { BackgroundJobManager } from '../domain_services/BackgroundJobManager';
import { BackgroundJobCreationFailed } from '../exceptions/BackgroundJobCreationFailed';
import { DomainException } from '../exceptions/DomainException';
import { InvalidSnowflakeQueryIdException } from '../exceptions/InvalidSnowflakeQueryIdException';
import { PlatformSettingsRepository } from '../repositories/PlatformSettingsRepository';
import { PlatformSettings } from '../entities/PlatformSettings';
import { LoggerInterface } from '../../services/logger/logger-interface';
import { MethodNotImplementedException } from '../../exceptions';
import { S3FileSystem } from '../domain_services/S3/S3FileSystem';
import SessionManager from '../../services/session/session-manager.session';
import { ExportStatuses } from '../../models/report-manager';

interface OutgoingEvent {
    readonly name: EventName;
    readonly payload: Record<string, any>;
}

export class ReportExtractionApplicationService {
    public constructor(
        private readonly platformSettingsRepository: PlatformSettingsRepository,
        private readonly scheduledReportsRepository: ScheduledReportsRepository,
        private readonly extractionsRepository: ExtractionsRepository,
        private readonly sqsClient: SQSClient,
        private readonly messagingQueueUrl: string,
        private readonly logger: LoggerInterface,
        private readonly sessionProvider: SessionProvider,
        private readonly backgroundManager: BackgroundJobManager,
        private readonly fileSystem: S3FileSystem,
        private readonly domain: string,
        private readonly delayCheckQueryInSeconds: number = 10,
    ) {
    }

    private doLogDebug(msg: string): void {
        this.logger.debug({ message: msg });
    }

    private doLogInfo(msg: string): void {
        this.logger.info({ message: msg });
    }

    private doLogError(msg: string): void {
        this.logger.error({ message: msg });
    }

    private doLogErrorWithException(msg: string, exception: Error): void {
        this.logger.error({ message: `${msg} ${exception.stack}` });
    }

    private async generateEvent(
      eventName: EventName,
      extractionId: ExtractionId,
      delaySeconds = 0,
    ): Promise<void> {
        const outgoingEvent: OutgoingEvent = {
            name: eventName,
            payload: {
                extraction_id: extractionId.Id,
                report_id: extractionId.ReportId,
            },
        };
        const messageAttributes = this.domain
            ? {
                MessageAttributes: {
                    Domain: {
                        DataType: 'String',
                        StringValue: this.domain
                    }
                },
            } : {}
        this.doLogDebug(`Generating event "${outgoingEvent.name}" in queue ${this.messagingQueueUrl} for extraction ${extractionId} (delaySeconds=${delaySeconds})`);

        const command = new SendMessageCommand({
            QueueUrl: this.messagingQueueUrl,
            MessageBody: JSON.stringify(outgoingEvent),
            ...messageAttributes,
            DelaySeconds: delaySeconds,
        });
        try {
            const response = await this.sqsClient.send(command) as SendMessageCommandOutput;
            this.doLogDebug(`Event "${outgoingEvent.name}" for extraction ${extractionId} generated in queue ${this.messagingQueueUrl} with MessageId "${response.MessageId}"`);
        } catch (ex: any) {
            this.doLogErrorWithException(`Error generating event "${outgoingEvent.name}" for extraction ${extractionId} in queue ${this.messagingQueueUrl}`, ex);
            throw ex;
        }
    }

    /**
     * @throws ExtractionNotFoundException
     */
    private async retrieveExtraction(extractionId: ExtractionId): Promise<Extraction> {
        this.doLogInfo(`Retrieve extraction from repository ${extractionId}`);

        try {
            return await this.extractionsRepository.getById(extractionId);
        } catch (error: any) {
            this.doLogErrorWithException(`Error retrieving extraction ${extractionId}`, error);
            throw error;
        }
    }

    private async retrieveSession(extraction: Extraction): Promise<SessionManager> {
        this.doLogDebug(`Retrieving session [Platform="${extraction.Status.platform}", Subfolder="${extraction.Status.subfolder}", UserId="${extraction.Status.id_user}", Extraction="${extraction.Id}]`);
        return this.sessionProvider.getSession(extraction.Status.platform, extraction.Status.subfolder ?? '', extraction.Status.id_user);
    }

    public async getSettingsByPlatform(platform: string): Promise<PlatformSettings> {
        this.doLogInfo(`Retrieving platform settings [Platform="${platform}"]`);
        return this.platformSettingsRepository.getByPlatform(platform);
    }

    /**
     * Start a report extraction and generates the relative event on SQS
     *
     * @throws ScheduledReportNotFoundException
     * @throws ReportCannotBeGeneratedException The requested scheduled report has been disabled or cannot be extracted
     * @throws ReportNotScheduledForTodayException The requested scheduled report exists and might be extracted, but it's not scheduled for today
     */
    public async startExtraction(reportId: ScheduledReportId): Promise<ExtractionId> {
        this.doLogDebug(`Starting the extraction of scheduled report ${reportId}`);

        const scheduledReport = await this.scheduledReportsRepository.getById(reportId);
        const extraction = scheduledReport.startExtraction();

        this.doLogInfo(`Persisting extraction ${extraction.Id} for scheduled report ${reportId}`);
        await this.extractionsRepository.add(extraction);
        this.doLogDebug(`Extraction ${extraction.Id} successfully persisted`);

        await this.generateEvent(EventName.Initialized, extraction.Id);

        return extraction.Id;
    }

    /**
     * Starts a query to Snowflake
     *
     * @throws ExtractionNotFoundException
     * @throws InvalidExtractionStatusException
     */
    public async performQuery(extractionId: ExtractionId): Promise<void> {
        this.doLogInfo(`Want to perform query for extraction ${extractionId}`);
        const extraction = await this.retrieveExtraction(extractionId);

        try {
            const sessionManager = await this.retrieveSession(extraction);

            this.doLogDebug(`Performing query for extraction ${extractionId}`);
            const queryId = await extraction.performQuery(sessionManager);
            this.doLogDebug(`Got query id "${queryId}" for extraction ${extractionId}`);

            await this.extractionsRepository.update(extraction);

            this.doLogInfo(`Creating a BackgroundJob for extraction ${extractionId}`);
            await this.backgroundManager.createBackgroundJobForExtraction(extraction);

            await this.generateEvent(EventName.CheckQueryStatus, extractionId, this.delayCheckQueryInSeconds);
        } catch (error: any) {
            this.doLogErrorWithException(`${error.message} for extraction ${extractionId}`, error);

            if (!(error instanceof DomainException) && !(error instanceof MethodNotImplementedException)) {
                throw error;
            }

            if (
                error instanceof BackgroundJobCreationFailed ||
                error instanceof InvalidSnowflakeQueryIdException ||
                error instanceof MethodNotImplementedException
            ) {
                extraction.finalizeWithError(error.message);
                await this.extractionsRepository.update(extraction);
            }
        }
    }

    /**
     * @throws ExtractionNotFoundException
     * @throws InvalidExtractionStatusException
     */
    public async checkQueryStatus(extractionId: ExtractionId): Promise<void> {
        this.doLogDebug(`Want to check query status for extraction ${extractionId}`);

        const extraction = await this.retrieveExtraction(extractionId);

        this.doLogInfo(`Checking query status for extraction ${extractionId}`);
        const queryStatus = await extraction.checkSnowflakeQueryStatus();
        this.doLogDebug(`Query status is: ${JSON.stringify(queryStatus)}`);

        await this.extractionsRepository.update(extraction);
        if (extraction.Status.status === ExportStatuses.FAILED) {
            this.doLogError(`Extraction ${extractionId} set to FAILED - ${extraction.Status.error_details}`);
        }

        let eventName: EventName;

        if (queryStatus.IsRunning) {
            eventName = queryStatus.RunningForTooLong
                ? EventName.QueryFailed
                : EventName.CheckQueryStatus;
        } else {
            eventName = queryStatus.IsError
                ? EventName.QueryFailed
                : EventName.QueryCompleted;
        }

        await this.generateEvent(
          eventName,
          extractionId,
          eventName === EventName.CheckQueryStatus
              ? this.delayCheckQueryInSeconds
              : 0
        );
    }

    /**
     * @throws ExtractionNotFoundException
     * @throws InvalidExtractionStatusException
     */
    public async exportToCsv(extractionId: ExtractionId): Promise<void> {
        this.doLogDebug(`Want to export extraction ${extractionId} to CSV`);

        const extraction = await this.retrieveExtraction(extractionId);

        try {
            this.doLogInfo(`Launching query to export extraction ${extractionId} to CSV`);
            const exportQueryId = await extraction.exportToCsv();
            this.doLogDebug(`Request to export extraction ${extractionId} to CSV returned query id: ${exportQueryId}`);

            await this.extractionsRepository.update(extraction);

            await this.generateEvent(EventName.CheckExportStatus, extractionId, this.delayCheckQueryInSeconds);
        } catch (error: any) {
            this.doLogErrorWithException(error.message, error);

            if (!(error instanceof DomainException)) {
                throw error;
            }

            if (error instanceof InvalidSnowflakeQueryIdException) {
                extraction.finalizeWithError(error.message);
                await this.extractionsRepository.update(extraction);
            }
        }
    }

    /**
     * @throws ExtractionNotFoundException
     * @throws InvalidExtractionStatusException
     */
    public async checkCsvQueryStatus(extractionId: ExtractionId): Promise<void> {
        this.doLogDebug(`Want to check Export CSV query status for extraction ${extractionId}`);

        const extraction = await this.retrieveExtraction(extractionId);

        this.doLogInfo(`Checking Export CSV query status for extraction ${extractionId}`);
        const queryStatus = await extraction.checkCsvExportQueryStatus();
        this.doLogDebug(`Export CSV query status is: ${JSON.stringify(queryStatus)}`);

        await this.extractionsRepository.update(extraction);
        if (extraction.Status.status === ExportStatuses.FAILED) {
            this.doLogError(`Extraction ${extractionId} set to FAILED - ${extraction.Status.error_details}`);
        }

        let eventName: EventName;

        if (queryStatus.IsRunning) {
            eventName = queryStatus.RunningForTooLong
                ? EventName.ExportFailed
                : EventName.CheckExportStatus;
        } else {
            eventName = queryStatus.IsError
                ? EventName.ExportFailed
                : EventName.ExportCompleted;
        }

        await this.generateEvent(
            eventName,
            extractionId,
            eventName === EventName.CheckExportStatus
                ? this.delayCheckQueryInSeconds
                : 0
        );
    }

    /**
     * @throws ExtractionNotFoundException
     * @throws InvalidExtractionStatusException
     */
    public async ensureExportedCSVFileIsNotEmpty(extractionId: ExtractionId): Promise<void> {
        this.doLogDebug(`Want to check the content of the Export CSV for extraction ${extractionId}`);

        const extraction = await this.retrieveExtraction(extractionId);

        try {
            this.doLogInfo(`Checking the content of the exported CSV file for extraction ${extractionId}`);
            await extraction.checkExportedCsvContent(this.fileSystem);
            this.doLogDebug(`Content of exported CSV file for extraction ${extractionId} completed`);

            await this.extractionsRepository.update(extraction);

            await this.generateEvent(EventName.ExportContentChecked, extractionId);
        } catch (error: any) {
            this.doLogErrorWithException(error.message, error);

            if (!(error instanceof DomainException)) {
                throw error;
            }
        }
    }

    /**
     * @throws ExtractionNotFoundException
     */
    public async compress(extractionId: ExtractionId): Promise<void> {
        this.doLogDebug(`Want to compress Export CSV for extraction ${extractionId}`);
        const extraction = await this.retrieveExtraction(extractionId);

        try {
            if (!extraction.Status.enableFileCompression) {
                this.doLogInfo(`Compression not required for extraction ${extractionId}`);
                extraction.finalizeWithSuccess();
                await this.extractionsRepository.update(extraction);

                return;
            }

            this.doLogInfo(`Starting to compress the CSV file for the extraction ${extractionId}`);
            await extraction.compress(this.fileSystem);
            await this.extractionsRepository.update(extraction);
            this.doLogDebug(`Compression of CSV file for the extraction ${extractionId} completed`);
        } catch (error: any) {
            this.doLogErrorWithException(error.message, error);

            if (!(error instanceof DomainException)) {
                throw error;
            }
        }
    }
}

