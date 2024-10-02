import { IncomingMessage } from '../message-handler';
import { redisFactory } from '../../services/redis/RedisFactory';
import { ReportSchedulationService } from '../../services/report-schedulation.service';
import { ExtractionComponent } from '../../models/extraction.component';
import { ExtractionModel } from '../../reports/interfaces/extraction.interface';
import PlatformManager from '../../services/session/platform-manager.session';
import { Dynamo } from '../../services/dynamo';
import { ScheduledReportBaseMessageHandler } from './scheduled-report-base-message-handler';
import { ScheduledReportId } from '../../domain/value_objects/ScheduledReportId';
import { DomainException } from '../../domain/exceptions/DomainException';
import { ScheduledReportNotFoundException } from '../../domain/exceptions/ScheduledReportNotFoundException';
import { ReportCannotBeGeneratedException } from '../../domain/exceptions/ReportCannotBeGeneratedException';
import { SidekiqManagerService } from '../../services/sidekiq-manager-service';
import { LoggerInterface } from '../../services/logger/logger-interface';
import { SessionLoggerService } from '../../services/logger/session-logger.service';
import {
	ReportExtractionApplicationServiceFactory
} from '../../domain/factories/ReportExtractionApplicationServiceFactory';
import {
	ReportExtractionApplicationService
} from '../../domain/application_services/ReportExtractionApplicationService';
import { NotFoundException, UnplannedScheduledReportException } from '../../exceptions';
import { Planning } from '../../models/custom-report';

export class NewExtractionMessageHandler extends ScheduledReportBaseMessageHandler {
	public constructor(
		applicationServiceFactory: ReportExtractionApplicationServiceFactory,
		logger: LoggerInterface,
		private readonly sessionLogger: SessionLoggerService,
		private readonly sidekiq: SidekiqManagerService,
	) {
		super(applicationServiceFactory, logger);
	}

	protected checkMessageValidity(message: IncomingMessage): void {
		if (!('platform' in message.payload)) {
			throw new Error(`Message payload does not contain the 'platform' property`);
		}

		if (typeof message.payload.platform !== 'string') {
			throw new Error(`Message payload.platform is expected to be a string, but it is ${typeof message.payload.platform}`);
		}

		if (!('id_report' in message.payload)) {
			throw new Error(`Message payload does not contain the 'id_report' property`);
		}

		if (typeof message.payload.id_report !== 'string') {
			throw new Error(`Message payload.id_report is expected to be a string, but it is ${typeof message.payload.id_report}`);
		}
	}

	private async isDatalakeV3active(platform: string, applicationService: ReportExtractionApplicationService): Promise<boolean> {
		const settings = await applicationService.getSettingsByPlatform(platform);

		return settings?.Details.toggleDatalakeV3;
	}

	private async startExtractionEventBased(message: IncomingMessage, applicationService: ReportExtractionApplicationService): Promise<void> {
		const reportId = message.payload.id_report;
		const platform = message.payload.platform;

		try {
			await applicationService.startExtraction(new ScheduledReportId(reportId, platform));
		} catch (ex: any) {
			this.doLogerrorWithStack(`Failed to start extraction ${JSON.stringify(message.payload)}`, ex);

			if (!(ex instanceof DomainException)) {
				throw ex;
			}

			if (ex instanceof ScheduledReportNotFoundException || ex instanceof ReportCannotBeGeneratedException) {
				this.doLogDebug(`Report "${reportId}" of platform "${platform}" cannot be extracted: ${ex.message}. Trying to delete the sidekiq scheduling.`);
				await this.sidekiq.deleteSidekiqSchedulerItem(reportId, platform);
				this.doLogDebug(`Sidekiq scheduling successfully deleted for report "${reportId}" of platform "${platform}"`);
			} else {
				this.doLogerrorWithStack(`Report "${reportId}" for platform "${platform}" cannot be extracted`, ex);
			}
		}
	}

	protected async doHandleMessage(message: IncomingMessage): Promise<void> {
		const platform = message.payload.platform;
		const idReport = message.payload.id_report;
		this.doLogDebug(`Launch report extraction. Platform: ${platform} - ReportId: ${idReport}`);
		const applicationService = await this.applicationServiceFactory.getReportExtractionApplicationService(message.domain);
		if (await this.isDatalakeV3active(platform, applicationService)) {
			this.doLogInfo(`DatalakeV3 active in Platform ${platform} - Manage extraction by events for report ${idReport}`);
			return this.startExtractionEventBased(message, applicationService);
		}

		this.doLogInfo(`DatalakeV3 not active in Platform ${platform} - Manage extraction in sync for report ${idReport}`);

		const redis = redisFactory.getRedis();
		const redisSettings = await redis.getRedisCommonParams(platform);
		const dynamo = new Dynamo(redisSettings.dynamoDbRegion, platform, '', new PlatformManager(), this.sessionLogger);
		// Return the report's details
		try {
			const report = await dynamo.getReport(idReport);

			// Store the field needed for the extraction model
			const { author, planning: { option: { timezone }}, deleted } = report;
			const { planning } = report;

			if (deleted === true) {
				throw new NotFoundException('Report deleted');
			}
			if (planning && !(planning as Planning).active) {
				throw new UnplannedScheduledReportException('Schedule not active');
			}
			// Check if the report should be extracted
			const extractionModel: ExtractionModel[] = [{idReport: idReport, platform: platform, planning, author}];
			const extractionComponent = new ExtractionComponent();
			const scheduledReport = await extractionComponent.filterScheduledByPeriod(new Date(), extractionModel, redisSettings.schedulationPrivateKey, timezone.toString(), this.sessionLogger);

			// If report scheduled run the extraction
			if (scheduledReport && Object.keys(scheduledReport).length > 0) {
				const schedulingService = new ReportSchedulationService(platform, redisSettings.schedulationPrivateKey, this.sessionLogger);
				await schedulingService.requestScheduledReportsExportByPlatform(scheduledReport[platform]);
			}
		} catch (ex: any) {
			if (ex instanceof NotFoundException || ex instanceof UnplannedScheduledReportException) {
				this.doLogDebug(`Report "${idReport}" of platform "${platform}" cannot be extracted: ${ex.message}. Trying to delete the sidekiq scheduling.`);
				await this.sidekiq.deleteSidekiqSchedulerItem(idReport, platform);
				this.doLogDebug(`Sidekiq scheduling successfully deleted for report "${idReport}" of platform "${platform}"`);
				return;
			}
			throw ex;
		}

	}

}
