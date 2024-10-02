import { IncomingMessage } from '../message-handler';
import { ScheduledReportBaseMessageHandler } from './scheduled-report-base-message-handler';
import { ExtractionId } from '../../domain/value_objects/ExtractionId';

export class QueryCompletedMessageHandler extends ScheduledReportBaseMessageHandler {
	protected async doHandleMessage(message: IncomingMessage): Promise<void> {
		const applicationService = await this.applicationServiceFactory.getReportExtractionApplicationService(message.domain);
		return applicationService.exportToCsv(new ExtractionId(message.payload.extraction_id, message.payload.report_id));
	}
}
