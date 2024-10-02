import { IncomingMessage } from '../message-handler';
import { ScheduledReportBaseMessageHandler } from './scheduled-report-base-message-handler';
import { ExtractionId } from '../../domain/value_objects/ExtractionId';

export class CheckQueryStatusMessageHandler extends ScheduledReportBaseMessageHandler {
	protected async doHandleMessage(message: IncomingMessage): Promise<void> {
		const applicationService = await this.applicationServiceFactory.getReportExtractionApplicationService(message.domain);
		return applicationService.checkQueryStatus(new ExtractionId(this.ExtractionId, this.ReportId));
	}
}
