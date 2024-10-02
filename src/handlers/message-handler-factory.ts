import { MessageHandler, NullMessageHandler } from './message-handler';
import { MessageHandlerMapping } from './impl/message-handler-mapping';
import { NewExtractionMessageHandler } from './impl/newExtraction-message-handler';
import { SessionLoggerService } from '../services/logger/session-logger.service';
import { EventName } from './impl/base-message-handler';
import { InitializedMessageHandler } from './impl/initialized-message-handler';
import { CheckQueryStatusMessageHandler } from './impl/check-query-status-message-handler';
import { CheckExportStatusMessageHandler } from './impl/check-export-status-message-handler';
import { QueryCompletedMessageHandler } from './impl/query-completed-message-handler';
import { ExportCompletedMessageHandler } from './impl/export-completed-message-handler';
import { ExportContentCheckedMessageHandler } from './impl/export-content-checked-message-handler';
import { ReportConversionSkippedMessageHandler } from './impl/report-conversion-skipped-message-handler';
import { ReportConvertedMessageHandler } from './impl/report-converted-message-handler';
import { SidekiqManagerService } from '../services/sidekiq-manager-service';
import { loggerFactory } from '../services/logger/logger-factory';
import {
    ReportExtractionApplicationServiceFactory
} from '../domain/factories/ReportExtractionApplicationServiceFactory';

export class MessageHandlerFactory {
    public constructor(private readonly sidekiq: SidekiqManagerService) {
    }

    public getMessageHandler(applicationServiceFactory: ReportExtractionApplicationServiceFactory): MessageHandler {
        const logger = loggerFactory.buildLogger();

        const messageHandlerMapping = new MessageHandlerMapping(
            new NullMessageHandler(logger),
            logger,
        );

        messageHandlerMapping.bind(
            EventName.NewExtraction,
            new NewExtractionMessageHandler(applicationServiceFactory, logger, new SessionLoggerService('NewExtraction'), this.sidekiq),
        );

        messageHandlerMapping.bind(
            EventName.Initialized,
            new InitializedMessageHandler(applicationServiceFactory, logger)
        );

        messageHandlerMapping.bind(
            EventName.CheckQueryStatus,
            new CheckQueryStatusMessageHandler(applicationServiceFactory, logger)
        );

        messageHandlerMapping.bind(
            EventName.QueryCompleted,
            new QueryCompletedMessageHandler(applicationServiceFactory, logger)
        );

        messageHandlerMapping.bind(
            EventName.CheckExportStatus,
            new CheckExportStatusMessageHandler(applicationServiceFactory, logger)
        );

        messageHandlerMapping.bind(
            EventName.ExportCompleted,
            new ExportCompletedMessageHandler(applicationServiceFactory, logger)
        );

        messageHandlerMapping.bind(
            EventName.ExportContentChecked,
            new ExportContentCheckedMessageHandler(applicationServiceFactory, logger)
        );

        messageHandlerMapping.bind(
            EventName.ReportConverted,
            new ReportConvertedMessageHandler(applicationServiceFactory, logger)
        );

        messageHandlerMapping.bind(
            EventName.ReportConversionSkipped,
            new ReportConversionSkippedMessageHandler(applicationServiceFactory, logger)
        );

        return messageHandlerMapping;
    }
}
