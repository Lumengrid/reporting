import { IncomingMessage, MessageHandler, MessageHandlerLogWrapper } from '../message-handler';
import { LoggerInterface } from '../../services/logger/logger-interface';

export class MessageHandlerMapping implements MessageHandler {
    private readonly handlers = new Map<string, MessageHandler>();

    public constructor(
        private readonly fallbackMessageHandler: MessageHandler,
        private readonly logger: LoggerInterface,
    ) {
    }

    public handlerName(): string {
        return 'MessageHandlerMapping';
    }

    public bind(
        messageName: string,
        handler: MessageHandler,
    ): void {
        this.handlers.set(messageName, new MessageHandlerLogWrapper(handler, this.logger));
    }

    private getHandlerForMessage(message: IncomingMessage): MessageHandler {
        return this.handlers.get(message.name) ?? this.fallbackMessageHandler;
    }

    public async handleMessage(message: IncomingMessage): Promise<void> {
        return this.getHandlerForMessage(message)
            .handleMessage(message);
    }
}
