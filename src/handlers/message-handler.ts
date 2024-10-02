import { LoggerInterface } from '../services/logger/logger-interface';

export interface IncomingMessage {
    readonly name: string;
    readonly payload?: {
        readonly [key: string]: any;
    };
    readonly domain?: string;
}

export interface MessageHandler {
    /**
     * Handles a message and can throw any error (not a big description)
     * @param message The incoming message to handle
     */
    handleMessage(message: IncomingMessage): Promise<void>;

    /**
     * The name of the handler that implements this interface (for logging purposes)
     */
    handlerName(): string;
}

export class NullMessageHandler implements MessageHandler {
    public constructor(
        private readonly logger: LoggerInterface,
    ) {
    }

    public handlerName(): string {
        return 'NullMessageHandler';
    }

    public async handleMessage(message: IncomingMessage): Promise<void> {
        this.logger.debug({message: `Nothing to do with message "${message.name}"`});
    }
}

export class MessageHandlerLogWrapper implements MessageHandler {
    public constructor(
        private readonly innerHandler: MessageHandler,
        private readonly logger: LoggerInterface,
    ) {
    }

    private doLog(message: string): void {
        this.logger.debug({message: `[${this.innerHandler.handlerName()}] ${message}`});
    }

    private doLogError(message: string, error: Error): void {
        this.logger.errorWithException({message: `[${this.innerHandler.handlerName()}] ${message}`}, error);
    }

    public handlerName(): string {
        return 'MessageHandlerLogWrapper';
    }

    public async handleMessage(message: IncomingMessage): Promise<void> {
        this.doLog(`Handling message: "${message.name}"`);
        const t0 = Date.now();

        try {
            await this.innerHandler.handleMessage(message);
            const dt = Date.now() - t0;
            this.doLog(`Message "${message.name}" handled properly in ~${dt.toFixed(3)} ms`);
        } catch (error: any) {
            this.doLogError(`Error while handling message "${message.name}`, error);
            throw error;
        }
    }
}
