import { SessionProvider } from './SessionProvider';
import { BackgroundJobParams, HydraCall } from '../../services/hydra';
import { HTTPService } from '../../services/http/HTTPService';
import { Extraction } from '../entities/Extraction';
import { BackgroundJobCreationFailed } from '../exceptions/BackgroundJobCreationFailed';
import { Method } from 'axios';
import { LoggerInterface } from '../../services/logger/logger-interface';
import { Utils } from '../../reports/utils';

export class BackgroundJobManager {
    public constructor(
        private readonly sessionProvider: SessionProvider,
        private readonly httpService: HTTPService,
        private readonly logger: LoggerInterface,
        private readonly numberOfAttempts = 3,
        private readonly delayBetweenAttemptsInMilliseconds = 200,
    ) {
    }

    private doLogDebug(message: string): void {
        this.logger.debug({ message });
    }

    private doLogError(message: string, error: Error): void {
        this.logger.errorWithException({ message }, error);
    }

    private async performCall(options: HydraCall): Promise<void> {
        const headers = {
            Authorization: options.token
        };
        const reqOptions = {
            method: options.method as Method,
            baseURL: `https://${options.hostname}`,
            url:  (options.subfolder !== '' ? `/${options.subfolder}` : '') + options.path,
            headers,
            data: options.body,
            params: options.params ? options.params : undefined,
        };

        await this.httpService.call(reqOptions);
    }

    private async createBJ(extraction: Extraction): Promise<void> {
        const platform = extraction.Status.platform;
        const hostnameBj = extraction.Status.hostname;
        const subfolder = extraction.Status.subfolder ?? '';
        const idUser = extraction.Status.id_user;
        const userToken = await this.sessionProvider.getTokenForUserId(platform, subfolder, idUser);
        const options = new HydraCall(platform, userToken, subfolder);

        const params = new BackgroundJobParams(
            extraction.Id.ReportId,
            extraction.Id.Id,
            idUser,
            [...extraction.Status.recipients],
            extraction.Status.title,
            hostnameBj ?? platform,
            subfolder
        );

        options.body = JSON.stringify(params);
        options.path = '/manage/v1/job';
        options.method = 'POST';

        await this.performCall(options);
    }

    /**
     * Creates a new BackgroundJob to keep track of the extraction progress.
     *
     * @throws BackgroundJobCreationFailed
     */
    public async createBackgroundJobForExtraction(extraction: Extraction): Promise<void> {
        for (let i = 0; i < this.numberOfAttempts; i++) {
            try {
                await this.createBJ(extraction);
                this.doLogDebug(`BackgroundJob successfully created for extraction ${extraction.Id}`);
                return;
            } catch (error: any) {
                this.doLogError(`Error while trying to create a BackgroundJob for extraction ${extraction.Id}, retrying in ${this.delayBetweenAttemptsInMilliseconds} milliseconds`, error);
                await Utils.sleep(this.delayBetweenAttemptsInMilliseconds);
            }
        }

        const errorMessage = `Failed to create a Background Job for extraction ${extraction.Id} after ${this.numberOfAttempts} attempts`;
        throw new BackgroundJobCreationFailed(errorMessage);
    }
}
