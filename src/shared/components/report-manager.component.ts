import httpContext from 'express-http-context';

import { Athena } from '../../services/athena';
import { SessionLoggerService } from '../../services/logger/session-logger.service';
import { S3 } from '../../services/s3';
import SessionManager from '../../services/session/session-manager.session';

// Reports created by Mangers in My Team Page
export class ReportManagerComponent {
    private readonly athenaService: Athena;
    private readonly s3Service: S3;
    private readonly logger: SessionLoggerService;

    constructor(
        private readonly session: SessionManager,
    ) {
        this.athenaService = this.session.getAthena();
        this.s3Service = this.session.getS3();
        this.logger = httpContext.get('logger');
     }

    public async dropTemporaryTable(temporaryTable: string): Promise<void> {
        try {
            if (temporaryTable && temporaryTable !== '') {
                await this.athenaService.connection.query(`DROP TABLE IF EXISTS ${temporaryTable}`);
                await this.s3Service.deleteTempTableFile(temporaryTable);
            }
        } catch (err: any) {
            const errMessage = 'Error while dropping managers subordinates temporary table on Athena or S3.';
            this.logger.errorWithStack(errMessage, err);
            throw new Error(errMessage);
        }
    }
}