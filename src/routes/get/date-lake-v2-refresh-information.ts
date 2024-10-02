import { Request, Response } from 'express';
import { SessionLoggerService } from '../../services/logger/session-logger.service';
import httpContext from 'express-http-context';
import { BaseReportManagerResponse } from '../../models/report-manager';
import { Dynamo } from '../../services/dynamo';
import SessionManager from '../../services/session/session-manager.session';

export const getDateLakeV2LastCompleteTime = async (req: Request, res: Response) => {
    const logger: SessionLoggerService = httpContext.get('logger');
    const session: SessionManager = res.locals.session;
    const response = new BaseReportManagerResponse();
    response.success = true;

    try {
        const dynamo: Dynamo = session.getDynamo();

        response.data = { complete_time: await dynamo.getDatalakeV2LastCompleteTime()};
    } catch (err: any) {
        logger.errorWithStack(`Internal error on get datalake v2 last complete time.`, err);
        res.status(500);
        response.success = false;
    }

    res.json(response);
};
