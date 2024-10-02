import { NextFunction, Request, Response } from 'express';
import httpContext from 'express-http-context';
import { SessionLoggerService } from '../../services/logger/session-logger.service';
import SessionManager from '../../services/session/session-manager.session';
import { BaseResponse } from '../../models/base';
import { ArchivedAuditTrailManager } from '../../archived-audit-trail/manager/archived-audit-trail-manager';
import { BadRequestException } from '../../exceptions/bad-request.exception';
import { redisFactory } from '../../services/redis/RedisFactory';

export const archivedAuditTrailStatus = async (req: Request, res: Response, next: NextFunction) => {
    const logger: SessionLoggerService = httpContext.get('logger');
    const session: SessionManager = res.locals.session;
    const queryExecutionId = req.params.query_execution_id;
    const responseOptions: BaseResponse = {success: true};

    try {
        await ArchivedAuditTrailManager.existsQueryExecutionIdOnRedis(redisFactory.getRedis(), session.user.getIdUser(), queryExecutionId);
        const queryExecutionStatus = await ArchivedAuditTrailManager.getAthenaQueryStatus(session.getAthena(), queryExecutionId);

        responseOptions.data = {queryStatus: queryExecutionStatus};
        res.status(200);
    } catch (e: any) {
        logger.errorWithStack(`Error on get status for QueryExecutionId ${queryExecutionId}`, e);
        responseOptions.success = false;
        if (e instanceof BadRequestException) {
            res.status(400);
            responseOptions.error = e.message;
            responseOptions.errorCode = e.getCode();
        } else {
            responseOptions.error = 'Generic error. See the log';
            res.status(500);
        }
    }

    res.json(responseOptions);
};
