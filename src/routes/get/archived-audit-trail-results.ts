import { NextFunction, Request, Response } from 'express';
import httpContext from 'express-http-context';
import { SessionLoggerService } from '../../services/logger/session-logger.service';
import SessionManager from '../../services/session/session-manager.session';
import { DEFAULT_PAGE_SIZE } from '../../shared/constants';
import { BasePaginateResultResponse } from '../../models/report-manager';
import { ArchivedAuditTrailManager } from '../../archived-audit-trail/manager/archived-audit-trail-manager';
import { BadRequestException } from '../../exceptions/bad-request.exception';
import { redisFactory } from '../../services/redis/RedisFactory';

export const archivedAuditTrailResults = async (req: Request, res: Response, next: NextFunction) => {
    const logger: SessionLoggerService = httpContext.get('logger');
    const session: SessionManager = res.locals.session;
    const queryExecutionId = req.params.query_execution_id;
    const responseOptions: BasePaginateResultResponse = {success: true};
    const nextToken: string | undefined = req.query.nextToken ? req.query.nextToken.toString() : undefined;
    let pageSize: number | undefined = req.query.pageSize ? parseInt(req.query.pageSize.toString(), 10) : DEFAULT_PAGE_SIZE;
    res.type('application/json');

    // this is a known bug of athena, if there isn't a nextToken the page size must increment 1
    if (typeof nextToken === 'undefined') {
        pageSize = pageSize + 1;
    }

    try {
        const athena = session.getAthena();
        await ArchivedAuditTrailManager.existsQueryExecutionIdOnRedis(redisFactory.getRedis(), session.user.getIdUser(), queryExecutionId);
        await ArchivedAuditTrailManager.isQuerySucceeded(athena, queryExecutionId);

        const results = await athena.getQueryResult(queryExecutionId, pageSize, nextToken);
        responseOptions.data = athena.getQueryResultsAsArray(results);
        responseOptions.nextToken = results.NextToken;
        res.status(200);
    } catch (e: any) {
        logger.errorWithStack(`Error on get results for QueryExecutionId ${queryExecutionId}: ${e.toString()}`, e);
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
