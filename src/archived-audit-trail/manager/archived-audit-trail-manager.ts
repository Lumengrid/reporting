import { OLD_AUDITTRAIL_CONTEXT } from '../../shared/constants';
import { BadRequestException } from '../../exceptions/bad-request.exception';
import { Redis } from '../../services/redis/redis';
import { Athena } from '../../services/athena';
import { ExportStatuses } from '../../models/report-manager';
import { ErrorsCode } from '../../models/base';

export class ArchivedAuditTrailManager {

    /**
     * Check if the queryExecutionId belongs to a current user logged
     * @param redis
     * @param userId
     * @param queryExecutionId
     */
    static async existsQueryExecutionIdOnRedis(redis: Redis, userId: number, queryExecutionId: string): Promise<void> {
        if (!(await redis.existsQueryExecutionIdOnRedis(userId, queryExecutionId, OLD_AUDITTRAIL_CONTEXT))) {
            throw new BadRequestException('QueryExecutionId not valid', ErrorsCode.QueryExecutionIdNotFound);
        }
    }

    /**
     * Get status of queryExecutionId in athena
     * @param athena
     * @param queryExecutionId
     */
    static async getAthenaQueryStatus(athena: Athena, queryExecutionId: string): Promise<string> {
        const status = await athena.checkQueryStatus(queryExecutionId);
        return (status && status.QueryExecution && status.QueryExecution.Status) ? status.QueryExecution.Status.State : '';
    }

    /**
     * Check if query execution status is equal to 'succeeded'
     * @param athena
     * @param queryExecutionId
     */
    static async isQuerySucceeded(athena: Athena, queryExecutionId: string): Promise<void> {
        const queryExecutionStatus = await this.getAthenaQueryStatus(athena, queryExecutionId);
        if (queryExecutionStatus !== ExportStatuses.SUCCEEDED) {
            throw new BadRequestException('QueryExecutionId results is not ready', ErrorsCode.ExtractionAlreadyInExecution);
        }
    }
}
