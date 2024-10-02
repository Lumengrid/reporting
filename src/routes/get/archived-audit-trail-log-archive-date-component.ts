import SessionManager from '../../services/session/session-manager.session';
import { redisFactory } from '../../services/redis/RedisFactory';

export class ArchivedAuditTrailArchiveDateComponent {
    private session: SessionManager;

    constructor (session: SessionManager) {
        this.session = session;
    }

    async getArchiveDate(): Promise<string> {
        const redis = redisFactory.getRedis();
        const legacyAuditTrailDBName = await redis.getLegacyAuditTrailLogsDBName();
        const legacyAuditTrailLogsTableName = await redis.getLegacyAuditTrailLogsTableName();

        const sql = `SELECT DATE_FORMAT(MAX(timestamp), '%Y-%m-%d %H:%i:%s')
            FROM ${legacyAuditTrailLogsTableName}
            WHERE platform = '${this.session.platform.getPlatformBaseUrlPath()}'`;

        const athena = this.session.getAthena();
        const data = await athena.runQuery(sql, legacyAuditTrailDBName);

        return data.ResultSet.Rows[1]?.Data[0]?.VarCharValue;
    }
}
