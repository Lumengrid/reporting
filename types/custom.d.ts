import { Pool } from 'generic-pool';
import { DBConnection } from '../src/services/snowflake/interfaces/snowflake.interface';

declare global {
    namespace NodeJS {
        interface Global {
            snowflakePool: Pool<DBConnection> | null;
        }
    }
}
