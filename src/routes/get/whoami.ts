import { Request, Response, NextFunction } from 'express';
import SessionManager from '../../services/session/session-manager.session';

export const whoami = async(request: Request, response: Response, next: NextFunction) => {
  try {
    const session: SessionManager = response.locals.session;
    const conn = session.getSnowflake();
    const ret = await conn.runQuery(`SELECT "param_name", "param_value" FROM core_setting WHERE "param_name" = 'url'`);

    response.json({
      domain: ret[0].param_value,
    });
  } catch (error: any) {
    console.error(error);
    response.sendStatus(500);
  }
};
