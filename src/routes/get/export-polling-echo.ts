import { NextFunction, Request, Response } from 'express';

/**
 * @category Report
 * @summary Report export polling
 * @notes Api called from BackgroundJobExecutor to terminate the file export
 * @method GET
 * @url /analytics/v1/exports/polling/echo
 *
 * @response data [array, required] Data response
 *      @item success [boolean, optional] success
 * @end
 */
export const getExportPollingEcho = (req: Request, res: Response, next: NextFunction) => {
    // static response - do not modify
    const response = {
        data: [
            {
                success: true
            },
        ],
    };

    res.type('application/json');
    res.status(200);
    res.json(response);
};
