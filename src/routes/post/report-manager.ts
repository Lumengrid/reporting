import { NextFunction, Request, Response } from 'express';
import httpContext from 'express-http-context';
import { SessionLoggerService } from '../../services/logger/session-logger.service';
import { getDefaultStructureReportManager } from '../../models/report-switcher';
import SessionManager from '../../services/session/session-manager.session';
import { BaseResponse, DataLakeRefreshStatus } from '../../models/base';
import { BadRequestException } from '../../exceptions/bad-request.exception';
import { ReportExtractionInfo } from '../../services/dynamo';
import { ExportStatuses } from '../../models/report-manager';
import { UserAdditionalFieldsComponent } from '../get/user-additional-fields-component';
import { LastRefreshDate } from '../../reports/interfaces/extraction.interface';
import { ExtractionComponent } from '../../models/extraction.component';
import { BaseReportManager } from '../../models/base-report-manager';
import { v4 } from 'uuid';

/**
 * @category Manager
 * @summary Create a manager report
 * @method POST
 * @get report_type_code [string, required] Report type code
 * @url /analytics/v1/manager/report/{report_type_code}
 *
 * @parameter managerTypes [array(integer), optional] Filter out some manager types
 * @parameter enrollmentStatus [array(integer),optional] Filter out the entities by the enrollment status
 * @parameter userAdditionalFieldsFilter [array(object), optional] User additional fields IDs to be used in the search
 * @end
 * @parameter enrollmentDate [object, optional] Object that describes the filter for the enrollment date
 *          @item days [integer, required] Number of days
 *          @item from [string, required] Date from
 *          @item to [string, required] Date to
 *          @item any [boolean, required] Any date
 *          @item type [integer, required] The operator for the comparison, can be isAfter, isBefore, range
 *          @item operator [string, required] Operator
 * @end
 *
 * @response success [boolean, required] Whether or not the operation was successful
 * @response data [object, required]
 *      @item QueryExecutionId [string, required] Id of the query execution id created
 * @end
 * @response error [string, optional] Error Message
 * @response errorCode [integer, optional] Error code
 */
export const reportManager = async (req: Request, res: Response, next: NextFunction) => {
    const logger: SessionLoggerService = httpContext.get('logger');

    const session: SessionManager = res.locals.session;
    const reportTypeCode = req.params.report_type_code;
    const responseOptions: BaseResponse = {success: true};
    const subfolder = req.params.subfolder ? req.params.subfolder.replace(/[^a-zA-Z0-9-]/gi, '') : '';

    let userAddFieldsDropdown = undefined;
    let report: BaseReportManager;

    try {

        // If My Team Additional filters Toggle is ON and the setting "Additional Fields in My Team Page" is enabled
        // retrieve the user addititional fields (only dropdopwn type) that the user can see (if is not Super Admin)
        const isMyTeamUserAddFilter = session.platform.isMyTeamUserAddFilterToggleActive() && session.platform.checkUserAddFieldsFiltersForManager();
        if (isMyTeamUserAddFilter) {
            const userId = session.user.getIdUser();
            const userAdditionalFieldsComponent = new UserAdditionalFieldsComponent(session);
            userAddFieldsDropdown = await userAdditionalFieldsComponent.getUserAdditionalFields(userId);
        }

        const dynamo = session.getDynamo();

        let extraction: ReportExtractionInfo;
        let queryExecutionId: string;

        report = await getDefaultStructureReportManager(session, reportTypeCode, req.body, isMyTeamUserAddFilter, userAddFieldsDropdown);

        if (session.platform.isDatalakeV3ToggleActive()) {
            const query = await report.getQuerySnowflake(session.platform.getCsvExportLimit(), false, false, false);
            const date = new Date();
            const snowflake = session.getSnowflake();

            queryExecutionId = v4();

            extraction = new ReportExtractionInfo(reportTypeCode, queryExecutionId, ExportStatuses.RUNNING, report.convertDateObjectToDatetime(date), 'csv', session.user.getIdUser(), false);
            extraction.snowflakeRequestSort = report.querySorting;
            extraction.convertSnowflakeRequestSelectedColumns(report.querySelect);
            extraction.extraction_id = queryExecutionId;
            extraction.snowflakeRequestID = await snowflake.runQuery(query, false, true);
        } else {
            const query = await report.getQuery(session.platform.getCsvExportLimit(), false, false);

            const athena = session.getAthena();
            const extractionComponent = new ExtractionComponent();

            let refreshInfo: LastRefreshDate;

            try {
                refreshInfo = await extractionComponent.getDataLakeLastRefresh(session);
            } catch (e: any) {
                // nothing to do for now
            }

            const date = new Date();

            refreshInfo = await extractionComponent.startDataLakeRefreshIfStatusIsError(session, refreshInfo);

            let refreshError = false;

            if (session.platform.isDatalakeV2Active() && (refreshInfo.refreshStatus === DataLakeRefreshStatus.RefreshInProgress || refreshInfo.isRefreshNeeded)) {
                // In this case we will save the export to run it again when the refresh will end and prepare the background job if required
                if (refreshInfo.refreshStatus !== DataLakeRefreshStatus.RefreshInProgress && refreshInfo.isRefreshNeeded) {
                    const sqs = session.getSQS();
                    try {
                        await sqs.runDataLakeV2Refresh(session, refreshInfo);
                    } catch (e: any) {
                        refreshError = true;
                    }
                }

                if (!refreshError) {
                    queryExecutionId = v4();

                    extraction = new ReportExtractionInfo(reportTypeCode, queryExecutionId, ExportStatuses.RUNNING, report.convertDateObjectToDatetime(date), 'csv', session.user.getIdUser(), false);
                    extraction.queuedExtractionID = queryExecutionId;
                    extraction.query = query;
                } else {
                    const data = await athena.runCSVExport(query);
                    queryExecutionId = data.QueryExecutionId;

                    extraction = new ReportExtractionInfo(reportTypeCode, queryExecutionId, ExportStatuses.RUNNING, report.convertDateObjectToDatetime(date), 'csv', session.user.getIdUser(), false);
                }
            } else {
                const data = await athena.runCSVExport(query);
                queryExecutionId = data.QueryExecutionId;

                extraction = new ReportExtractionInfo(reportTypeCode, queryExecutionId, ExportStatuses.RUNNING, report.convertDateObjectToDatetime(date), 'csv', session.user.getIdUser(), false);
            }

            extraction.managerSubordinatesTable = report.managerSubordinatesTable;
        }

        extraction.hostname = req.hostname;
        extraction.subfolder = subfolder;

        await dynamo.createOrEditReportExtraction(extraction);
        res.status(200);
        responseOptions.data = {QueryExecutionId: queryExecutionId};
    } catch (e: any) {
        responseOptions.success = false;

        if (report) {
            report.dropTemporaryTables();
        }

        if (e instanceof BadRequestException) {
            responseOptions.error = e.message;
            responseOptions.errorCode = e.getCode();
            res.status(400);
        } else {
            logger.errorWithStack('Error on create a manager report', e);
            responseOptions.error = 'Generic Error. See the log';
            res.status(500);
        }
    }

    res.json(responseOptions);
};
