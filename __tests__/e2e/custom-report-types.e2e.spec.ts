import { E2ETestUtils } from './utils';
import { AxiosResponse } from 'axios';
import { ErrorCode } from '../../src/exceptions';

jest.setTimeout(50000);

describe('Custom Report Types Service', () => {

    let customReportTypeId = '';
    let adminToken: string;
    let customReportId = '';
    let queryExecutionId = '';
    let isToggleDatalakeV2 = false;

    beforeAll(async () => {
        E2ETestUtils.loadTestEnv();
        if (!adminToken) {
            adminToken = await E2ETestUtils.loginAsGodAdmin();
        }
        isToggleDatalakeV2 = await E2ETestUtils.checkToggleDatalakeV2(adminToken);
    });

    afterAll(async () => {
        // If exists, delete Custom Report Type at the end
        await cleanCustomReportType();
    })

    const createCustomReportType = async () => {
        try {
           const customReportType = await E2ETestUtils.createCustomReportTypes(
                {
                    name: 'Functional custom report types test',
                    description: 'This custom report type is created automatically by a functional test! You should never see this!'
                },
                adminToken
            );
            customReportTypeId = customReportType.data.data.idCustomReportTypes;
            return customReportType;

        } catch (error) {
            console.log(' **** Cannot create Custom Report Type **** ');
        }
    }

    const editCustomReportTypesFunction = async (data: any) => {
        // This is necessary if you want test only one test closure
        let deleteCustomReportTypes = false;
        if (!customReportTypeId) {
            const response = await E2ETestUtils.createCustomReportTypes(
                {
                    name: 'Functional custom report types test',
                    description: 'This custom report type is created automatically by a functional test! You should never see this!'
                },
                adminToken
            );
            customReportTypeId = response.data.data.idCustomReportTypes;
            deleteCustomReportTypes = true;
        }

        try {
            return await E2ETestUtils.editCustomReportTypes(
                customReportTypeId,
                adminToken,
                data
            );
        } catch (error) {
            if (deleteCustomReportTypes) {
                await E2ETestUtils.deleteCustomReportTypes(
                    customReportTypeId,
                    adminToken,
                );
                customReportTypeId = '';
            }
            throw error;
        }

    };

    const cleanCustomReportType = async () => {
        if (!customReportTypeId) return;
        try {
            await E2ETestUtils.deleteCustomReportTypes(
                customReportTypeId,
                adminToken,
            );
        } catch (error) {
            console.log(' ++++ Cannot delete Custom Report Type ++++ ');
        }
    };


    it('Should be able, as admin, to create custom report type', async () => {
        const response = await createCustomReportType();

        expect(response?.data).toBeDefined();
        expect(response?.data.success).toEqual(true);
        expect(response?.data.data.idCustomReportTypes).toBeDefined();
        customReportTypeId = response?.data.data.idCustomReportTypes;

    });

    it('Should be able to save custom report type inactive with sql empty', async () => {
        const response: AxiosResponse = await editCustomReportTypesFunction({sql: '', status: 0});

        expect(response.data).toBeDefined();
        expect(response.data.success).toEqual(true);

    });

    it('Shouldn\'t be able to save custom report type active with sql empty', async () => {
        let error = {} as any;
        try {
            await editCustomReportTypesFunction({sql: '', status: 1});
        } catch (exception) {
            error = exception;
        }

        expect(error.response.status).toEqual(400);
        expect(error.response.data.success).toEqual(false);
        expect(error.response.data.error).toEqual('Provide a valid SQL');
        expect(error.response.data.errorCode).toEqual(ErrorCode.WRONG_SQL);

    });

    it('Shouldn\'t be able to save custom report type active with a query as select *', async () => {
        const data: object = {sql: 'select * from pippo', status: 1};
        let error = {} as any;
        try {
            await editCustomReportTypesFunction(data);
        } catch (exception) {
            error = exception;
        }

        expect(error.response.status).toEqual(400);
        expect(error.response.data.success).toEqual(false);
        expect(error.response.data.error).toEqual('Provide a valid SQL');
        expect(error.response.data.errorCode).toEqual(ErrorCode.WRONG_SQL);

    });

    it('Shouldn\'t be able to save custom report type active with a query as select * (2)', async () => {
        const data: object = {sql: 'select attr, * from pippo', status: 1};
        let error = {} as any;

        try {
            await editCustomReportTypesFunction(data);
        } catch (exception) {
            error = exception;
        }

        expect(error.response.status).toEqual(400);
        expect(error.response.data.success).toEqual(false);
        expect(error.response.data.error).toEqual('Provide a valid SQL');
        expect(error.response.data.errorCode).toEqual(ErrorCode.WRONG_SQL);

    });

    it('Shouldn\'t be able to save custom report type active with a query as select * (3)', async () => {
        const data: object = {sql: 'select attr, (select * from pluto) from pippo', status: 1};
        let error = {} as any;

        try {
            await editCustomReportTypesFunction(data);
        } catch (exception) {
            error = exception;
        }

        expect(error.response.status).toEqual(400);
        expect(error.response.data.success).toEqual(false);
        expect(error.response.data.error).toEqual('Provide a valid SQL');
        expect(error.response.data.errorCode).toEqual(ErrorCode.WRONG_SQL);

    });

    it('Should be able to save custom report type active with a query with * char as wildcard, or math operator',
        async () => {
            let sql = `SELECT cu."idst", cu2."idst" as idst1,
                    CASE
                        WHEN regexp_like(cu."userid", '/.*v.*/') THEN 'Has v'
                        ELSE 'other'
                    END as user_category
                FROM core_user cu
                LEFT JOIN core_user cu2 ON cu."idst" * 1 = cu2."idst"
                WHERE cu."idst" * 1 = cu."idst"`;

            if (isToggleDatalakeV2) {
                sql = `select idst,
                    (select idst from core_user where idst*1 =cu.idst) as idst1,
                    CASE WHEN regexp_like(userid, '/.*v.*') THEN 'Has v' ELSE 'other' END
                    from core_user cu
                    where idst * 1 = idst`;
            }

            let data = {
                sql,
                status: 1
            };

        const response: AxiosResponse = await editCustomReportTypesFunction(data);

        expect(response.data).toBeDefined();
        expect(response.data.success).toEqual(true);
    });

    it('Should be able to save custom report type active with a query with * count(*)',
        async () => {
            const data: object = {sql: 'select count(*) from core_user', status: 1};
            const response: AxiosResponse = await editCustomReportTypesFunction(data);

            expect(response.data).toBeDefined();
            expect(response.data.success).toEqual(true);

        });

    it('Should be able to save custom report type active with a valid sql', async () => {
        let sql = 'select "idst" from core_user limit 1';

        if(isToggleDatalakeV2){
            sql = 'select idst from core_user limit 1'
        }

        const data: object = {sql, status: 1};

        const response: AxiosResponse = await editCustomReportTypesFunction(data);

        expect(response.data).toBeDefined();
        expect(response.data.success).toEqual(true);
    });

    it('Shouldn\'t be able to save custom report type active with a valid sql but empty json area', async () => {
        let sql = 'select "idst" from core_user limit 1 where {filter1}';

        if(isToggleDatalakeV2){
            sql = 'select idst from core_user limit 1 where {filter1}'
        }

        const data: object = {sql, status: 1, json: ''};
        let error: any = {};

        try {
            await editCustomReportTypesFunction(data);
        } catch (exception) {
            error = exception;
        }

        expect(error.response.status).toEqual(400);
        expect(error.response.data.success).toEqual(false);
        expect(error.response.data.error).toEqual('Provide a valid JSON');
        expect(error.response.data.errorCode).toEqual(ErrorCode.JSON_AREA_EMPTY);

    });

    it('Shouldn\'t be able to save custom report type active a valid sql but invalid json structure', async () => {
        let sql = 'select "idst" from core_user limit 1 where {filter1} or {filter2}';

        if(isToggleDatalakeV2){
            sql = 'select idst from core_user limit 1 where {filter1} or {filter2}'
        }

        const data: object = {sql, json: '{aaa:}', status: 1};
        let error: any = {};
        try {
            await editCustomReportTypesFunction(data);
        } catch (exception) {
            error = exception;
        }

        expect(error.response.status).toEqual(400);
        expect(error.response.data.success).toEqual(false);
        expect(error.response.data.error).toEqual('Provide a valid JSON');
        expect(error.response.data.errorCode).toEqual(ErrorCode.WRONG_JSON);

    });

    it('Shouldn\'t be able to save custom report type active with a more filter in sql area then json area', async () => {
        let sql = 'select "idst" from core_user limit 1 where {filter1} or {filter2}';

        if(isToggleDatalakeV2){
            sql = 'select idst from core_user limit 1 where {filter1} or {filter2}'
        }

        const data: object = {
            sql,
            json: '{"filter1": {"filed":"field", "type": "type"}}',
            status: 1
        };
        let error: any = {};

        try {
            await editCustomReportTypesFunction(data);
        } catch (exception) {
            error = exception;
        }

        expect(error.response.status).toEqual(400);
        expect(error.response.data.success).toEqual(false);
        expect(error.response.data.error).toEqual('Provide a valid JSON');
        expect(error.response.data.errorCode).toEqual(ErrorCode.FILTER_NOT_FOUND_IN_JSON);

    });

    it('Shouldn\'t be able to save custom report type active with a more filter in json area then sql area', async () => {
        let sql = 'select "idst" from core_user limit 1 where {filter1}';

        if(isToggleDatalakeV2){
            sql = 'select idst from core_user limit 1 where {filter1}'
        }

        const data: object = {
            sql,
            json: '{"filter1": {"filed":"field", "type": "type"}, "filter2": {"filed":"field", "type": "type"}}',
            status: 1
        };
        let error: any = {};

        try {
            await editCustomReportTypesFunction(data);
        } catch (exception) {
            error = exception;
        }

        expect(error.response.status).toEqual(400);
        expect(error.response.data.success).toEqual(false);
        expect(error.response.data.error).toEqual('Provide a valid JSON');
        expect(error.response.data.errorCode).toEqual(ErrorCode.MORE_FILTER_IN_JSON);

    });

    it('Shouldn\'t be able to save custom report type active with valid sql area and a invalid json (missing field)', async () => {
        let sql = 'select "idst" from core_user limit 1 where {filter1}';

        if(isToggleDatalakeV2){
            sql = 'select idst from core_user limit 1 where {filter1}'
        }

        const data: object = {
            sql,
            json: '{"filter1": {"type": "type"}}',
            status: 1
        };
        let error: any = {};

        try {
            await editCustomReportTypesFunction(data);
        } catch (exception) {
            error = exception;
        }

        expect(error.response.status).toEqual(400);
        expect(error.response.data.success).toEqual(false);
        expect(error.response.data.error).toEqual('Provide a valid JSON');
        expect(error.response.data.errorCode).toEqual(ErrorCode.MISSING_FIELD_IN_JSON_FILTER);

    });

    it('Shouldn\'t be able to save custom report type active with valid sql area and a invalid json (missing type)', async () => {
        let sql = 'select "idst" from core_user limit 1 where {filter1}';

        if(isToggleDatalakeV2){
            sql = 'select idst from core_user limit 1 where {filter1}'
        }

        const data: object = {
            sql,
            json: '{"filter1": {"field": "field"}}',
            status: 1
        };
        let error: any = {};

        try {
            await editCustomReportTypesFunction(data);
        } catch (exception) {
            error = exception;
        }

        expect(error.response.status).toEqual(400);
        expect(error.response.data.success).toEqual(false);
        expect(error.response.data.error).toEqual('Provide a valid JSON');
        expect(error.response.data.errorCode).toEqual(ErrorCode.MISSING_TYPE_IN_JSON_FILTER);

    });

    it('Shouldn\'t be able to save custom report type active with valid sql area and a invalid json (missing description)', async () => {
        let sql = 'select "idst" from core_user limit 1 where {filter1}';

        if(isToggleDatalakeV2){
            sql = 'select idst from core_user limit 1 where {filter1}'
        }

        const data: object = {
            sql,
            json: '{"filter1": {"field": "lastenter","type": "date"}}',
            status: 1
        };
        let error: any = {};

        try {
            await editCustomReportTypesFunction(data);
        } catch (exception) {
            error = exception;
        }

        expect(error.response.status).toEqual(400);
        expect(error.response.data.success).toEqual(false);
        expect(error.response.data.error).toEqual('Provide a valid JSON');
        expect(error.response.data.errorCode).toEqual(ErrorCode.MISSING_DESCRIPTION_IN_JSON_FILTER);

    });

    it('Shouldn\'t be able to save custom report type active with a valid sql without filter but with json area filled', async () => {
        let sql = 'select "idst" from core_user';

        if(isToggleDatalakeV2){
            sql = 'select idst from core_user';
        }

        const data: object = {
            sql,
            json: '{"filter1": {"field": "field"}}',
            status: 1
        };

        let error: any = {};

        try {
            await editCustomReportTypesFunction(data);
        } catch (exception) {
            error = exception;
        }

        expect(error.response.status).toEqual(400);
        expect(error.response.data.success).toEqual(false);
        expect(error.response.data.error).toEqual('Provide a valid JSON');
        expect(error.response.data.errorCode).toEqual(ErrorCode.JSON_AREA_FILLED);

    });


    it('Shouldn\'t be able to save custom report type active with a valid sql and json area with a not allowed type', async () => {
        let sql = 'select "idst" from core_user where {filter1}';

        if(isToggleDatalakeV2){
            sql = 'select idst from core_user where {filter1}';
        }

        const data: object = {
            sql,
            json: '{"filter1": {"field": "field", "type": "notAllowed"}}',
            status: 1
        };
        let error = {} as any;

        try {
            await editCustomReportTypesFunction(data);
        } catch (exception) {
            error = exception;
        }

        expect(error.response.status).toEqual(400);
        expect(error.response.data.success).toEqual(false);
        expect(error.response.data.error).toEqual('Provide a valid JSON');
        expect(error.response.data.errorCode).toEqual(ErrorCode.WRONG_TYPE_IN_JSON_FILTER);

    });

    it('Should be able to receive queryExecutionId with a valid query and valid filter', async () => {
        let sql = 'select "idst" from core_user where {filter1} limit 1';

        if(isToggleDatalakeV2){
            sql = 'select idst from core_user where {filter1} limit 1';
        }

        const json = '{"filter1": {"field": "lastenter", "type": "date", "description": "Mandatory"}}';

        const data = {sql: `${sql}`, json: `${json}`};
        const response: AxiosResponse = await editCustomReportTypesFunction(data);

        expect(response.data).toBeDefined();
        expect(response.data.success).toEqual(true);

    });

    it('Should be able to receive queryExecutionId with a valid query', async () => {
        // If you want to launch only this test, need to create the custom report type
        if (!customReportTypeId) {
            await createCustomReportType();
        }

        let sql = 'select "idst" from core_user limit 1';

        if(isToggleDatalakeV2){
            sql = 'select idst from core_user limit 1';
        }

        const response: AxiosResponse = await E2ETestUtils.customReportTypesPreview(
            customReportTypeId,
            adminToken,
            sql
        );

        expect(response.data).toBeDefined();
        expect(response.data.success).toEqual(true);
        expect(response.data.data.QueryExecutionId).toBeDefined();

        queryExecutionId = response.data.data.QueryExecutionId;

    });


    it('Shouldn\'t be able to receive queryExecutionId with a invalid query', async () => {
        if (!customReportTypeId) {
            await createCustomReportType();
        }

        const sql = '';
        let error = {} as any;

        try {
            await E2ETestUtils.customReportTypesPreview(
                customReportTypeId,
                adminToken,
                sql
            );
        } catch (exception) {
            error = exception;
        }

        expect(error.response.status).toEqual(400);
        expect(error.response.data.success).toEqual(false);
        expect(error.response.data.error).toEqual('Provide a valid SQL');
        expect(error.response.data.errorCode).toEqual(ErrorCode.WRONG_SQL);

    });

    it('Should be able to see query preview with a valid queryExecutionId', async () => {
        // If launch only this test, create a custom report type, run preview and store QueryExecutionId
        let queryExecutionIdSingleTest = ''
        if (!customReportTypeId) {
            let sql = 'select "idst" from core_user limit 1';
            if(isToggleDatalakeV2){
                sql = 'select idst from core_user limit 1';
            }
            await createCustomReportType();

            const response: AxiosResponse = await E2ETestUtils.customReportTypesPreview(
                customReportTypeId,
                adminToken,
                sql
            );
            queryExecutionIdSingleTest = response.data.data.QueryExecutionId;
        }

        const response: AxiosResponse = await E2ETestUtils.customReportTypesResults(
            customReportTypeId,
            adminToken,
            (queryExecutionId || queryExecutionIdSingleTest)
        );

        expect(response.data).toBeDefined();
        expect(response.data.success).toEqual(true);
        expect(response.data.data.queryStatus).toEqual('SUCCEEDED');
        expect(response.data.data.result).toBeDefined();

    });


    it('Shouldn\'t be able to see query preview with an invalid queryExecutionId', async () => {
        if (!customReportTypeId) {
            let sql = 'select "idst" from core_user limit 1';
            if(isToggleDatalakeV2){
                sql = 'select idst from core_user limit 1';
            }
            await createCustomReportType();
        }

        let error = {} as any;
        const invalidQueryExecutionId = '1234-5678-abcd';

        try {
            await E2ETestUtils.customReportTypesResults(
                customReportTypeId,
                adminToken,
                invalidQueryExecutionId
            );
        } catch (exception) {
            error = exception;
        }

        expect(error.response.status).toEqual(404);
        expect(error.response.data.success).toEqual(false);
        expect(error.response.data.error).toEqual('QueryExecutionId doesn\'t exist or not is associated with this customReportId');

    });



    it('Should be able to create a report with a valid query Builder Id', async () => {
        const response = await E2ETestUtils.createQueryBuilderReport(customReportTypeId, adminToken);

        expect(response.data).toBeDefined();
        expect(response.status).toBe(200);
        expect(response.data.data.idReport).toBeDefined();

        customReportId = response.data.data.idReport;

    });

    it('Shouldn\'t be able to create a report with a invalid query Builder Id', async () => {
        let error = {} as any;

        try {
            await E2ETestUtils.createQueryBuilderReport('test-123', adminToken);
        } catch (exception) {
            error = exception;
        }

        expect(error.response.data).toBeDefined();
        expect(error.response.status).toBe(500);
        expect(error.response.data.success).toEqual(false);
        expect(error.response.data.error).toEqual('Generic error. See the logs for more information');

    });

    it('Shouldn\'t be able to create a report with a query Builder Id inactive', async () => {
        let inactiveCustomReportTypeId = '';
        let error: any = {};

        try {
            const response = await E2ETestUtils.createCustomReportTypes(
                {
                    name: 'Functional custom report types test',
                    description: 'This custom report type is created automatically by a functional test! You should never see this!'
                },
                adminToken
            );
            inactiveCustomReportTypeId = response.data.data.idCustomReportTypes;

            await E2ETestUtils.createQueryBuilderReport(inactiveCustomReportTypeId, adminToken);
        } catch (exception) {
            error = exception;
        }

        expect(error.response.data).toBeDefined();
        expect(error.response.status).toBe(400);
        expect(error.response.data.success).toEqual(false);
        expect(error.response.data.error).toEqual('Query builder selected is not active');

        await E2ETestUtils.deleteCustomReportTypes(inactiveCustomReportTypeId, adminToken);

    });

    it('Should be able to see related reports in query builder detail', async () => {
        const response: AxiosResponse = await E2ETestUtils.getCustomReportTypes(customReportTypeId, adminToken);

        expect(response.data).toBeDefined();
        expect(response.status).toBe(200);
        expect(response.data.data.relatedReports).toBeDefined();
        expect(response.data.data.relatedReports.length).toEqual(1);

    });

    it('Shouldn\'t be able to invalidate a query builder related to custom report', async () => {
        let error: any = {};
        let sql = 'select "idst" from core_user limit 1';

        if (isToggleDatalakeV2) {
            sql = 'select idst from core_user limit 1';
        }

        try {
            const data: object = {sql, json: '', status: 0};

            await editCustomReportTypesFunction(data);
        } catch (exception) {
            error = exception;
        }

        expect(error.response.status).toEqual(400);
        expect(error.response.data.success).toEqual(false);
        expect(error.response.data.data.length).toEqual(1);
        expect(error.response.data.errorCode).toEqual(ErrorCode.QUERY_BUILDER_RELATED_REPORT);

    });

    it('Should be able to retrieve all reports associate with a valid query Builder Id', async () => {
        const response = await E2ETestUtils.getAllReportsByQueryBuilderId(customReportTypeId, adminToken);

        expect(response.data).toBeDefined();
        expect(response.status).toBe(200);
        expect(response.data.data.length).toEqual(1);
        expect(response.data.data[0].idReport).toEqual(customReportId);

        await E2ETestUtils.deleteReport(customReportId, adminToken);

    });

    it('Should be able to deleted a valid custom-report-type', async () => {
        const response = await E2ETestUtils.deleteCustomReportTypes(customReportTypeId, adminToken);
        customReportTypeId = '';

        expect(response.data).toBeDefined();
        expect(response.status).toBe(200);
        expect(response.data.success).toEqual(true);

    });


    it('Shouldn\'t be able to deleted a invalid custom-report-type id', async () => {
        let error: any = {};

        try {
            await E2ETestUtils.deleteCustomReportTypes('123-test', adminToken);
        } catch (exception) {
            error = exception;
        }

        expect(error.response.data).toBeDefined();
        expect(error.response.status).toBe(404);
        expect(error.response.data.success).toEqual(false);
        expect(error.response.data.errorCode).toEqual(ErrorCode.REPORT_NOT_FOUND);

    });
});
