import AWS from 'aws-sdk';
import AthenaExpress from 'athena-express';
import { ExportStatuses } from '../models/report-manager';
import httpContext from 'express-http-context';
import { Utils } from '../reports/utils';

export class Athena {
    protected region: string;
    protected db: string;
    protected s3Path: string;
    protected s3ExportPath: string;

    public connection: AthenaExpress;
    public athena: AWS.Athena;

    public MAX_QUERY_CHARACTERS = 20000;
    private logger;

    public constructor(region: string, s3Path: string, s3ExportPath: string, db: string, dbOverride: string) {
        this.region = region;
        this.s3Path = s3Path;
        this.s3ExportPath = s3ExportPath;
        this.db = db;
        if (dbOverride !== '') {
            this.db = dbOverride;
        }

        const awsCredentials = {
            region: this.region
        };
        AWS.config.update(awsCredentials);
        // AWS.config.logger = console; // Uncomment this to have a full log in console for the AWS sdk

        const athenaExpressConfig = {
            aws: AWS,
            s3: this.s3Path,
            db: this.db,
            formatJson: true,
            retry: 1000,
            getStats: false,
            ignoreEmpty: false,
        };

        this.connection = new AthenaExpress(athenaExpressConfig);
        this.athena = new AWS.Athena();
        this.logger = httpContext.get('logger');
    }

    public async runCSVExport(query: string, temporary = false, db?: string): Promise<AWS.Athena.StartQueryExecutionOutput> {
        return new Promise(async (resolve, reject) => {
            let database = this.db;
            if (db) {
                database = db;
            }
            const maxRetries = 12;
            let err;
            let i = 0;
            const params = {
                QueryString: query,
                QueryExecutionContext: {
                    Database: database
                },
                ResultConfiguration: {
                    OutputLocation: temporary ? this.s3Path : this.s3ExportPath
                }
            };

            while (i < maxRetries) {
                try {
                    i++;
                    const data = await this.athena.startQueryExecution(params).promise();
                    resolve(data);
                    return;
                } catch (error: any) {
                    err = error;
                    if (error.hasOwnProperty('code')) {
                        if (error.code === 'ThrottlingException' || error.code === 'Throttling' || error.code === 'TooManyRequestsException') {
                            await Utils.sleep(10000);
                        } else {
                            reject(error);
                        }
                    } else {
                        reject(error);
                    }
                }
            }

            this.logger.errorWithStack(`Error on call 'startQueryExecution' in db: "${this.db}"`, err);
            reject(err);
        });
    }

    public async checkQueryStatus(queryExecutionId: string): Promise<AWS.Athena.GetQueryExecutionOutput> {
        return new Promise((resolve, reject) => {
            const params = {
                QueryExecutionId: queryExecutionId
            };

            this.athena.getQueryExecution(params, (err: AWS.AWSError, data: AWS.Athena.GetQueryExecutionOutput) => {
                if (err) {
                    reject(err);
                } else {
                    resolve(data);
                }
            });
        });
    }

    public async runQuery(query: string, db?: string): Promise<AWS.Athena.GetQueryResultsOutput> {
        return new Promise(async (resolve, reject) => {
            const queryExecutionData = await this.runCSVExport(query, true, db);

            while (true) {
                let status: AWS.Athena.GetQueryExecutionOutput;
                try {
                    status = await this.checkQueryStatus('' + queryExecutionData.QueryExecutionId);
                } catch (error: any) {
                    if (error.hasOwnProperty('code')) {
                        if (error.code === 'ThrottlingException' || error.code === 'Throttling' || error.code === 'TooManyRequestsException' || error.code === 'TooManyRequestsException') {
                            await Utils.sleep(5000);
                            continue;
                        } else {
                            reject(error);
                            break;
                        }
                    } else {
                        reject(error);
                        break;
                    }
                }
                if (status && status.QueryExecution && status.QueryExecution.Status) {
                    if (status.QueryExecution.Status.State === ExportStatuses.QUEUED || status.QueryExecution.Status.State === ExportStatuses.RUNNING) {
                        await Utils.sleep(2000);
                        continue;
                    } else if (status.QueryExecution.Status.State === ExportStatuses.FAILED) {
                        reject(new Error(status.QueryExecution.Status.StateChangeReason));
                        break;
                    } else {
                        while (true) {
                            let data;
                            try {
                                data = await this.getQueryResult(queryExecutionData.QueryExecutionId);
                            } catch (error: any) {
                                await Utils.sleep(5000);
                                continue;
                            }
                        resolve(data);
                        break;
                    }
                        break;
                    }
                }
            }
        });
    }

    /**
     * Return a query results object. If page Size and nextToken are populated is feasible navigate in the results with
     * the paginate
     * @param queryExecutionId
     * @param pageSize
     * @param nextToken
     */
    public async getQueryResult(queryExecutionId: string, pageSize: number|undefined = undefined, nextToken: string|undefined = undefined): Promise<AWS.Athena.Types.GetQueryResultsOutput>  {
        const params: AWS.Athena.Types.GetQueryResultsInput = {
            QueryExecutionId: queryExecutionId,
            MaxResults: pageSize,
            NextToken: nextToken
        };

        return await this.athena.getQueryResults(params).promise();
    }

    /**
     * Get query results as array of object with this format
     * [
     *   {
     *      "columnName1": value_row_1_column_1,
     *      "columnName2": value_row_1_column_2,
     *   },
     *   {
     *      "columnName1": value_row_2_column_1,
     *      "columnName2": value_row_2_column_1,
     *   },
     *   ....
     * ]
     * @param results
     */
    public getQueryResultsAsArray(results: any): [] {
        const resultArray: any = [];
        let resultArrayTmp: any = [];
        const columnInfo = results.ResultSet.ResultSetMetadata.ColumnInfo;

        let i, j: number;

        for (i = 0; i < results.ResultSet.Rows.length; i++) {
            resultArrayTmp = [];
            for (j = 0; j < results.ResultSet.Rows[i].Data.length; j++) {
                let resultValue = results.ResultSet.Rows[i].Data[j].VarCharValue;
                const columnName = columnInfo[j].Name;

                // To prevent that the undefined values are removed from the array result,
                // we associate at the undefined value an empty string (DD-23539)
                if (resultValue === undefined) {
                    resultValue = '';
                }

                if (resultValue !== columnName) {
                    resultArrayTmp = {...resultArrayTmp, ...{[columnName]: resultValue}};
                }
            }

            if (resultArrayTmp.length !== 0) {
                resultArray.push(resultArrayTmp);
            }
        }

        return resultArray;

    }

    public renderStringInQuerySelect(text: string): string {
        if (typeof text !== 'string') {
            throw(new Error('Invalid translation returned'));
        }
        return `"${text.replace(/"/g, '""')}"`;
    }

    public renderStringInQueryCase(text: string): string {
        if (typeof text !== 'string') {
            throw(new Error('Invalid translation returned'));
        }
        return `'${text.replace(/'/g, "''")}'`;
    }
}
