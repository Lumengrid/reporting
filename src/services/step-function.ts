import AWS from 'aws-sdk';
import Config from '../config';
import { SessionLoggerService } from './logger/session-logger.service';
import httpContext from 'express-http-context';
import SessionManager from './session/session-manager.session';
import { LastRefreshDate } from '../reports/interfaces/extraction.interface';
import { STEP_FUNCTION_MAX_ATTEMPTS } from '../shared/constants';
import { DataLakeRefreshStatus } from '../models/base';
import moment from 'moment';

const config = new Config();

export interface StepFunctionParams {
    stateMachineArn: string;
    input?: string;
    name?: string;
    traceHeader?: string;
}

export enum StepFunctionStatuses {
    RUNNING = 'RUNNING',
    SUCCEEDED = 'SUCCEEDED',
    FAILED = 'FAILED',
    TIMED_OUT = 'TIMED_OUT',
    ABORTED = 'ABORTED',
}

export class StepFunction {
    protected region: string;
    protected logger: SessionLoggerService;

    protected stepFunction: AWS.StepFunctions;

    public constructor(region: string) {
        this.region = region;

        const awsCredentials = {
            region: this.region
        };
        AWS.config.update(awsCredentials);
        // AWS.config.logger = console; // Uncomment this to have a full log in console for the AWS sdk
        this.stepFunction = new AWS.StepFunctions();
        this.logger = httpContext.get('logger');
    }

    public async getExecutionStatus(stepFunctionExecutionId: string): Promise<string> {
        const executionInfo = await this.stepFunction.describeExecution({executionArn: stepFunctionExecutionId}).promise();
        return executionInfo.status;
    }
}
