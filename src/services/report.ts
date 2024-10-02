import { Report, TimeFrameOptions } from '../models/custom-report';
import Hydra, { UsersResponse } from './hydra';
import { ReportManagerInfo } from '../models/report-manager';
import { LegacyReportsResponse, MigrateInputPayload } from '../models/migration-component';
import { SessionLoggerService } from './logger/session-logger.service';
import httpContext from 'express-http-context';

type AuthorName = {
    firstName: string,
    lastName: string,
    userId: string,
};

export class ReportService {
    private logger: SessionLoggerService;

    constructor(
        private readonly hydraService: Hydra
    ) {
        this.logger = httpContext.get('logger');
    }

    public convertDynamoReportMetadataToAamonReportMetadata(dynamoReport: ReportManagerInfo): Report {
        const aamonReport: Report = new Report();

        aamonReport.idReport = dynamoReport.idReport;
        aamonReport.name = dynamoReport.title;
        aamonReport.description = dynamoReport.description;
        aamonReport.standard = dynamoReport.standard;
        aamonReport.type = dynamoReport.type;
        aamonReport.createdBy = dynamoReport.author !== 0 ? dynamoReport.author : '';
        aamonReport.creationDate = dynamoReport.creationDate;
        aamonReport.visibility = dynamoReport.visibility.type;

        const planning = dynamoReport.planning;
        aamonReport.planning = {
            active: (planning && planning.active === true) ? true : false,
            option: {
                every: (planning && planning.option && planning.option.every) ? planning.option.every : 1,
                recipients: (planning && planning.option && planning.option.recipients) ? planning.option.recipients : [],
                timeFrame: (planning && planning.option && planning.option.timeFrame) ? planning.option.timeFrame : TimeFrameOptions.days,
                isPaused: (planning && planning.option && planning.option.isPaused) ? true : false,
                scheduleFrom: (planning && planning.option && planning.option.scheduleFrom) ? planning.option.scheduleFrom : '',
                startHour: (planning && planning.option && planning.option.startHour) ? planning.option.startHour : '',
                timezone: (planning && planning.option && planning.option.timezone) ? planning.option.timezone : '',
            }
        };

        return aamonReport;
    }


    public async convertDynamoReportsMetadataToAamonReportsMetadata(dynamoReports: ReportManagerInfo[]): Promise<Report[]> {
        // List of enriched reports
        const aamonReports: Report[] = [];

        // List of authors enriched objects
        const aamonReportsAuthors: Map<number, AuthorName> =
            new Map<number, AuthorName>();

        for (const dynamoReport of dynamoReports) {
            const aamonReport: Report = this.convertDynamoReportMetadataToAamonReportMetadata(dynamoReport);

            aamonReports.push(aamonReport);
            aamonReportsAuthors.set(aamonReport.createdBy as number, { firstName: '', lastName: '', userId: '' });
        }

        // pass an array of users id and return users detail info
        const usersData = await this.getReportsDataFromHydra([...aamonReportsAuthors.keys()]);

        // For each userId match the corresponding firstname and lastname
        for (const author in usersData.data) {
            aamonReportsAuthors.set(parseInt(author, 10), {
                firstName: usersData.data[author].firstname,
                lastName: usersData.data[author].lastname,
                userId: usersData.data[author].userid,
            });
        }

        // Add the createdByDescription field to the final report
        this.addAuthorNameToTheReport(aamonReports, aamonReportsAuthors);

        return aamonReports;
    }

    /**
     * Updates the learning object types of a report with the list in input
     *
     * @param allLoTypes All the Learning Objects of the LMS
     * @param loTypes The learning object saved in the report
     *
     * @return { [p: string]: boolean } A union of loTypes with allLoTypes, new values are set
     *     to true if all selections in loTypes are true, false otherwise.
     */
    refreshSelectedLoTypes(allLoTypes: string[], loTypes: { [p: string]: boolean })
    : { [p: string]: boolean } {
        // get the default value for the new types - if they are all Selected it's true, otherwise false
        const areAllSelected = Object.values(loTypes).every(selected => selected === true);

        for (const type of allLoTypes) {
            // initialize all the loTypes - default value for the new type is areAllSelected constant
            loTypes[type] = loTypes[type] ?? areAllSelected;
        }

        return loTypes;
    }

    private async getReportsDataFromHydra(userIds: (string | number)[]): Promise<UsersResponse> {
        return await this.hydraService.getUsers(userIds as number[]);
    }

    private addAuthorNameToTheReport(aamonReports: Report[], aamonReportsAuthors: Map<number, AuthorName>) {
        for (const report of aamonReports) {
            const reportAuthorInfo = aamonReportsAuthors.get(report.createdBy as number);

            if (reportAuthorInfo) {
                report.createdByDescription = {
                    firstname: reportAuthorInfo.firstName,
                    lastname: reportAuthorInfo.lastName,
                    userId: reportAuthorInfo.userId,
                };
            }
        }
    }

    async getOldReportsFromHydra(payload: MigrateInputPayload): Promise<LegacyReportsResponse> {
        const response = await this.hydraService.getOldReportsFromHydra(payload);
        if (!response || !response.data) {
            this.logger.error('Bad response format from getOldReportsFromHydra');
            throw new Error('Cannot retrieve reports from hydra');
        }
        return response.data;
    }
}
