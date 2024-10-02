import Config from '../../config';
import { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { ReportId } from '../value_objects/ReportId';
import { Report } from '../entities/Report';
import { ReportsRepository } from '../repositories/ReportsRepository';
import UserManager from '../../services/session/user-manager.session';
import { v4 } from 'uuid';
import { TimeFrameOptions, VisibilityTypes } from '../../models/custom-report';
import { ReportsTypes } from '../constants/report-types';
import moment from 'moment/moment';

export class ReportClone {
    public constructor(
        private readonly user: UserManager
    ) {
    }

    private REPORT_TYPES_USERS: string[] = [
        ReportsTypes.LP_USERS_STATISTICS,
        ReportsTypes.CERTIFICATIONS_USERS,
        ReportsTypes.COURSES_USERS,
        ReportsTypes.VIEWER_ASSET_DETAILS,
        ReportsTypes.USERS_CERTIFICATIONS,
        ReportsTypes.USERS_LEARNINGOBJECTS,
        ReportsTypes.USERS,
        ReportsTypes.USER_CONTRIBUTIONS,
        ReportsTypes.USERS_BADGES,
        ReportsTypes.SURVEYS_INDIVIDUAL_ANSWERS,
        ReportsTypes.USERS_EXTERNAL_TRAINING,
        ReportsTypes.GROUPS_COURSES,
        ReportsTypes.USERS_COURSES,
        ReportsTypes.QUERY_BUILDER_DETAIL,
        ReportsTypes.ECOMMERCE_TRANSACTION,
        ReportsTypes.SESSIONS_USER_DETAIL,
        ReportsTypes.USERS_CLASSROOM_SESSIONS,
        ReportsTypes.USERS_ENROLLMENT_TIME,
        ReportsTypes.USERS_LP,
        ReportsTypes.USERS_WEBINAR,
    ];

    private REPORT_TYPES_COURSES: string[] = [
        ReportsTypes.USERS_LEARNINGOBJECTS,
        ReportsTypes.SURVEYS_INDIVIDUAL_ANSWERS,
        ReportsTypes.GROUPS_COURSES,
        ReportsTypes.COURSES_USERS,
        ReportsTypes.ECOMMERCE_TRANSACTION,
        ReportsTypes.SESSIONS_USER_DETAIL,
        ReportsTypes.USERS_CLASSROOM_SESSIONS,
        ReportsTypes.USERS_ENROLLMENT_TIME,
        ReportsTypes.USERS_WEBINAR,
        ReportsTypes.USERS_COURSES,
    ];

    private resetForChangingPU(type: string, author: number, clonedReport: Report): void {
        if (!(this.user.isPowerUser() && author !== this.user.getIdUser())) {
            return;
        }
        if (this.REPORT_TYPES_USERS.includes(type)) {
            if (clonedReport.Info.users) {
                clonedReport.Info.users.all = true;
                clonedReport.Info.users.users = [];
                clonedReport.Info.users.groups = [];
                clonedReport.Info.users.branches = [];
                clonedReport.Info.users.isUserAddFields = false;
                clonedReport.Info.userAdditionalFieldsFilter = {};
            }
        }
        if (this.REPORT_TYPES_COURSES.includes(type)) {
            if (clonedReport.Info.courses) {
                clonedReport.Info.courses.all = true;
                clonedReport.Info.courses.courses = [];
            }
        }
        if (this.REPORT_TYPES_COURSES.includes(type) ||
            type === ReportsTypes.LP_USERS_STATISTICS ||
            type === ReportsTypes.USERS_LP
        ) {
            if (clonedReport.Info.learningPlans) {
                clonedReport.Info.learningPlans.all = true;
                clonedReport.Info.learningPlans.learningPlans = [];
            }
        }
        switch (type) {
            case ReportsTypes.ASSETS_STATISTICS:
            case ReportsTypes.VIEWER_ASSET_DETAILS:
                if (clonedReport.Info.assets) {
                    clonedReport.Info.assets.all = true;
                    clonedReport.Info.assets.assets = [];
                    clonedReport.Info.assets.channels = [];
                }
                break;
            case ReportsTypes.USERS_BADGES:
                if (clonedReport.Info.badges) {
                    clonedReport.Info.badges.all = true;
                    clonedReport.Info.badges.badges = [];
                }
                break;
            case ReportsTypes.USERS_CLASSROOM_SESSIONS:
                if (clonedReport.Info.sessions) {
                    clonedReport.Info.sessions.all = true;
                    clonedReport.Info.sessions.sessions = [];
                }
                break
            case ReportsTypes.SURVEYS_INDIVIDUAL_ANSWERS:
                if (clonedReport.Info.surveys) {
                    clonedReport.Info.surveys.all = true;
                    clonedReport.Info.surveys.surveys = [];
                }
                break;
        }
    }

    private convertDateObjectToDatetime(d: Date): string {
        return moment(d).format('YYYY-MM-DD HH:mm:ss');
    }

    public async execute(reportId: ReportId, title: string, description: string, queryBuilderId?: string): Promise<Report> {
        const date = new Date();
        const config = new Config();
        const region = config.getAwsRegion();
        const documentDb = DynamoDBDocumentClient.from(
            new DynamoDBClient({region})
        );
        const repository = new ReportsRepository(documentDb, config.getReportsTableName());
        const originalReport = await repository.getById(reportId);
        const clonedReport = new Report(
            new ReportId(v4(), reportId.Platform),
            originalReport.Info
        );
        if (queryBuilderId) {
            clonedReport.Info.queryBuilderId = queryBuilderId;
        }
        // A cloned report cannot be a standard one
        clonedReport.Info.standard = false;

        // Update the report title and description
        clonedReport.Info.title = title;
        clonedReport.Info.description = description;

        // Reset the report visibility and the planning options
        clonedReport.Info.visibility.type = VisibilityTypes.ALL_GODADMINS;
        clonedReport.Info.visibility.users = [];
        clonedReport.Info.visibility.groups = [];
        clonedReport.Info.visibility.branches = [];
        clonedReport.Info.planning = {
            active: false,
            option: {
                timeFrame: TimeFrameOptions.days,
                every: 1,
                recipients: [],
                isPaused: false,
                scheduleFrom: '',
                startHour: '00:00',
                timezone: this.user.getTimezone(),
            }
        };
        // Update the author and the last edit info with the current user info and date info
        clonedReport.Info.author = this.user.getIdUser();
        clonedReport.Info.lastEditBy.idUser = this.user.getIdUser();
        clonedReport.Info.lastEditBy.lastname = clonedReport.Info.lastEditBy.firstname = clonedReport.Info.lastEditBy.username = clonedReport.Info.lastEditBy.avatar = '';
        clonedReport.Info.creationDate = this.convertDateObjectToDatetime(date);
        clonedReport.Info.lastEdit = this.convertDateObjectToDatetime(date);

        // Reset filter if clone is made by a different user and it's a PU
        this.resetForChangingPU(originalReport.Info.type, originalReport.Info.author, clonedReport);

        await repository.add(clonedReport);

        return clonedReport;
    }
}
