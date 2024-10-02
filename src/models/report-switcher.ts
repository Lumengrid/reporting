import moment from 'moment';

import { BadRequestException, DisabledReportTypeException, ErrorCode, NotFoundException } from '../exceptions';
import {
    reportManageEnrollmentFields,
    reportManagerCertificationsFields,
    reportManagerCourseFields,
    reportManagerCourseUserFields,
    reportManagerLpEnrollmentFields,
    reportManagerLpFields,
    reportManagerSessionFields,
    reportManagerUsageStatisticsFields,
    reportManagerUserFields,
} from '../reports/constants/report-manager-fields';
import { ReportsTypes } from '../reports/constants/report-types';
import { Utils } from '../reports/utils';
import { Dynamo, DynamoReport } from '../services/dynamo';
import SessionManager from '../services/session/session-manager.session';
import { BaseReportManager } from './base-report-manager';
import { UserAdditionalFieldsFilterParam, UserAdditionalFieldType, VisibilityTypes } from './custom-report';
import { AssetsStatisticsManager } from './report-assets-statistics';
import { CertificationsUsersManager } from './report-certifications-users';
import { CoursesUsersManager } from './report-courses-users';
import { EcommerceTransactionsManager } from './report-ecommerce-transaction';
import { GroupsCoursesManager } from './report-groups-courses';
import {
    FieldsList,
    ReportAvailablesFields,
    ReportField,
    ReportManagerCertificationsFilter,
    ReportManagerInfoCoursesFilter,
    ReportManagerInfoUsersFilter,
    ReportManagerLearningPlansFilter,
} from './report-manager';
import { QueryBuilderManager } from './report-query-builder';
import { UserContributionsManager } from './report-user-contributions';
import { LearningplansUsersStatisticsManager } from './report-learningplans-users-statistics';
import { UsersManager } from './report-users';
import { UsersAssets } from './report-users-assets';
import { UsersBadges } from './report-users-badges';
import { UsersCertificationsManager } from './report-users-certifications';
import { UsersClassroomSessionsManager } from './report-users-classroom-sessions';
import { UsersCoursesManager } from './report-users-courses';
import { UsersEnrollmentTimeManager } from './report-users-enrollment-time';
import { UsersExternalTrainingManager } from './report-users-external-training';
import { UsersLearningObjectManager } from './report-users-learningobject';
import { UsersLearningPlansManager } from './report-users-learningplans';
import { UsersWebinarManager } from './report-users-webinar';
import { SessionsUserStatisticsManager } from './report-sessions-user';
import { SurveysIndividualAnswersManager } from './report-surveys-individual-answers';
import { ErrorsCode } from './base';


export async function ReportManagerSwitcher(session: SessionManager, idReport: string, isDownloadAction = false): Promise<BaseReportManager> {
        const dynamo: Dynamo = session.getDynamo();
        let reportDetails: DynamoReport;

        try {
            reportDetails = await dynamo.getReport(idReport);
        } catch (e: any) {
            if (e instanceof NotFoundException) {
                throw new NotFoundException('Report not found!', ErrorsCode.ReportNotExist);
            }
            throw new Error(`Cannot get report details, error was: ${e}`);
        }

        if (!reportDetails || !reportDetails.type) {
            throw new Error(`Cannot get report details or type`);
        }

        const newReportManagerSwitcher = NewReportManagerSwitcher(session, reportDetails.type, reportDetails);
        if (reportDetails.type === ReportsTypes.QUERY_BUILDER_DETAIL) {
            await (newReportManagerSwitcher as QueryBuilderManager).loadQueryBuilder(session, reportDetails.queryBuilderId);

        }
        if (newReportManagerSwitcher.info.fields && newReportManagerSwitcher.info.fields.length > 0) {
            const index = newReportManagerSwitcher.info.fields.indexOf(FieldsList.USER_BRANCHES);
            if (index >= 0) {
                const indexPath = newReportManagerSwitcher.info.fields.indexOf(FieldsList.USER_BRANCH_PATH);
                if (indexPath >= 0) {
                    newReportManagerSwitcher.info.fields.slice(index, 1);
                } else {
                    newReportManagerSwitcher.info.fields[index] = FieldsList.USER_BRANCH_PATH;
                }
            }
        }
        // if is a download action all for 'power user' checks are skipped
        if (session.user.isPowerUser() && isDownloadAction === false) {
            // If the user id the author of the report add it to the list
            if (newReportManagerSwitcher.info.author === session.user.getIdUser() || newReportManagerSwitcher.info.visibility.type === VisibilityTypes.ALL_GODADMINS_AND_PU) {
                return newReportManagerSwitcher;
            } else if (newReportManagerSwitcher.info.visibility.type === VisibilityTypes.ALL_GODADMINS_AND_SELECTED_PU) {
                // Check for specific visibility
                // Visibility by users
                if (newReportManagerSwitcher.info.visibility.users && newReportManagerSwitcher.info.visibility.users.length > 0 && newReportManagerSwitcher.info.visibility.users.map(a => a.id).indexOf(session.user.getIdUser()) !== -1) {
                    return newReportManagerSwitcher;
                }

                // Visibility by groups
                if (newReportManagerSwitcher.info.visibility.groups && newReportManagerSwitcher.info.visibility.groups.length > 0) {
                    for (const group of session.user.getUserGroups()) {
                        if (newReportManagerSwitcher.info.visibility.groups.map(a => a.id).indexOf(group) !== -1) {
                            return newReportManagerSwitcher;
                        }
                    }
                }

                // Visibility by branches
                if (newReportManagerSwitcher.info.visibility.branches && newReportManagerSwitcher.info.visibility.branches.length > 0) {
                    const descendants: number[] = [];
                    const noDescendants: number[] = [];

                    newReportManagerSwitcher.info.visibility.branches.forEach(element => {
                        if (typeof element.id === 'string') {
                            element.id = parseInt(element.id, 10);
                        }

                        if (element.descendants) {
                            descendants.push(element.id);
                        } else {
                            noDescendants.push(element.id);
                        }
                    });

                    // Check for direct branch assignment
                    if (noDescendants.length > 0) {
                        for (const branch of session.user.getUserBranches()) {
                            if (noDescendants.indexOf(branch) !== -1) {
                                return newReportManagerSwitcher;
                            }
                        }
                    }

                    // Check for branch assignment with parents
                    if (descendants.length > 0) {
                        for (const branch of session.user.getUserBranchesWithParents()) {
                            if (descendants.indexOf(branch) !== -1) {
                                return newReportManagerSwitcher;
                            }
                        }
                    }
                }
            }

            throw new NotFoundException('Report not found!', ErrorsCode.ReportNotExist);
        }

        return newReportManagerSwitcher;
}

export function NewReportManagerSwitcher(session: SessionManager, type: string, reportDetails?: DynamoReport): BaseReportManager {
    switch (type) {
        case ReportsTypes.USERS_COURSES:
            return new UsersCoursesManager(session, reportDetails);
        case ReportsTypes.GROUPS_COURSES:
            return new GroupsCoursesManager(session, reportDetails);
        case ReportsTypes.USERS_WEBINAR:
            return new UsersWebinarManager(session, reportDetails);
        case ReportsTypes.USERS_LP:
            return new UsersLearningPlansManager(session, reportDetails);
        case ReportsTypes.COURSES_USERS:
            return new CoursesUsersManager(session, reportDetails);
        case ReportsTypes.USERS_LEARNINGOBJECTS:
            return new UsersLearningObjectManager(session, reportDetails);
        case ReportsTypes.USERS_CLASSROOM_SESSIONS:
            if (!session.platform.checkPluginClassroomEnabled()) {
                throw new DisabledReportTypeException('Report type disabled', ErrorCode.DISABLED_REPORT_TYPE);
            }
            return new UsersClassroomSessionsManager(session, reportDetails);
        case ReportsTypes.USERS_ENROLLMENT_TIME:
            return new UsersEnrollmentTimeManager(session, reportDetails);
        case ReportsTypes.USERS:
            return new UsersManager(session, reportDetails);
        case ReportsTypes.USERS_CERTIFICATIONS:
            if (!session.platform.checkPluginCertificationEnabled()) {
                throw new DisabledReportTypeException('Report type disabled', ErrorCode.DISABLED_REPORT_TYPE);
            }
            return new UsersCertificationsManager(session, reportDetails);
        case ReportsTypes.USERS_BADGES:
            if (!session.platform.checkPluginGamificationEnabled()) {
                throw new DisabledReportTypeException('Report type disabled', ErrorCode.DISABLED_REPORT_TYPE);
            }
            return new UsersBadges(session, reportDetails);
        case ReportsTypes.CERTIFICATIONS_USERS:
            if (!session.platform.checkPluginCertificationEnabled()) {
                throw new DisabledReportTypeException('Report type disabled', ErrorCode.DISABLED_REPORT_TYPE);
            }
            return new CertificationsUsersManager(session, reportDetails);
        case ReportsTypes.USERS_EXTERNAL_TRAINING:
            if (!session.platform.checkPluginTranscriptEnabled()) {
                throw new DisabledReportTypeException('Report type disabled', ErrorCode.DISABLED_REPORT_TYPE);
            }
            return new UsersExternalTrainingManager(session, reportDetails);
        case ReportsTypes.ECOMMERCE_TRANSACTION:
            if (!session.platform.checkPluginEcommerceEnabled()) {
                throw new DisabledReportTypeException('Report type disabled', ErrorCode.DISABLED_REPORT_TYPE);
            }
            return new EcommerceTransactionsManager(session, reportDetails);
        case ReportsTypes.ASSETS_STATISTICS:
            if (!session.platform.checkPluginShareEnabled()) {
                throw new DisabledReportTypeException('Report type disabled', ErrorCode.DISABLED_REPORT_TYPE);
            }
            return new AssetsStatisticsManager(session, reportDetails);
        case ReportsTypes.USER_CONTRIBUTIONS:
            if (!session.platform.checkPluginShareEnabled()) {
                throw new DisabledReportTypeException('Report type disabled', ErrorCode.DISABLED_REPORT_TYPE);
            }
            return new UserContributionsManager(session, reportDetails);
        case ReportsTypes.QUERY_BUILDER_DETAIL:
            return new QueryBuilderManager(session, reportDetails);
        case ReportsTypes.VIEWER_ASSET_DETAILS:
            if (!session.platform.checkPluginShareEnabled()) {
                throw new DisabledReportTypeException('Report type disabled', ErrorCode.DISABLED_REPORT_TYPE);
            }
            return new UsersAssets(session, reportDetails);
        case ReportsTypes.SESSIONS_USER_DETAIL:
            return new SessionsUserStatisticsManager(session, reportDetails);
        case ReportsTypes.SURVEYS_INDIVIDUAL_ANSWERS:
            return new SurveysIndividualAnswersManager(session, reportDetails);
        case ReportsTypes.LP_USERS_STATISTICS:
            if (!session.platform.isDatalakeV3ToggleActive() || !session.platform.isToggleLearningPlansStatisticsReport()) {
                throw Error('Invalid report type');
            }
            return new LearningplansUsersStatisticsManager(session, reportDetails);
        default:
            throw Error('Invalid report type');
    }
}

export async function getDefaultStructureReportManager(session: SessionManager, type: string, body: any, isMyTeamUserAddFilter: boolean, userAddFieldsDropdown: UserAdditionalFieldType[]): Promise<BaseReportManager> {
    let report: BaseReportManager;
    let availableFields: ReportAvailablesFields;

    // load parameters from the body payload
    const managerTypes = body.managerTypes ? body.managerTypes : [];
    const enrollmentStatusSelected = body.enrollmentStatus ? body.enrollmentStatus : [];
    const enrollmentDate = body.enrollmentDate ? body.enrollmentDate : {};
    const userAdditionalFieldsFilterParam: UserAdditionalFieldsFilterParam[] | [] = body.userAdditionalFieldsFilter ? body.userAdditionalFieldsFilter : [];

    checkEnrollmentDateIsValid(enrollmentDate);

    const enrollmentStatusFields = [
        'completed',
        'inProgress',
        'notStarted', // subscribed
        'waitingList',
        'suspended',
        'enrollmentsToConfirm',
        'overbooking',
    ];

    switch (type) {
        case ReportsTypes.MANAGER_USERS_COURSES:
            report = new UsersCoursesManager(session, undefined);
            availableFields = await getBaseAvailableFieldsForManager(ReportsTypes.MANAGER_USERS_COURSES, await report.loadTranslations(true), session);

            report.info.courses = new ReportManagerInfoCoursesFilter();
            report.info.courses.all = true;

            report.info.learningPlans = new ReportManagerLearningPlansFilter();
            report.info.learningPlans.all = true;

            report.info.courseExpirationDate = report.getDefaultDateOptions();
            report.info.completionDate = report.getDefaultDateOptions();

            break;
        case ReportsTypes.MANAGER_USERS_LP:
            report = new UsersLearningPlansManager(session, undefined);
            availableFields = await getBaseAvailableFieldsForManager(ReportsTypes.MANAGER_USERS_LP, await report.loadTranslations(true));

            report.info.learningPlans = new ReportManagerLearningPlansFilter();
            report.info.learningPlans.all = true;
            report.info.completionDate = report.getDefaultDateOptions();

            break;
        case ReportsTypes.MANAGER_USERS_CERTIFICATIONS:
            report = new UsersCertificationsManager(session, undefined);
            availableFields = await getBaseAvailableFieldsForManager(ReportsTypes.MANAGER_USERS_CERTIFICATIONS, await report.loadTranslations(true));

            report.info.certifications = new ReportManagerCertificationsFilter();

            break;
        case ReportsTypes.MANAGER_USERS_CLASSROOM_SESSIONS:
            report = new UsersClassroomSessionsManager(session, undefined);
            availableFields = await getBaseAvailableFieldsForManager(ReportsTypes.MANAGER_USERS_CLASSROOM_SESSIONS, await report.loadTranslations(true));

            report.info.courses = new ReportManagerInfoCoursesFilter();
            report.info.courses.all = true;
            report.info.courseExpirationDate = report.getDefaultDateOptions();
            report.info.learningPlans = new ReportManagerLearningPlansFilter();
            report.info.learningPlans.all = true;

            report.info.completionDate = report.getDefaultDateOptions();

            break;
        default:
            throw new BadRequestException('Invalid report type', ErrorCode.REPORT_TYPE_NOT_VALID);
    }

    report.info.type = type;

    if (session.platform.isDatalakeV3ToggleActive()) {
        const isManagerSubordinates = await report.createManagerSubordinatesFilterTableV3(session.user.getIdUser(), managerTypes);
        if (!isManagerSubordinates) {
            throw new BadRequestException(`No members in the team`, ErrorCode.NO_MEMBER_IN_TEAM);
        }
    } else {
        const isManagerSubordinates = await report.createManagerSubordinatesFilterTable(session.user.getIdUser(), managerTypes);
        if (!isManagerSubordinates) {
            throw new BadRequestException(`No members in the team`, ErrorCode.NO_MEMBER_IN_TEAM);
        }
    }

    const enrollmentStatus = report.getDefaultEnrollment();

    if (enrollmentStatusSelected.length > 0) {
        Object.keys(enrollmentStatus).forEach(v => enrollmentStatus[v] = false);

        for (const status of enrollmentStatusSelected) {
            const statusName = enrollmentStatusFields[status];
            if (statusName) {
                enrollmentStatus[`${statusName}`] = true;
            } else {
                throw new BadRequestException(`Enrollment status is not valid`, ErrorCode.ENROLLMENT_STATUS_NOT_VALID);
            }
        }

    }

    report.info.users = new ReportManagerInfoUsersFilter();
    report.info.users.all = false;
    report.info.users.users = [];

    report.info.enrollment = enrollmentStatus;

    report.info.enrollmentDate = report.getDefaultDateOptions();

    if (Object.keys(enrollmentDate).length > 0) {
        enrollmentDate.any = false;
        report.info.enrollmentDate = enrollmentDate;
    }

    // User Additional fields filter
    if (isMyTeamUserAddFilter && userAdditionalFieldsFilterParam.length > 0) {
        userAdditionalFieldsFilterParamValidation(userAdditionalFieldsFilterParam, userAddFieldsDropdown);

        const userAdditionalFieldsFilter = transformUserAdditionalFieldsFilterParam(userAdditionalFieldsFilterParam);
        report.info.userAdditionalFieldsFilter = userAdditionalFieldsFilter;
        report.info.users.isUserAddFields = true;
    }


    const fields = [];
    let reportField: ReportField;
    Object.keys(availableFields).forEach(key => {
        for (reportField of availableFields[key]) {
            fields.push(reportField.field);
        }
    });


    report.info.fields = fields;
    report.info.timezone = session.user.getTimezone();

    return report;
}

/**
 * Check if the enrollment date object sent in input is valid.
 * If it is not valid throw the exception, otherwise no operation
 * @param enrollmentDate
 */
function checkEnrollmentDateIsValid(enrollmentDate): void {
    if (Object.keys(enrollmentDate).length === 0) {
        return;
    }

    hasOwnProperties(['type'], enrollmentDate, 'enrollmentDate', ErrorCode.ENROLLMENT_DATE_NOT_VALID);

    if (enrollmentDate.type !== 'range' && enrollmentDate.type !== 'absolute' && enrollmentDate.type !== 'relative') {
        throw new BadRequestException(`Property 'type' is not valid. Accepted values: 'absolute', 'range' or 'relative'`, ErrorCode.ENROLLMENT_DATE_NOT_VALID);
    }

    if (enrollmentDate.type === 'range') {
        hasOwnProperties(['operator', 'from', 'to'], enrollmentDate, 'enrollmentDate', ErrorCode.ENROLLMENT_DATE_NOT_VALID);

        if (enrollmentDate.operator !== 'range') {
            throw new BadRequestException(`Property 'operator' is not valid. Accepted values: 'range'`, ErrorCode.ENROLLMENT_DATE_NOT_VALID);
        }

        if (!moment(enrollmentDate.from, 'YYYY-MM-DD', true).isValid()) {
            throw new BadRequestException(`Property 'from' is not valid. Format accepted 'YYYY-MM-DD'`, ErrorCode.ENROLLMENT_DATE_NOT_VALID);
        }

        if (!moment(enrollmentDate.to, 'YYYY-MM-DD', true).isValid()) {
            throw new BadRequestException(`Property 'to' is not valid. Format accepted 'YYYY-MM-DD'`, ErrorCode.ENROLLMENT_DATE_NOT_VALID);
        }
    }

    if (enrollmentDate.type === 'relative') {
        hasOwnProperties(['operator', 'days'], enrollmentDate, 'enrollmentDate', ErrorCode.ENROLLMENT_DATE_NOT_VALID);

        if (enrollmentDate.operator !== 'isBefore' && enrollmentDate.operator !== 'isAfter') {
            throw new BadRequestException(`Property 'operator' is not valid. Accepted values: 'isBefore' and 'isAfter'`, ErrorCode.ENROLLMENT_DATE_NOT_VALID);
        }

        if (typeof enrollmentDate.days !== 'number') {
            throw new BadRequestException(`Property 'days' is not valid. Accepted only number`, ErrorCode.ENROLLMENT_DATE_NOT_VALID);
        }
    }

    if (enrollmentDate.type === 'absolute') {
        hasOwnProperties(['operator', 'to'], enrollmentDate, 'enrollmentDate', ErrorCode.ENROLLMENT_DATE_NOT_VALID);

        if (enrollmentDate.operator !== 'isBefore' && enrollmentDate.operator !== 'isAfter') {
            throw new BadRequestException(`Operator wrong. Accepted operator in range type: 'isBefore' and 'isAfter'`, ErrorCode.ENROLLMENT_DATE_NOT_VALID);
        }


        if (!moment(enrollmentDate.to, 'YYYY-MM-DD', true).isValid()) {
            throw new BadRequestException(`Property 'to' is not valid. Format accepted 'YYYY-MM-DD'`, ErrorCode.ENROLLMENT_DATE_NOT_VALID);
        }
    }
}

function hasOwnProperties(properties: string[], object: any, objectName: string, errorCode: number): boolean {
    for (const property of properties) {
        if (!object.hasOwnProperty(`${property}`) || object[`${property}`] === '') {
            throw new BadRequestException(`Missing property '${property}' or is empty in ${objectName} object`, errorCode);
        }
    }

    return true;
}

function userAdditionalFieldsFilterParamValidation(userAdditionalFieldsFilterParam: UserAdditionalFieldsFilterParam[], userAddFieldsDropdown: UserAdditionalFieldType[]) {

    // Store only the ids for the visiblity check
    const userAddFieldsVisibleIds = userAddFieldsDropdown.map(item => item.id);
    const userAddFieldsParamsIds = userAdditionalFieldsFilterParam.map(item => item.key);

    userAdditionalFieldsFilterParam.forEach((filter: UserAdditionalFieldsFilterParam) => {

        // The input params must be an object with only "key" and "value" properties
        if (filter.hasOwnProperty('key') === false || filter.hasOwnProperty('value') === false || Object.keys(filter).length > 2) {
            throw new BadRequestException(`User additional field has not a valid format`, ErrorCode.USER_ADD_FIELD_FORMAT_NOT_VALID);
        }

        // Check if input values are integers
        if (typeof filter.key !== 'number' || !Number.isInteger(filter.key)) {
            throw new BadRequestException(`Field key must be an integer`, ErrorCode.USER_ADD_FIELD_TYPE_NOT_VALID);
        }
        if (typeof filter.value !== 'number' || !Number.isInteger(filter.value)) {
            throw new BadRequestException(`Field value must be an integer`, ErrorCode.USER_ADD_FIELD_TYPE_NOT_VALID);
        }
    });

    // Check if the user has visibility on the field passed as param
    if (!userAddFieldsParamsIds.every(filterId => userAddFieldsVisibleIds.includes(filterId))) {
        throw new BadRequestException(`User additional field doesn't exist or is not available`, ErrorCode.USER_ADD_FIELD_NOT_AVAILABLE);
    }

}

// Transform the input params before passing it to the query
function transformUserAdditionalFieldsFilterParam(userAdditionalFieldsFilterParam: UserAdditionalFieldsFilterParam[]): { [key: string]: number } {

    const userAdditionalFieldsFilter: { [key: string]: number } = {};
    userAdditionalFieldsFilterParam.forEach(filter => {
        const keyName = filter.key;
        const value = filter.value;

        userAdditionalFieldsFilter[keyName] = value;

    });
    return userAdditionalFieldsFilter;
}

// Get selected fields for each Report Manager type
async function getBaseAvailableFieldsForManager(reportType: ReportsTypes, translations: { [key: string]: string }, session?: SessionManager): Promise<ReportAvailablesFields> {
    const utils = new Utils();
    const result: ReportAvailablesFields = {};

    switch (true) {
        case (reportType === ReportsTypes.MANAGER_USERS_COURSES):
            result.user = utils.getFieldsForManager(reportManagerUserFields, translations);
            result.course = utils.getFieldsForManager(reportManagerCourseFields, translations);
            result.courseuser = utils.getFieldsForManager(reportManagerCourseUserFields(session), translations);
            result.usageStatistics = utils.getFieldsForManager(reportManagerUsageStatisticsFields, translations);
            break;
        case (reportType === ReportsTypes.MANAGER_USERS_LP):
            result.user = utils.getFieldsForManager(reportManagerUserFields, translations);
            result.learningPlans = utils.getFieldsForManager(reportManagerLpFields, translations);
            result.learningPlansEnrollments = utils.getFieldsForManager(reportManagerLpEnrollmentFields, translations);
            break;
        case (reportType === ReportsTypes.MANAGER_USERS_CERTIFICATIONS):
            result.user = utils.getFieldsForManager(reportManagerUserFields, translations);
            result.certifications = utils.getFieldsForManager(reportManagerCertificationsFields, translations);
            break;
        case (reportType === ReportsTypes.MANAGER_USERS_CLASSROOM_SESSIONS):
            result.user = utils.getFieldsForManager(reportManagerUserFields, translations);
            result.course = utils.getFieldsForManager(reportManagerCourseFields, translations);
            result.session = utils.getFieldsForManager(reportManagerSessionFields, translations);
            result.enrollment = utils.getFieldsForManager(reportManageEnrollmentFields, translations);
            break;
        default: break;
    }

    return result;
}
