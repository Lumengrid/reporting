import {
    AssetsEntitiesLimits,
    CoursesEntitiesLimits,
    ReportManagerAssetsFilter,
    ReportManagerBadgesFilter,
    ReportManagerCertificationsFilter,
    ReportManagerInfo,
    ReportManagerInfoCoursesFilter,
    ReportManagerInfoSessionsFilter,
    ReportManagerInfoSurveysFilter,
    ReportManagerInfoUsersFilter,
    ReportManagerLearningPlansFilter,
    SelectionInfo,
    UsersEntitiesLimits
} from './report-manager';
import {
    DateOptionsValueDescriptor,
    Enrollment,
    ExternalTrainingStatusFilter,
    InstructorsFilter,
    Properties,
    PublishStatusFilter,
    SessionAttendanceType,
    SessionDates,
    VisibilityTypes
} from './custom-report';
import { ReportsTypes } from '../reports/constants/report-types';

export class CheckReportUpdates {
    private stringChanged(before?: string, after?: string): boolean {
        if (before === undefined && after === undefined) {
            return false;
        }
        const beforeValue = before === undefined || !before ? '' : before;
        const afterValue = after === undefined || !after ? '' : after;
        return beforeValue !== afterValue;
    }

    private booleanChanged(before?: boolean, after?: boolean): boolean {
        if (before === undefined && after === undefined) {
            return false;
        }
        const beforeValue = before === undefined || !before ? false : before;
        const afterValue = after === undefined || !after ? false : after;
        return beforeValue !== afterValue;
    }

    private numberChanged(before?: number, after?: number): boolean {
        if (before === undefined && after === undefined) {
            return false;
        }
        const beforeValue = before === undefined || !before ? 0 : before;
        const afterValue = after === undefined || !after ? 0 : after;
        return beforeValue !== afterValue;
    }
    private selectionInfoChanged(before?: SelectionInfo[], after?: SelectionInfo[]): boolean {
        if ((before === undefined && after === undefined) ||
            (before === undefined && after !== undefined && after.length === 0) ||
            (after === undefined && before !== undefined && before.length === 0)) {
            return false;
        }
        if (after === undefined || before !== undefined && before.length !== after.length) {
            return true;
        }
        for (const item of before) {
            const filtered = after.filter(it => it.id === item.id);
            if (
                filtered === undefined ||
                filtered.length === 0 ||
                this.booleanChanged(filtered[0].descendants, item.descendants)
            ) {
                return true;
            }
        }
    }

    private conditionsChanged(type: string, before?: string, after?: string): boolean {
        if (![ReportsTypes.USERS_CLASSROOM_SESSIONS.toString(), ReportsTypes.USERS_COURSES.toString(),
            ReportsTypes.COURSES_USERS.toString(), ReportsTypes.SESSIONS_USER_DETAIL.toString(),
            ReportsTypes.SURVEYS_INDIVIDUAL_ANSWERS.toString(), ReportsTypes.USERS_LEARNINGOBJECTS.toString(),
            ReportsTypes.USERS_WEBINAR.toString(), ReportsTypes.USERS.toString()].includes(type)) {
            return false;
        }
        return this.stringChanged(before, after);
    }

    private dateOptionsChanged(before?: DateOptionsValueDescriptor, after?: DateOptionsValueDescriptor): boolean {
        if (before === undefined && after === undefined) {
            return false;
        }
        return (before === undefined && after !== undefined) ||
            after === undefined ||
            this.booleanChanged(before.any, after.any) ||
            this.stringChanged(before.operator, after.operator) ||
            this.stringChanged(before.type, after.type) ||
            this.stringChanged(before.from, after.from) ||
            this.stringChanged(before.to, after.to) ||
            this.numberChanged(before.days, after.days);
    }

    private usersEntitiesLimitsChanged(before?: UsersEntitiesLimits, after?: UsersEntitiesLimits): boolean {
        if (before === undefined && after === undefined) {
            return false;
        }
        return (before === undefined && after !== undefined) ||
            (before !== undefined && after === undefined) ||
            !after ||
            (
                before !== undefined &&
                (
                    this.numberChanged(before.branchesLimit, after.branchesLimit) ||
                    this.numberChanged(before.groupsLimit, after.groupsLimit) ||
                    this.numberChanged(before.usersLimit, after.usersLimit)
                )
            );
    }

    private assetsEntitiesLimitsChanged(before?: AssetsEntitiesLimits, after?: AssetsEntitiesLimits): boolean {
        if (before === undefined && after === undefined) {
            return false;
        }
        return (before === undefined && after !== undefined) ||
            (before !== undefined && after === undefined) ||
            !after ||
            (
                before !== undefined &&
                (
                    this.numberChanged(before.assetsLimit, after.assetsLimit) ||
                    this.numberChanged(before.channelsLimit, after.channelsLimit)
                )
            );
    }

    private coursesEntitiesLimitsChanged(before?: CoursesEntitiesLimits, after?: CoursesEntitiesLimits): boolean {
        if (before === undefined && after === undefined) {
            return false;
        }
        return (before === undefined && after !== undefined) ||
            (before !== undefined && after === undefined) ||
            !after ||
            (
                before !== undefined &&
                (
                    this.numberChanged(before.coursesLimit, after.coursesLimit) ||
                    this.numberChanged(before.lpLimit, after.lpLimit) ||
                    this.numberChanged(before.courseInstructorsLimit, after.courseInstructorsLimit) ||
                    this.numberChanged(before.classroomLimit, after.classroomLimit) ||
                    this.numberChanged(before.sessionLimit, after.sessionLimit) ||
                    this.numberChanged(before.webinarLimit, after.webinarLimit)
                )
            );
    }

    private usersChanged(before?: ReportManagerInfoUsersFilter, after?: ReportManagerInfoUsersFilter): boolean {
        if (before === undefined && after === undefined) {
            return false;
        }
        return (before === undefined && after !== undefined) ||
            after === undefined ||
            this.booleanChanged(before.all, after.all) ||
            this.booleanChanged(before.hideExpiredUsers, after.hideExpiredUsers) ||
            this.booleanChanged(before.hideExpiredUsers, after.hideExpiredUsers) ||
            this.booleanChanged(before.hideDeactivated, after.hideDeactivated) ||
            this.booleanChanged(before.isUserAddFields, after.isUserAddFields) ||
            this.booleanChanged(before.showOnlyLearners, after.showOnlyLearners) ||
            this.usersEntitiesLimitsChanged(before.entitiesLimits, after.entitiesLimits) ||
            this.selectionInfoChanged(before.users, after.users) ||
            this.selectionInfoChanged(before.groups, after.groups) ||
            this.selectionInfoChanged(before.branches, after.branches);
    }

    private coursesChanged(type: string, before?: ReportManagerInfoCoursesFilter, after?: ReportManagerInfoCoursesFilter): boolean {
        if (![ReportsTypes.COURSES_USERS.toString(), ReportsTypes.ECOMMERCE_TRANSACTION.toString(),
            ReportsTypes.GROUPS_COURSES.toString(), ReportsTypes.SESSIONS_USER_DETAIL.toString(),
            ReportsTypes.SURVEYS_INDIVIDUAL_ANSWERS.toString(), ReportsTypes.USERS_CLASSROOM_SESSIONS.toString(),
            ReportsTypes.USERS_COURSES.toString(), ReportsTypes.USERS_ENROLLMENT_TIME.toString(),
            ReportsTypes.USERS_LEARNINGOBJECTS.toString(), ReportsTypes.USERS_WEBINAR.toString()].includes(type)) {
            return false;
        }
        if (before === undefined && after === undefined) {
            return false;
        }
        return (before === undefined && after !== undefined) ||
            after === undefined ||
            this.booleanChanged(before.all, after.all) ||
            before.courseType !== after.courseType ||
            this.coursesEntitiesLimitsChanged(before.entitiesLimits, after.entitiesLimits) ||
            this.selectionInfoChanged(before.courses, after.courses) ||
            this.selectionInfoChanged(before.categories, after.categories) ||
            this.selectionInfoChanged(before.instructors, after.instructors);
    }

    private surveysChanged(type: string, before?: ReportManagerInfoSurveysFilter, after?: ReportManagerInfoSurveysFilter): boolean {
        if (type !== ReportsTypes.SURVEYS_INDIVIDUAL_ANSWERS.toString() || (before === undefined && after === undefined)) {
            return false;
        }
        return (before === undefined && after !== undefined) ||
            after === undefined ||
            this.booleanChanged(before.all, after.all) ||
            this.numberChanged(before.entitiesLimits, after.entitiesLimits) ||
            this.selectionInfoChanged(before.surveys, after.surveys);
    }

    private learningPlansChanged(type: string, before?: ReportManagerLearningPlansFilter, after?: ReportManagerLearningPlansFilter): boolean {
        if (![ReportsTypes.USERS_LP.toString(), ReportsTypes.ECOMMERCE_TRANSACTION.toString(),
            ReportsTypes.GROUPS_COURSES.toString(), ReportsTypes.SESSIONS_USER_DETAIL.toString(),
            ReportsTypes.SURVEYS_INDIVIDUAL_ANSWERS.toString(), ReportsTypes.USERS_CLASSROOM_SESSIONS.toString(),
            ReportsTypes.USERS_COURSES.toString(), ReportsTypes.USERS_ENROLLMENT_TIME.toString(),
            ReportsTypes.USERS_LEARNINGOBJECTS.toString(), ReportsTypes.USERS_WEBINAR.toString(),
            ReportsTypes.COURSES_USERS.toString()].includes(type)) {
            return false;
        }
        if (before === undefined && after === undefined) {
            return false;
        }
        if ((before === undefined && after !== undefined) ||
            after === undefined ||
            this.booleanChanged(before.all, after.all) ||
            this.numberChanged(before.entitiesLimits, after.entitiesLimits) ||
            before.learningPlans.length !== after.learningPlans.length) {
            return true;
        }
        const lpBefore = before.learningPlans.map(item => item.id);
        const lpAfter = after.learningPlans.map(item => item.id);
        const deletedLp = lpBefore.filter(item => lpAfter.indexOf(item) < 0);
        const addedLp = lpAfter.filter(item => lpBefore.indexOf(item) < 0);

        return deletedLp.length > 0 || addedLp.length > 0;
    }

    private badgesChanged(type: string, before?: ReportManagerBadgesFilter, after?: ReportManagerBadgesFilter): boolean {
        if (type !== ReportsTypes.USERS_BADGES.toString() || (before === undefined && after === undefined)) {
            return false;
        }
        return (before === undefined && after !== undefined) ||
            after === undefined ||
            this.booleanChanged(before.all, after.all) ||
            this.numberChanged(before.entitiesLimits, after.entitiesLimits) ||
            this.selectionInfoChanged(before.badges, after.badges);
    }

    private assetsChanged(type: string, before?: ReportManagerAssetsFilter, after?: ReportManagerAssetsFilter): boolean {
        if (![ReportsTypes.ASSETS_STATISTICS.toString(), ReportsTypes.VIEWER_ASSET_DETAILS.toString()].includes(type)) {
            return false;
        }
        if (before === undefined && after === undefined) {
            return false;
        }
        return (before === undefined && after !== undefined) ||
            after === undefined ||
            this.booleanChanged(before.all, after.all) ||
            this.assetsEntitiesLimitsChanged(before.entitiesLimits, after.entitiesLimits) ||
            this.selectionInfoChanged(before.assets, after.assets) ||
            this.selectionInfoChanged(before.channels, after.channels);
    }

    private enrollmentChanged(type: string, before: Enrollment, after: Enrollment): boolean {
        if (![ReportsTypes.USERS_CLASSROOM_SESSIONS.toString(), ReportsTypes.USERS_COURSES.toString(),
            ReportsTypes.USERS_LP.toString(), ReportsTypes.USERS_WEBINAR.toString()].includes(type)) {
            return false;
        }
        if (before === undefined && after === undefined) {
            return false;
        }
        return (before === undefined && after !== undefined) ||
            after === undefined ||
            this.booleanChanged(before.enrollmentsToConfirm, after.enrollmentsToConfirm) ||
            this.booleanChanged(before.completed, after.completed) ||
            this.booleanChanged(before.inProgress, after.inProgress) ||
            this.booleanChanged(before.notStarted, after.notStarted) ||
            this.booleanChanged(before.overbooking, after.overbooking) ||
            this.booleanChanged(before.subscribed, after.subscribed) ||
            this.booleanChanged(before.suspended, after.suspended) ||
            this.booleanChanged(before.waitingList, after.waitingList) ||
            before.enrollmentTypes !== after.enrollmentTypes;
    }

    private sessionDatesChanged(type: string, before?: SessionDates, after?: SessionDates): boolean {
        if (![ReportsTypes.USERS_CLASSROOM_SESSIONS.toString(), ReportsTypes.SESSIONS_USER_DETAIL.toString(), ReportsTypes.USERS_WEBINAR.toString()].includes(type)) {
            return false;
        }
        if (before === undefined && after === undefined) {
            return false;
        }
        return (before === undefined && after !== undefined) ||
            after === undefined ||
            this.stringChanged(before.conditions, after.conditions) ||
            this.dateOptionsChanged(before.startDate, after.startDate) ||
            this.dateOptionsChanged(before.endDate, after.endDate);
    }

    private instructorsChanged(type: string, before?: InstructorsFilter, after?: InstructorsFilter): boolean {
        if (type !== ReportsTypes.USERS_WEBINAR || (before === undefined && after === undefined)) {
            return false;
        }
        return (before === undefined && after !== undefined) ||
            after === undefined ||
            this.booleanChanged(before.all, after.all) ||
            this.selectionInfoChanged(before.instructors, after.instructors);
    }

    private certificationsChanged(type: string, before?: ReportManagerCertificationsFilter, after?: ReportManagerCertificationsFilter): boolean {
        if (![ReportsTypes.CERTIFICATIONS_USERS.toString(), ReportsTypes.USERS_CERTIFICATIONS.toString()].includes(type)) {
            return false;
        }
        if (before === undefined && after === undefined) {
            return false;
        }
        return (before === undefined && after !== undefined) ||
            after === undefined ||
            this.booleanChanged(before.all, after.all) ||
            this.booleanChanged(before.activeCertifications, after.activeCertifications) ||
            this.booleanChanged(before.archivedCertifications, after.archivedCertifications) ||
            this.booleanChanged(before.expiredCertifications, after.expiredCertifications) ||
            this.numberChanged(before.entitiesLimits, after.entitiesLimits) ||
            this.selectionInfoChanged(before.certifications, after.certifications) ||
            this.dateOptionsChanged(before.certificationDate, after.certificationDate) ||
            this.dateOptionsChanged(before.certificationExpirationDate, after.certificationExpirationDate) ||
            this.stringChanged(before.conditions, after.conditions);
    }

    private externalTrainingStatusChanged(type: string, before?: ExternalTrainingStatusFilter, after?: ExternalTrainingStatusFilter): boolean {
        if (type !== ReportsTypes.USERS_EXTERNAL_TRAINING.toString() || (before === undefined && after === undefined)) {
            return false;
        }
        return (before === undefined && after !== undefined) ||
            after === undefined ||
            this.booleanChanged(before.approved, after.approved) ||
            this.booleanChanged(before.rejected, after.rejected) ||
            this.booleanChanged(before.waiting, after.waiting);
    }

    private publishStatusChanged(type: string, before?: PublishStatusFilter, after?: PublishStatusFilter): boolean {
        if (type !== ReportsTypes.VIEWER_ASSET_DETAILS.toString() || (before === undefined && after === undefined)) {
            return false;
        }
        return (before === undefined && after !== undefined) ||
            after === undefined ||
            this.booleanChanged(before.published, after.published) ||
            this.booleanChanged(before.unpublished, after.unpublished);
    }

    private sessionAttendanceTypeChanged(type: string, before?: SessionAttendanceType, after?: SessionAttendanceType): boolean {
        if (![ReportsTypes.USERS_CLASSROOM_SESSIONS.toString(), ReportsTypes.SESSIONS_USER_DETAIL.toString()].includes(type)) {
            return false;
        }
        if (before === undefined && after === undefined) {
            return false;
        }
        return (before === undefined && after !== undefined) ||
            after === undefined ||
            this.booleanChanged(before.blended, after.blended) ||
            this.booleanChanged(before.flexible, after.flexible) ||
            this.booleanChanged(before.fullOnline, after.fullOnline) ||
            this.booleanChanged(before.fullOnsite, after.fullOnsite);
    }

    private sessionsChanged(type: string, before?: ReportManagerInfoSessionsFilter, after?: ReportManagerInfoSessionsFilter): boolean {
        if (![ReportsTypes.USERS_CLASSROOM_SESSIONS.toString(), ReportsTypes.SESSIONS_USER_DETAIL.toString()].includes(type)) {
            return false;
        }
        if (before === undefined && after === undefined) {
            return false;
        }
        return (before === undefined && after !== undefined) ||
            after === undefined ||
            this.booleanChanged(before.all, after.all) ||
            this.numberChanged(before.entitiesLimits, after.entitiesLimits) ||
            this.selectionInfoChanged(before.sessions, after.sessions);
    }

    private userAdditionalFieldsChanged(before?: { [key: number]: number; }, after?: { [key: number]: number; }): boolean {
        if (before === undefined && after === undefined) {
            return false;
        }
        if ((before === undefined && after !== undefined) || after === undefined) {
            return true;
        }
        const keys = [...new Set([...Object.keys(before), ...Object.keys(after)])];
        let differences = false;

        for (const key of keys) {
            if (this.numberChanged(before[key], after[key])) {
                differences = true;
                break;
            }
        }
        return differences;
    }

    private loTypesChanged(type: string, before?: {[key: string]: boolean}, after?: {[key: string]: boolean}): boolean {
        if (type !== ReportsTypes.USERS_LEARNINGOBJECTS.toString() || (before === undefined && after === undefined)) {
            return false;
        }
        if ((before === undefined && after !== undefined) || after === undefined) {
            return true;
        }
        const keys = [...new Set([...Object.keys(before), ...Object.keys(after)])];
        let differences = false;

        for (const key of keys) {
            if (before[key] !== after[key]) {
                differences = true;
                break;
            }
        }
        return differences;
    }

    private datesOptionsChanged(before: ReportManagerInfo, after: ReportManagerInfo): boolean {
        const dateOptionsFields = [
            'enrollmentDate',
            'surveyCompletionDate',
            'archivingDate',
            'courseExpirationDate',
            'issueDate',
            'creationDateOpts',
            'expirationDateOpts',
            'publishedDate',
            'contributionDate',
            'externalTrainingDate',
            'completionDate'
        ];
        return dateOptionsFields.some(field => {
            if (this.dateOptionsChanged(before[field], after[field])) {
                return true;
            }
        });
    }

    public propertiesChanged(before: ReportManagerInfo, after: ReportManagerInfo): Properties {
        const changes: Properties = {};
        if (this.stringChanged(before.title, after.title)) {
            changes.name = after.title;
        }
        if (this.stringChanged(before.description, after.description)) {
            changes.description = after.description;
        }
        if (this.booleanChanged(before.loginRequired, after.loginRequired)) {
            changes.login_required_download_report = after.loginRequired;
        }
        if (this.stringChanged(before.timezone, after.timezone)) {
            changes.timezone = after.timezone;
        }
        if (this.stringChanged(before.visibility.type.toString(), after.visibility.type.toString())) {
            switch (after.visibility.type) {
                case VisibilityTypes.ALL_GODADMINS_AND_PU:
                    changes.visibility_rules = 'All Superadmins and Power Users';
                    break;
                case VisibilityTypes.ALL_GODADMINS_AND_SELECTED_PU:
                    changes.visibility_rules = 'All Superadmins and some selected Power Users';
                    break;
                default:
                    changes.visibility_rules = 'All Superadmins';
                    break;
            }
        }

        return changes;
    }

    public viewOptionsChanged(before: ReportManagerInfo, after: ReportManagerInfo): boolean {
        if (before.fields.length !== after.fields.length) {
            return true;
        }
        const deletedFields = before.fields.filter(item => after.fields.indexOf(item) < 0);
        const addedFields = after.fields.filter(item => before.fields.indexOf(item) < 0);

        return deletedFields.length > 0 || addedFields.length > 0;
    }

    public filtersChanged(before: ReportManagerInfo, after: ReportManagerInfo): boolean {
        return this.publishStatusChanged(before.type, before.publishStatus, after.publishStatus)
            || this.surveysChanged(before.type, before.surveys, after.surveys)
            || this.instructorsChanged(before.type, before.instructors, after.instructors)
            || this.externalTrainingStatusChanged(before.type, before.externalTrainingStatusFilter, after.externalTrainingStatusFilter)
            || this.badgesChanged(before.type, before.badges, after.badges)
            || this.loTypesChanged(before.type, before.loTypes, after.loTypes)
            || this.sessionsChanged(before.type, before.sessions, after.sessions)
            || this.sessionAttendanceTypeChanged(before.type, before.sessionAttendanceType, after.sessionAttendanceType)
            || this.certificationsChanged(before.type, before.certifications, after.certifications)
            || this.assetsChanged(before.type, before.assets, after.assets)
            || this.sessionDatesChanged(before.type, before.sessionDates, after.sessionDates)
            || this.enrollmentChanged(before.type, before.enrollment, after.enrollment)
            || this.conditionsChanged(before.type, before.conditions, after.conditions)
            || this.learningPlansChanged(before.type, before.learningPlans, after.learningPlans)
            || this.coursesChanged(before.type, before.courses, after.courses)
            || this.userAdditionalFieldsChanged(before.userAdditionalFieldsFilter, after.userAdditionalFieldsFilter)
            || this.usersChanged(before.users, after.users)
            || this.datesOptionsChanged(before, after);
    }
}
