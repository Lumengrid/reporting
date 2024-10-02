import { ReportsTypes } from '../constants/report-types';
import { FieldsList, ReportManagerInfo } from '../../models/report-manager';
import { FieldNotEditableException } from '../exceptions/FieldNotEditableException';
import { MandatoryFieldNotFoundException } from '../exceptions/MandatoryFieldNotFoundException';
import { UserLevels } from '../../services/session/user-manager.session';
import { QUERY_BUILDER_FILTER_TYPE_TEXT } from '../../query-builder/models/query-builder';
import { JsonTextFilter } from '../../query-builder/interfaces/query-builder.interface';
import { EnrollmentTypes, TextFilterOptions, TimeFrameOptions, VisibilityTypes } from '../../models/custom-report';
import { InvalidFieldException } from '../exceptions/InvalidFieldException';
import {
    Conditions,
    DateOptionsFilter,
    Enrollment,
    InfoFilter,
    Operators,
    PlanningOption,
    ReportPatchInput,
    Selectors,
    TypeOperator
} from '../interfaces/patch.interface';
import { ReportException } from '../exceptions/ReportException';
import { CourseTypeFilter } from '../../models/base';
import { isOClockTime, isValidTimezone } from '../../shared/customValidators';

export class ReportValidation {
    public static FIELDS_NOT_EDITABLE: string[] = [
        'deleted',
        'queryBuilderId',
        'queryBuilderName',
        'author',
        'creationDate',
        'lastEdit',
        'lastEditBy',
        'standard',
        'type',
        'platform',
        'idReport',
        'isReportDownloadPermissionLink'
    ];

    private static DATE_OPTIONS_FILTERS: string[] = [
        'enrollmentDate', 'completionDate', 'surveyCompletionDate',
        'archivingDate', 'courseExpirationDate', 'issueDate',
        'creationDateOpts', 'expirationDateOpts', 'publishedDate',
        'contributionDate', 'externalTrainingDate',
    ];

    private static REPORT_TYPES_USERS_MANDATORY: string[] = [
        ReportsTypes.USERS,
        ReportsTypes.USERS_BADGES,
        ReportsTypes.USERS_CLASSROOM_SESSIONS,
        ReportsTypes.USERS_EXTERNAL_TRAINING,
        ReportsTypes.USERS_LP,
        ReportsTypes.LP_USERS_STATISTICS,
        ReportsTypes.USER_CONTRIBUTIONS,
        ReportsTypes.COURSES_USERS,
        ReportsTypes.ECOMMERCE_TRANSACTION,
        ReportsTypes.GROUPS_COURSES,
        ReportsTypes.USERS_COURSES,
        ReportsTypes.USERS_ENROLLMENT_TIME,
        ReportsTypes.USERS_LEARNINGOBJECTS,
        ReportsTypes.SESSIONS_USER_DETAIL,
        ReportsTypes.USERS_CERTIFICATIONS,
        ReportsTypes.USERS_WEBINAR
    ];

    private static REPORT_TYPES_COURSES_MANDATORY: string[] = [
        ReportsTypes.COURSES_USERS,
        ReportsTypes.ECOMMERCE_TRANSACTION,
        ReportsTypes.GROUPS_COURSES,
        ReportsTypes.USERS_COURSES,
        ReportsTypes.USERS_ENROLLMENT_TIME,
        ReportsTypes.USERS_LEARNINGOBJECTS,
        ReportsTypes.SESSIONS_USER_DETAIL,
        ReportsTypes.USERS_WEBINAR
    ];

    private static checkNotEditableFields(infoBefore: ReportManagerInfo, data: ReportManagerInfo | ReportPatchInput): void {
        this.FIELDS_NOT_EDITABLE.map((field) => {
            if (data[field] !== undefined && infoBefore[field] !== data[field]) {
                throw new FieldNotEditableException(field);
            }
        });
    }

    public static checkLoginRequired(reportInfo: ReportManagerInfo, data: ReportManagerInfo | ReportPatchInput): void {
        if (data?.loginRequired !== undefined && data.loginRequired !== reportInfo.loginRequired) {
            throw new FieldNotEditableException('loginRequired');
        }
    }

    private static isValidEmail(email: string): boolean {
        const emailRegex = /^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$/;
        return emailRegex.test(email);
    }

    private static isValidDate(dateString: string): boolean {
        try {
            const date = new Date(dateString);
            return !isNaN(date.getTime());
        } catch (ex) {
            return false;
        }
    }

    private static validateField(name: string, value: any, expectedType: string, allowedValues?: any[]): boolean {
        if (value === undefined) {
            return true;
        }
        if (value === null) {
            throw new InvalidFieldException(name);
        }
        try {
            let valid: boolean;
            switch (expectedType) {
                case 'string':
                case 'boolean':
                case 'number':
                    valid = typeof value === expectedType;
                    break;
                case 'numberGreaterThanZero':
                    valid = typeof value === 'number' && Number(value) > 0;
                    break;
                case 'date':
                    valid = typeof value === 'string' && this.isValidDate(value);
                    break;
                case 'timezone':
                    valid = typeof value === 'string' && isValidTimezone(value);
                    break;
                case 'time':
                    valid = typeof value === 'string' && isOClockTime(value);
                    break;
                case 'object':
                    valid = typeof value === 'object' && !Array.isArray(value);
                    break;
                case 'string[]':
                    valid = Array.isArray(value) && value.every(item => typeof item === 'string');
                    break;
                case 'emails[]':
                    valid = Array.isArray(value) && value.every(item => typeof item === 'string' && this.isValidEmail(item));
                    break;
                case 'userAdditionalFieldsFilter':
                    valid = Object.keys(value).every(key => !isNaN(Number(key)) && typeof value[Number(key)] === 'number');
                    break;
                case 'loTypes':
                    valid = Object.keys(value).every(key => typeof value[key] === 'boolean');
                    break;
                case 'InfoFilter':
                    valid = Array.isArray(value) && value.every(item => {
                        return typeof item.id === 'number' &&
                            (typeof item.descendants === 'undefined' || typeof item.descendants === 'boolean');
                    });
                    break;
                case 'PlanningOption':
                    const planningOption = value as PlanningOption;
                    valid = typeof value === 'object' &&
                        !Array.isArray(value) &&
                        this.validateField(name + '.recipients', planningOption.recipients, 'emails[]') &&
                        this.validateField(name + '.every', planningOption.every, 'numberGreaterThanZero') &&
                        this.validateField(name + '.timeFrame', planningOption.timeFrame, 'string',
                            [TimeFrameOptions.days, TimeFrameOptions.weeks, TimeFrameOptions.months]) &&
                        this.validateField(name + '.scheduleFrom', planningOption.scheduleFrom, 'date') &&
                        this.validateField(name + '.startHour', planningOption.startHour, 'time') &&
                        this.validateField(name + '.timezone', planningOption.timezone, 'timezone');
                    break;
                case 'Enrollment':
                    const enrollment = value as Enrollment;
                    valid = typeof value === 'object' &&
                        !Array.isArray(value) &&
                        this.validateField(name + '.completed', enrollment.completed, 'boolean') &&
                        this.validateField(name + '.inProgress', enrollment.inProgress, 'boolean') &&
                        this.validateField(name + '.notStarted', enrollment.notStarted, 'boolean') &&
                        this.validateField(name + '.waitingList', enrollment.waitingList, 'boolean') &&
                        this.validateField(name + '.suspended', enrollment.suspended, 'boolean') &&
                        this.validateField(name + '.enrollmentsToConfirm', enrollment.enrollmentsToConfirm, 'boolean') &&
                        this.validateField(name + '.subscribed', enrollment.subscribed, 'boolean') &&
                        this.validateField(name + '.overbooking', enrollment.overbooking, 'boolean') &&
                        this.validateField(name + '.enrollmentTypes', enrollment.enrollmentTypes, 'number',
                            [EnrollmentTypes.active, EnrollmentTypes.activeAndArchived, EnrollmentTypes.archived]);
                    break;
                case 'DateOptionsFilter':
                    const dateFilter = value as DateOptionsFilter;
                    valid = typeof value === 'object' &&
                        !Array.isArray(value) &&
                        this.validateField(name + '.any', dateFilter.any, 'boolean') &&
                        this.validateField(name + '.operator', dateFilter.operator, 'string', Object.values(Operators)) &&
                        this.validateField(name + '.type', dateFilter.type, 'string', Object.values(TypeOperator)) &&
                        this.validateField(name + '.from', dateFilter.from, 'date') &&
                        this.validateField(name + '.to', dateFilter.to, 'date') &&
                        this.validateField(name + '.days', dateFilter.days, 'number');
                    break;
                default:
                    valid = true;
                    break;
            }
            if (valid && ['number', 'string'].includes(expectedType) && allowedValues !== undefined && !allowedValues.includes(value)) {
                valid = false
            }
            if (!valid) {
                throw new InvalidFieldException(name);
            }

            return valid;
        } catch (ex) {
            throw new InvalidFieldException(name);
        }
    }

    private static isExtraField(field: string): boolean {
        const extraFields = [
            'course_extrafield_',
            'courseuser_extrafield_',
            'user_extrafield_',
            'classroom_extrafield_',
            'external_activity_extrafield_',
            'lp_extrafield_',
            'webinar_extrafield_'
        ];
        const checks = extraFields.filter((item) => {
            if (field.startsWith(item) && parseInt(field.replace(item, ''), 10) >= 0) {
                return item;
            }
        });

        return checks.length > 0;
    }

    private static validateFieldViews(reportInfo: ReportManagerInfo, input: ReportPatchInput): void {
        if (reportInfo.type === ReportsTypes.QUERY_BUILDER_DETAIL) {
            return;
        }
        const fields = input.fields !== undefined ? input.fields : (reportInfo.fields ?? []);
        const sortingField = input.sortingOptions?.selectedField !== undefined
            ? input.sortingOptions?.selectedField
            : (reportInfo.sortingOptions?.selectedField ?? '');


        fields.map((field) => {
            if (!Object.values(FieldsList).includes(field as FieldsList) && !this.isExtraField(field)) {
                throw new InvalidFieldException('fields.' + field);
            }
        });
        if (!fields.includes(sortingField)) {
            throw new InvalidFieldException('sortingOptions.selectedField');
        }
    }

    private static validatePatchInput(input: ReportPatchInput): void {
        try {
            const validations: [string, any, string, any[]?][] = [
                ['loginRequired', input.loginRequired, 'boolean'],
                ['description', input.description, 'string'],
                ['timezone', input.timezone, 'timezone'],
                ['title', input.title, 'string'],
                ['fields', input.fields, 'string[]'],
                ['conditions', input.conditions, 'string', Object.values(Conditions)],
                ['userAdditionalFieldsFilter', input.userAdditionalFieldsFilter, 'userAdditionalFieldsFilter'],
                ['loTypes', input.loTypes, 'loTypes'],
                ['visibility', input.visibility, 'object'],
                ['visibility.type', input.visibility?.type, 'number', Object.values(VisibilityTypes)],
                ['visibility.groups', input.visibility?.groups, 'InfoFilter'],
                ['visibility.users', input.visibility?.users, 'InfoFilter'],
                ['visibility.branches', input.visibility?.branches, 'InfoFilter'],
                ['planning', input.planning, 'object'],
                ['planning.active', input.planning?.active, 'boolean'],
                ['planning.option', input.planning?.option, 'PlanningOption'],
                ['sortingOptions', input.sortingOptions, 'object'],
                ['sortingOptions.selector', input.sortingOptions?.selector, 'string', Object.values(Selectors)],
                ['sortingOptions.selectedField', input.sortingOptions?.selectedField, 'string'],
                ['sortingOptions.orderBy', input.sortingOptions?.orderBy, 'string', ['desc', 'asc']],
                ['users.hideDeactivated', input.users?.hideDeactivated, 'boolean'],
                ['users.showOnlyLearners', input.users?.showOnlyLearners, 'boolean'],
                ['users.hideExpiredUsers', input.users?.hideExpiredUsers, 'boolean'],
                ['users.isUserAddFields', input.users?.isUserAddFields, 'boolean'],
                ['users.groups', input.users?.groups, 'InfoFilter'],
                ['users.branches', input.users?.branches, 'InfoFilter'],
                ['courses.categories', input.courses?.categories, 'InfoFilter'],
                ['courses.instructors', input.courses?.instructors, 'InfoFilter'],
                ['courses.courseType', input.courses?.courseType, 'number', Object.values(CourseTypeFilter)],
                ['assets.channels', input.assets?.channels, 'InfoFilter'],
                ['enrollment', input.enrollment, 'Enrollment'],
                ['certifications', input.certifications, 'object'],
                ['certifications.activeCertifications', input.certifications?.activeCertifications, 'boolean'],
                ['certifications.expiredCertifications', input.certifications?.expiredCertifications, 'boolean'],
                ['certifications.archivedCertifications', input.certifications?.archivedCertifications, 'boolean'],
                ['certifications.certificationDate', input.certifications?.certificationDate, 'DateOptionsFilter'],
                ['certifications.certificationExpirationDate', input.certifications?.certificationExpirationDate, 'DateOptionsFilter'],
                ['certifications.conditions', input.certifications?.conditions, 'string', Object.values(Conditions)],
                ['sessionDates', input.sessionDates, 'object'],
                ['sessionDates.startDate', input.sessionDates?.startDate, 'DateOptionsFilter'],
                ['sessionDates.endDate', input.sessionDates?.endDate, 'DateOptionsFilter'],
                ['sessionDates.conditions', input.sessionDates?.conditions, 'string', Object.values(Conditions)],
                ['externalTrainingStatusFilter', input.externalTrainingStatusFilter, 'object'],
                ['externalTrainingStatusFilter.approved', input.externalTrainingStatusFilter?.approved, 'boolean'],
                ['externalTrainingStatusFilter.waiting', input.externalTrainingStatusFilter?.waiting, 'boolean'],
                ['externalTrainingStatusFilter.rejected', input.externalTrainingStatusFilter?.rejected, 'boolean'],
                ['publishStatus', input.publishStatus, 'object'],
                ['publishStatus.published', input.publishStatus?.published, 'boolean'],
                ['publishStatus.unpublished', input.publishStatus?.unpublished, 'boolean'],
                ['sessionAttendanceType', input.sessionAttendanceType, 'object'],
                ['sessionAttendanceType.blended', input.sessionAttendanceType?.blended, 'boolean'],
                ['sessionAttendanceType.fullOnsite', input.sessionAttendanceType?.fullOnsite, 'boolean'],
                ['sessionAttendanceType.fullOnline', input.sessionAttendanceType?.fullOnline, 'boolean'],
                ['sessionAttendanceType.flexible', input.sessionAttendanceType?.flexible, 'boolean'],
            ];
            validations.map(([name, value, expectedType, allowedValues]) => {
                this.validateField(name, value, expectedType, allowedValues)
            });

            this.DATE_OPTIONS_FILTERS.map((field) => this.validateField(field, input[field], 'DateOptionsFilter'));

            const genericFilters = [
                'users', 'courses', 'surveys',
                'learningPlans', 'badges', 'assets',
                'sessions', 'instructors', 'certifications'
            ];
            genericFilters.map((field) => {
                const value = input[field];
                if (value !== undefined) {
                    this.validateField(field, value, 'object');
                    this.validateField(field + '.all', value['all'], 'boolean');
                    this.validateField(field + '.' + field, value[field], 'InfoFilter');
                }
            });
        } catch (ex) {
            if (ex instanceof ReportException) {
                throw ex;
            }
            throw new InvalidFieldException('Generic error on validation');
        }
    }

    public static validate(reportInfo: ReportManagerInfo,
                           userLevel: string,
                           isReportDownloadPermissionLinkEnable: boolean,
                           patch: boolean,
                           data: ReportManagerInfo | ReportPatchInput): void {
        if (data.platform !== undefined && reportInfo.platform !== data.platform) {
            throw new InvalidFieldException('platform');
        }
        if (userLevel === UserLevels.POWER_USER && !isReportDownloadPermissionLinkEnable) {
            ReportValidation.checkLoginRequired(reportInfo, data);
        }
        if (!patch) {
            return;
        }

        this.checkNotEditableFields(reportInfo, data);
        this.validatePatchInput(data as ReportPatchInput);
        this.validateFieldViews(reportInfo, data as ReportPatchInput);
    }

    public static checkFilters(reportInfo: ReportManagerInfo): void {
        if (reportInfo.visibility.type === VisibilityTypes.ALL_GODADMINS_AND_SELECTED_PU &&
            (!reportInfo.visibility.users || reportInfo.visibility.users.length === 0) &&
            (!reportInfo.visibility.branches || reportInfo.visibility.branches.length === 0) &&
            (!reportInfo.visibility.groups || reportInfo.visibility.groups.length === 0)) {
            throw new InvalidFieldException('visibility');
        }

        const validations: [string, any, string[]?][] = [
            ['users', reportInfo.users, ['groups', 'branches']],
            ['courses', {
                ...reportInfo.courses,
                'learningPlans': reportInfo.learningPlans?.learningPlans ?? []
            }, ['learningPlans']],
            ['surveys', reportInfo.surveys],
            ['learningPlans', reportInfo.learningPlans],
            ['badges', reportInfo.badges],
            ['assets', reportInfo.assets, ['channels']],
            ['sessions', reportInfo.sessions],
            ['instructors', reportInfo.instructors],
            ['certifications', reportInfo.certifications],
        ];

        validations.map(([field, value, additionalFilters]) => {
            if (value !== undefined && value.all === false) {
                let filters = [field];
                if (additionalFilters !== undefined) {
                    filters = [
                        field,
                        ...additionalFilters
                    ];
                }
                const filtered = filters.filter((filter) => value[filter] !== undefined &&
                    Array.isArray(value[filter]) &&
                    value[filter].length > 0);
                if (filtered === undefined || filtered.length === 0) {
                    throw new InvalidFieldException(field + '.all');
                }
            }
        });
    }

    public static checkDateOptions(reportInfo: ReportManagerInfo): void {
        this.DATE_OPTIONS_FILTERS.map((filter) => {
            if (reportInfo[filter] !== undefined && reportInfo[filter] !== null) {
                const value = reportInfo[filter] as DateOptionsFilter;
                this.validateField(filter + '.any', value.any, 'boolean');
                if (!value.any) {
                    this.validateField(filter + '.operator', value.operator, 'string', Object.values(Operators));
                    switch (value.operator) {
                        case 'isAfter':
                        case 'isBefore':
                            this.validateField(filter + '.type', value.type, 'string', [TypeOperator.relative, TypeOperator.absolute]);
                            if (value.type === TypeOperator.relative) {
                                this.validateField(filter + '.days', value.days, 'number');
                            } else {
                                this.validateField(filter + '.to', value.to, 'date');
                            }
                            break
                        case 'range':
                            this.validateField(filter + '.from', value.from, 'date');
                            this.validateField(filter + '.to', value.to, 'date');
                            const from = new Date(value.from).setHours(0, 0, 0);
                            const to = new Date(value.to).setHours(23, 59, 59);
                            if (from > to) {
                                throw new InvalidFieldException(filter + '.from');
                            }
                            break;
                        case 'expiringIn':
                            this.validateField(filter + '.days', value.days, 'number');
                            break;
                    }
                }
            }
        });
    }

    public static checkEnrollment(reportInfo: ReportManagerInfo): void {
        if (reportInfo.enrollment === undefined ||
            reportInfo.enrollment.completed ||
            reportInfo.enrollment.inProgress ||
            reportInfo.enrollment.notStarted ||
            reportInfo.enrollment.waitingList ||
            reportInfo.enrollment.suspended ||
            reportInfo.enrollment.enrollmentsToConfirm ||
            reportInfo.enrollment.subscribed ||
            reportInfo.enrollment.overbooking) {
            return;
        }

        throw new InvalidFieldException('enrollment');
    }

    public static checkMandatoryFields(isDatalakeV2Active: boolean, reportInfo: ReportManagerInfo): void {
        if (reportInfo.title === '' || !reportInfo.title) {
            throw new MandatoryFieldNotFoundException('title');
        }
        if (!reportInfo.author) {
            throw new MandatoryFieldNotFoundException('author');
        }
        if (!reportInfo.fields || reportInfo.fields.length === 0) {
            throw new MandatoryFieldNotFoundException('fields');
        }
        if (!reportInfo.visibility) {
            throw new MandatoryFieldNotFoundException('visibility');
        }
        if (reportInfo.planning?.active === true &&
            (reportInfo.planning?.option?.recipients === undefined || reportInfo.planning.option?.recipients.length === 0)) {
            throw new MandatoryFieldNotFoundException('planning.option.recipients');
        }
        if (!isDatalakeV2Active) {
            return;
        }
        if (!reportInfo.planning.option?.startHour || reportInfo.planning.option.startHour === '' || reportInfo.planning.option.startHour === null) {
            throw new MandatoryFieldNotFoundException('planning.option.startHour');
        }
    }

    public static checkMandatoryFieldsForSpecificReport(reportInfo: ReportManagerInfo): void {
        if (!reportInfo.users && this.REPORT_TYPES_USERS_MANDATORY.includes(reportInfo.type)) {
            throw new MandatoryFieldNotFoundException('users');
        }
        if (!reportInfo.courses && this.REPORT_TYPES_COURSES_MANDATORY.includes(reportInfo.type)) {
            throw new MandatoryFieldNotFoundException('courses');
        }
        switch (reportInfo.type) {
            case ReportsTypes.CERTIFICATIONS_USERS:
            case ReportsTypes.USERS_CERTIFICATIONS:
                if (!reportInfo.certifications) {
                    throw new MandatoryFieldNotFoundException('certifications');
                }
                break;
            case ReportsTypes.USERS_WEBINAR:
                if (!reportInfo.instructors) {
                    throw new MandatoryFieldNotFoundException('instructors');
                }
                break;
            case ReportsTypes.ASSETS_STATISTICS:
            case ReportsTypes.VIEWER_ASSET_DETAILS:
                if (!reportInfo.assets) {
                    throw new MandatoryFieldNotFoundException('assets');
                }
                break;
            case ReportsTypes.QUERY_BUILDER_DETAIL:
                if (typeof reportInfo.queryBuilderFilters !== undefined && reportInfo.queryBuilderFilters) {
                    const jsonObject = reportInfo.queryBuilderFilters;
                    Object.keys(reportInfo.queryBuilderFilters).forEach(filterName => {
                        const filter = jsonObject[filterName];
                        if (filter.type === QUERY_BUILDER_FILTER_TYPE_TEXT) {
                            const jsonFilterText = filter as JsonTextFilter;
                            if ((jsonFilterText.any === false && !jsonFilterText.operator) ||
                                (jsonFilterText.any === false && !(<any>Object).values(TextFilterOptions).includes(jsonFilterText.operator))) {
                                throw new InvalidFieldException(`operator for filter ${filterName}`);
                            }
                        }
                    });
                }
        }
    }
}
