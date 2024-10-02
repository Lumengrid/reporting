import httpContext from 'express-http-context';
import moment from 'moment';
import fs from 'fs';
import slugify from 'slugify';
import v5 = require('uuid/v5');
import SessionManager from '../services/session/session-manager.session';
import { SessionLoggerService } from '../services/logger/session-logger.service';
import { ReportService } from '../services/report';
import { DynamoReport } from '../services/dynamo';
import { UserLevels } from '../services/session/user-manager.session';
import {
    QUERY_BUILDER_FILTER_TYPE_COURSES,
    QUERY_BUILDER_FILTER_TYPE_TEXT,
    QUERY_BUILDER_FILTER_TYPE_USERS
} from '../query-builder/models/query-builder';
import {
    CustomReportType,
    JsonCourseFilter,
    JsonTextFilter,
    JsonUserFilter
} from '../query-builder/interfaces/query-builder.interface';
import { ReportsTypes } from '../reports/constants/report-types';
import {
    CourseExtraFields,
    CourseuserExtraFields,
    LearningPlanExtraFields,
    TranscriptsExtraFields,
    Translations,
    UserExtraFields,
    UserExtraFieldsResponse,
    UserProps,
    UserPropsResponse
} from '../services/hydra';
import { BadRequestException } from '../exceptions/bad-request.exception';
import {
    DateOptions,
    DateOptionsValueDescriptor,
    Enrollment,
    ExternalTrainingStatusFilter,
    SessionAttendanceType,
    Planning,
    PublishStatusFilter,
    SortingOptions,
    TextFilterOptions,
    TimeFrameOptions,
    VisibilityTypes,
    EnrollmentTypes
} from './custom-report';
import {
    AttendancesTypes,
    CourseuserLevels,
    UserLevelsGroups,
    joinedTables,
    CourseTypes,
    EnrollmentStatuses,
    AdditionalFieldsTypes,
    SessionEvaluationStatus,
    ExternalTrainingStatus,
    ScoresTypes,
    LOStatus,
    LOTypes,
    LOQuestTypes,
    AssignmentTypes
} from './base';
import { FieldsDescriptor, LegacyOrderByParsed, LegacyReport, VisibilityRule } from './migration-component';
import {
    AthenaFieldsDataResponse,
    fieldListTypeString,
    FieldsList,
    FieldTranslation,
    fieldTranslationsKey,
    MassDeleteResponse,
    ReportAvailablesFields,
    ReportChannelsFilterRecover,
    ReportField,
    ReportFieldsArray,
    ReportManagerDataResponse,
    ReportManagerInfo,
    ReportManagerInfoResponse,
    ReportManagerInfoSessionsFilter,
    ReportManagerInfoVisibility,
    TablesList,
    TablesListAliases
} from './report-manager';
import { SidekiqScheduler } from './sidekiq-scheduler';
import { ReportCheckChanges } from '../reports/components/ReportCheckChanges';
import { loggerFactory } from '../services/logger/logger-factory';
import { ReportId } from '../reports/value_objects/ReportId';
import { ReportUpdate } from '../reports/components/ReportUpdate';
import { ReportClone } from '../reports/components/ReportClone';

export abstract class BaseReportManager {
    info: ReportManagerInfo;

    athenaUserAdditionalFields?: number[];
    athenaCourseAdditionalFields?: number[];
    athenaEnrollmentAdditionalFields?: number[];
    athenaTranscriptsAdditionalFields?: number[];

    session: SessionManager;
    logger: SessionLoggerService;
    reportService: ReportService;

    puUserTable: string;
    puCourseTable: string;
    puUserBranchesTable: string;
    managerSubordinatesTable: string;
    puApiPageSize: number;
    querySorting: string;
    querySelect: string[];
    allFields: ReportFieldsArray;

    public constructor(session: SessionManager, reportDetails?: DynamoReport) {
        if (reportDetails !== undefined) {
            this.info = reportDetails as ReportManagerInfo;
        } else {
            this.info = new ReportManagerInfo();
        }

        this.session = session;
        this.logger = httpContext.get('logger');
        this.reportService = new ReportService(session.getHydra());
        this.puUserTable = '';
        this.puApiPageSize = 500000;
        this.managerSubordinatesTable = '';
        this.puCourseTable = '';
        this.puUserBranchesTable = '';
        this.querySorting = '';
        this.allFields = {};
    }

    /**
     * This function make a general check on the common reports fields and make general operation on them
     *
     * @param info The data of the report to load
     */
    public loadInfo(info: ReportManagerInfo) {
        // Those fields must be keep always the same
        if (info.idReport !== this.info.idReport) {
            throw(new Error('Report ID not coherent.'));
        }
        if (info.platform !== this.info.platform) {
            throw(new Error('Report platform not coherent.'));
        }
        if (info.type !== this.info.type) {
            throw(new Error('Report type not coherent.'));
        }
        if (info.deleted !== this.info.deleted) {
            throw(new Error('Report deletion status not coherent.'));
        }

        // Check base report mandatory fields
        if (info.title === '' || !info.title) {
            throw(new Error('Missing mandatory field title'));
        }
        if (!info.author) {
            throw(new Error('Missing mandatory field author'));
        }
        if (!info.fields) {
            throw(new Error('Missing mandatory field fields'));
        }
        if (!info.lastEditBy) {
            throw(new Error('Missing mandatory field lastEditBy'));
        }
        if (!info.visibility) {
            throw(new Error('Missing mandatory field visibility'));
        }
        if (info.planning.active && info.planning.option) {
            info.planning.option.hostname = this.session.getHydra().getHostname();
            info.planning.option.subfolder = this.session.getHydra().getSubfolder();
        }

        this.info = info;
    }

    public async getInfo(data?: ReportManagerInfo): Promise<ReportManagerInfoResponse> {
        const response = new ReportManagerInfoResponse();
        const entitiesLimits = this.session.platform.getEntitiesLimits();

        if (typeof data !== 'undefined') {
            response.data = data;
        } else {
            response.data = this.info;
        }

        if (response.data.fields.length > 0) {
            const index = response.data.fields.indexOf(FieldsList.USER_BRANCHES);
            if (index >= 0) {
                const indexPath = response.data.fields.indexOf(FieldsList.USER_BRANCH_PATH);
                if (indexPath >= 0) {
                    response.data.fields.slice(index, 1);
                } else {
                    response.data.fields[index] = FieldsList.USER_BRANCH_PATH;
                }
            }
        }
        response.data.timezone = this.info.timezone ? this.info.timezone : this.session.user.getTimezone();
        // set a default value of true if loginRequired is not defined
        if (response.data.loginRequired !== false) {
            response.data.loginRequired = true;
        }

        // The value is taken from advanced settings -> advanced -> "Report Download Permission from Link"
        // Refers only to PU
        if (this.session.user.getLevel() === UserLevels.POWER_USER) {
            response.data.isReportDownloadPermissionLink = this.session.platform.getReportDownloadPermissionLink();
        } else {
            response.data.isReportDownloadPermissionLink = false;
        }

        let usersToRetrieve: number[] = [];
        let groupsToRetrieve: number[] = [];
        let branchesToRetrieve: number[] = [];
        let coursesToRetrieve: number[] = [];
        let learningPlansToRetrieve: number[] = [];
        let needAllLoTypes = false;
        let certificationsToRetrieve: number[] = [];
        let badgesToRetrieve: number[] = [];
        let assetsToRetrieve: number[] = [];
        let channelsToRetrieve: number[] = [];
        let courseCategoriesToRetrieve: number[] = [];
        let courseInstructorsToRetrieve: number[] = [];
        let sessionsToRetrieve: number[] = [];
        let surveysToRetrieve: number[] = [];


        if (typeof response.data.visibility !== 'undefined') {
            usersToRetrieve = usersToRetrieve.concat(response.data.visibility.users.map(a => a.id));
            groupsToRetrieve = groupsToRetrieve.concat(response.data.visibility.groups.map(a => a.id));
            branchesToRetrieve = branchesToRetrieve.concat(response.data.visibility.branches.map(a => a.id));
        }

        if (typeof response.data.queryBuilderFilters !== undefined && response.data.queryBuilderFilters) {
            const jsonObject = response.data.queryBuilderFilters;
            Object.keys(response.data.queryBuilderFilters).forEach(filterName => {
                const filter = jsonObject[filterName];
                if (filter.type === QUERY_BUILDER_FILTER_TYPE_USERS) {
                    usersToRetrieve = Array.from(new Set(usersToRetrieve.concat(filter.users.map(a => a.id))));
                    groupsToRetrieve = Array.from(new Set(groupsToRetrieve.concat(filter.groups.map(a => a.id))));
                    branchesToRetrieve = Array.from(new Set(branchesToRetrieve.concat(filter.branches.map(a => a.id))));
                } else if (filter.type === QUERY_BUILDER_FILTER_TYPE_COURSES) {
                    const jsonFilterCourse = filter as JsonCourseFilter;
                    coursesToRetrieve = Array.from(new Set(coursesToRetrieve.concat(jsonFilterCourse.courses.map(a => a.id))));
                    if (jsonFilterCourse.categories) {
                        courseCategoriesToRetrieve = Array.from(new Set(courseCategoriesToRetrieve.concat(jsonFilterCourse.categories.map(a => a.id))));
                    }

                    learningPlansToRetrieve = learningPlansToRetrieve.concat(jsonFilterCourse.learningPlans.map(a => {
                        a.id = parseInt(a.id as string, 10);

                        return a.id;
                    }));
                }
            });
        }

        if (typeof response.data.users !== 'undefined') {
            usersToRetrieve = usersToRetrieve.concat(response.data.users.users.map(a => a.id));
            groupsToRetrieve = groupsToRetrieve.concat(response.data.users.groups.map(a => a.id));
            branchesToRetrieve = branchesToRetrieve.concat(response.data.users.branches.map(a => a.id));
        }

        if (typeof response.data.courses !== 'undefined') {
            coursesToRetrieve = coursesToRetrieve.concat(response.data.courses.courses.map(a => a.id));
        }

        if (response.data.courses && response.data.courses.categories) {
            courseCategoriesToRetrieve = courseCategoriesToRetrieve.concat(response.data.courses.categories.map(c => c.id));
        }

        if (response.data.courses && response.data.courses.instructors) {
            courseInstructorsToRetrieve = courseInstructorsToRetrieve.concat(response.data.courses.instructors.map(c => c.id));
        }

        if (typeof response.data.surveys !== 'undefined') {
            surveysToRetrieve = Array.from(new Set(response.data.surveys.surveys.map(a => a.id)));
        }

        if (typeof response.data.learningPlans !== 'undefined') {
            learningPlansToRetrieve = learningPlansToRetrieve.concat(response.data.learningPlans.learningPlans.map(a => {
                a.id = parseInt(a.id as string, 10);

                return a.id;
            }));
        }

        if (typeof response.data.sessions !== 'undefined') {
            sessionsToRetrieve = sessionsToRetrieve.concat(response.data.sessions.sessions.map(a => a.id));
            response.data.sessions.entitiesLimits = entitiesLimits.classrooms.sessionLimit;
        }

        // update the lo types only for the USERS_LEARNINGOBJECTS report
        if (response.data.type === ReportsTypes.USERS_LEARNINGOBJECTS) {
            needAllLoTypes = !needAllLoTypes;
        }

        if (response.data.certifications && response.data.certifications.certifications.length !== 0) {
            certificationsToRetrieve = response.data.certifications.certifications.map(c => c.id);
        }

        if (response.data.badges && response.data.badges.badges.length !== 0) {
            badgesToRetrieve = response.data.badges.badges.map(b => b.id);
        }
        if (response.data.assets && response.data.assets.assets.length !== 0) {
            assetsToRetrieve = response.data.assets.assets.map(a => a.id);
        }
        if (response.data.assets && response.data.assets.channels.length !== 0) {
            channelsToRetrieve = response.data.assets.channels.map(c => c.id);
        }

        const hydra = this.session.getHydra();
        const [usersInfo, groupsInfo, branchesInfo, coursesInfo, learningPlansInfo, sessionsInfo, userPropsResponse, allLoTypes, certificationsDetails, badgesDetails] = await Promise.all([
            hydra.getUsers(usersToRetrieve),
            hydra.getGroups(groupsToRetrieve),
            hydra.getBranches(branchesToRetrieve),
            hydra.getCourses(coursesToRetrieve),
            hydra.getLearningPlans(learningPlansToRetrieve),
            hydra.getSessions(sessionsToRetrieve),
            hydra.getUserProps(this.info.lastEditBy.idUser as number).catch(error => {
                this.logger.errorWithStack(`Last edit user not found from Hydra API`, error);
                const anonymousUser: UserPropsResponse = {
                    data: {
                        idUser: 0,
                        avatar: '',
                        firstname: '',
                        lastname: '',
                        username: '',
                    }
                };

                return anonymousUser;
            }),
            needAllLoTypes ? hydra.getAllLOTypes() : undefined,
            certificationsToRetrieve.length !== 0 ? hydra.getCertificationsDetail(certificationsToRetrieve) : undefined,
            badgesToRetrieve.length !== 0 ? hydra.getBadgesDetail(badgesToRetrieve) : undefined,

        ]);

        const [assetsDetails, channelsDetails, courseCategoriesDetails, courseInstructorsDetails, surveysInfo] = await Promise.all([
            assetsToRetrieve.length !== 0 ? hydra.getAssetsDetail(assetsToRetrieve) : undefined,
            channelsToRetrieve.length !== 0 ? hydra.getChannelsDetail(channelsToRetrieve) : undefined,
            courseCategoriesToRetrieve.length !== 0 ? hydra.getCourseCategories(courseCategoriesToRetrieve) : undefined,
            courseInstructorsToRetrieve.length !== 0 ? hydra.getUsers(courseInstructorsToRetrieve) : undefined,
            surveysToRetrieve.length !== 0 ? hydra.getSurveys(surveysToRetrieve) : undefined,
        ]);


        if (userPropsResponse && userPropsResponse.data) {
            response.data.lastEditBy = userPropsResponse.data as UserProps;
        }

        let i = 0;

        if (typeof response.data.visibility !== 'undefined') {

            for (i = response.data.visibility.users.length - 1; i >= 0; i--) {
                const index = response.data.visibility.users[i].id;
                if (usersInfo.data[index]) {
                    response.data.visibility.users[i].name = usersInfo.data[index].userid;
                } else {
                    response.data.visibility.users.splice(i, 1);
                }
            }
            for (i = response.data.visibility.groups.length - 1; i >= 0; i--) {
                const index = response.data.visibility.groups[i].id;
                if (groupsInfo.data[index]) {
                    response.data.visibility.groups[i].name = groupsInfo.data[index];
                } else {
                    response.data.visibility.groups.splice(i, 1);
                }
            }
            for (i = response.data.visibility.branches.length - 1; i >= 0; i--) {
                const index = response.data.visibility.branches[i].id;
                if (branchesInfo.data[index]) {
                    response.data.visibility.branches[i].name = branchesInfo.data[index];
                } else {
                    response.data.visibility.branches.splice(i, 1);
                }
            }
        }

        if (typeof response.data.users !== 'undefined') {

            // set the entities limits for the users filter
            response.data.users.entitiesLimits = entitiesLimits.users;

            for (i = response.data.users.users.length - 1; i >= 0; i--) {
                const index = response.data.users.users[i].id;
                if (usersInfo.data[index]) {
                    response.data.users.users[i].name = usersInfo.data[index].userid;
                } else {
                    response.data.users.users.splice(i, 1);
                }
            }
            for (i = response.data.users.groups.length - 1; i >= 0; i--) {
                const index = response.data.users.groups[i].id;
                if (groupsInfo.data[index]) {
                    response.data.users.groups[i].name = groupsInfo.data[index];
                } else {
                    response.data.users.groups.splice(i, 1);
                }
            }
            for (i = response.data.users.branches.length - 1; i >= 0; i--) {
                const index = response.data.users.branches[i].id;
                if (branchesInfo.data[index]) {
                    response.data.users.branches[i].name = branchesInfo.data[index];
                } else {
                    response.data.users.branches.splice(i, 1);
                }
            }

            // flag to check if exists user additional fields filter
            const isUserAdditionalFieldsFilter = response.data.userAdditionalFieldsFilter ? Object.keys(response.data.userAdditionalFieldsFilter as {}).length > 0 : false;
            if (response.data.users.isUserAddFields === undefined) {
                response.data.users.isUserAddFields = isUserAdditionalFieldsFilter;
            }

        }

        if (typeof response.data.queryBuilderFilters !== undefined && response.data.queryBuilderFilters) {
            const jsonObject = response.data.queryBuilderFilters;
            Object.keys(response.data.queryBuilderFilters).forEach(filterName => {
                const filter = jsonObject[filterName];
                if (filter.type === QUERY_BUILDER_FILTER_TYPE_USERS) {
                    filter.entitiesLimits = entitiesLimits.users;
                    for (i = filter.users.length - 1; i >= 0; i--) {
                        const index = filter.users[i].id;
                        if (usersInfo.data[index]) {
                            filter.users[i].name = usersInfo.data[index].userid;
                        } else {
                            filter.users.splice(i, 1);
                        }
                    }

                    for (i = filter.groups.length - 1; i >= 0; i--) {
                        const index = filter.groups[i].id;
                        if (groupsInfo.data[index]) {
                            filter.groups[i].name = groupsInfo.data[index];
                        } else {
                            filter.groups.splice(i, 1);
                        }
                    }
                    for (i = filter.branches.length - 1; i >= 0; i--) {
                        const index = filter.branches[i].id;
                        if (branchesInfo.data[index]) {
                            filter.branches[i].name = branchesInfo.data[index];
                        } else {
                            filter.branches.splice(i, 1);
                        }
                    }

                    // flag to check if exists user additional fields filter
                    const isUserAdditionalFieldsFilter = filter.userAdditionalFieldsFilter ? Object.keys(filter.userAdditionalFieldsFilter as {}).length > 0 : false;
                    if (filter.isUserAddFields === undefined) {
                        filter.isUserAddFields = isUserAdditionalFieldsFilter;
                    }
                } else if (filter.type === QUERY_BUILDER_FILTER_TYPE_COURSES) {
                    const jsonFilterCourse = filter as JsonCourseFilter;

                    jsonFilterCourse.entitiesLimits = entitiesLimits.courses;

                    for (i = jsonFilterCourse.courses.length - 1; i >= 0; i--) {
                        const index = jsonFilterCourse.courses[i].id;
                        if (coursesInfo.data[index]) {
                            jsonFilterCourse.courses[i].name = coursesInfo.data[index].title;
                        } else {
                            jsonFilterCourse.courses.splice(i, 1);
                        }
                    }

                    if (courseCategoriesDetails && courseCategoriesDetails.data) {
                        jsonFilterCourse.categories = courseCategoriesDetails.data.map((category) => {
                            return {
                                id: +category.id,
                                name: category.title
                            };
                        });
                    }


                    for (i = jsonFilterCourse.learningPlans.length - 1; i >= 0; i--) {
                        const index = jsonFilterCourse.learningPlans[i].id as number;
                        if (learningPlansInfo.data[index]) {
                            jsonFilterCourse.learningPlans[i].name = learningPlansInfo.data[index].title;
                        } else {
                            jsonFilterCourse.learningPlans.splice(i, 1);
                        }
                    }

                } else if (filter.type === QUERY_BUILDER_FILTER_TYPE_TEXT) {
                    const jsonFilterText = filter as JsonTextFilter;

                    if (jsonFilterText.any === false && !jsonFilterText.operator) {
                        throw new BadRequestException(`Missing operator for filter ${filterName}. Report id: ${response.data.idReport}`);
                    }

                    if (jsonFilterText.any === false && !(<any>Object).values(TextFilterOptions).includes(jsonFilterText.operator)) {
                        throw new BadRequestException(`Wrong operator "${jsonFilterText.operator}" for filter ${filterName}. Report id: ${response.data.idReport}`);
                    }
                }
            });

        }

        if (typeof response.data.courses !== 'undefined') {

            // set the entities limits for the courses filter (switch courses type)
            switch (response.data.type) {
                case ReportsTypes.USERS_CLASSROOM_SESSIONS:
                    response.data.courses.entitiesLimits = entitiesLimits.classrooms;
                    break;
                case ReportsTypes.USERS_WEBINAR:
                    response.data.courses.entitiesLimits = entitiesLimits.webinars;
                    break;
                default:
                    response.data.courses.entitiesLimits = entitiesLimits.courses;
                    break;
            }

            for (i = response.data.courses.courses.length - 1; i >= 0; i--) {
                const index = response.data.courses.courses[i].id;
                if (coursesInfo.data[index]) {
                    response.data.courses.courses[i].name = coursesInfo.data[index].title;
                } else {
                    response.data.courses.courses.splice(i, 1);
                }
            }

            if (courseCategoriesDetails && courseCategoriesDetails.data) {
                response.data.courses.categories = courseCategoriesDetails.data.map((category) => {
                    return {
                        id: +category.id,
                        name: category.title
                    };
                });
            }

            if (courseInstructorsDetails && courseInstructorsDetails.data) {

                for (i = response.data.courses.instructors.length - 1; i >= 0; i--) {
                    const index = response.data.courses.instructors[i].id;

                    if (courseInstructorsDetails.data[index]) {

                        // Return the firstname and lastname with fallback on username (REMOVED)
                        // const showNameSwitcher = this.session.platform.getShowFirstNameFirst() ? `${courseInstructorsDetails.data[index].firstname} ${courseInstructorsDetails.data[index].lastname}` : `${courseInstructorsDetails.data[index].lastname} ${courseInstructorsDetails.data[index].firstname}`;
                        // Fallback on userid if Firstname and Lastname are empty
                        // response.data.courses.instructors[i].name = showNameSwitcher.trim() === '' ?  courseInstructorsDetails.data[index].userid : showNameSwitcher;

                        response.data.courses.instructors[i].name = courseInstructorsDetails.data[index].userid;
                    } else {
                        response.data.courses.instructors.splice(i, 1);
                    }
                }
            }
        }

        if (typeof response.data.sessions !== 'undefined') {
            for (i = response.data.sessions.sessions.length - 1; i >= 0; i--) {
                const index = response.data.sessions.sessions[i].id;
                if (sessionsInfo.data[index]) {
                    response.data.sessions.sessions[i].name = sessionsInfo.data[index].name;
                } else {
                    response.data.sessions.sessions.splice(i, 1);
                }
            }
        }

        if (typeof response.data.surveys !== 'undefined') {
            response.data.surveys.entitiesLimits = entitiesLimits.surveysLimit;
            for (i = response.data.surveys.surveys.length - 1; i >= 0; i--) {
                const index = response.data.surveys.surveys[i].id;
                if (surveysInfo.data[index]) {
                    response.data.surveys.surveys[i].name = surveysInfo.data[index].title;
                } else {
                    response.data.surveys.surveys.splice(i, 1);
                }
            }
        }

        if (typeof response.data.learningPlans !== 'undefined') {
            // set the entities limits for the courses filter
            response.data.learningPlans.entitiesLimits = entitiesLimits.lpLimit;

            for (i = response.data.learningPlans.learningPlans.length - 1; i >= 0; i--) {
                const index = response.data.learningPlans.learningPlans[i].id as number;
                if (learningPlansInfo.data[index]) {
                    response.data.learningPlans.learningPlans[i].name = learningPlansInfo.data[index].title;
                } else {
                    response.data.learningPlans.learningPlans.splice(i, 1);
                }
            }
        }

        // updates the available lo types
        if (needAllLoTypes && allLoTypes && allLoTypes.data) {
            response.data.loTypes = this.reportService
                .refreshSelectedLoTypes(allLoTypes.data as string[], response.data.loTypes ?? {});
        }

        if (response.data.certifications) {
            // set the entities limits for the certifications filter
            response.data.certifications.entitiesLimits = entitiesLimits.certificationsLimit;

            if (certificationsDetails?.data) {
                response.data.certifications.certifications = certificationsDetails.data.map(certDetail => {
                    return {
                        id: +certDetail.id_cert,
                        name: certDetail.title,
                    };
                });
            }
        }

        if (response.data.badges) {
            // set the entities limits for the badges filter
            response.data.badges.entitiesLimits = entitiesLimits.badgesLimit;

            if (badgesDetails?.data) {
                response.data.badges.badges = badgesDetails.data.map(badgeDetail => {
                    return {
                        id: +badgeDetail.id_badge,
                        name: badgeDetail.name
                    };
                });
            }
        }

        if (response.data.assets) {
            // set the entities limits for the assets and channels filter
            response.data.assets.entitiesLimits = entitiesLimits.assets;

            if (assetsDetails?.data) {
                response.data.assets.assets = assetsDetails.data.map((assetDetail) => {
                    return {
                        id: +assetDetail.id,
                        name: assetDetail.title
                    };
                });
            }

            if (channelsDetails?.data) {
                response.data.assets.channels = channelsDetails.data.map((channelDetail) => {
                    return {
                        id: +channelDetail.id,
                        name: channelDetail.name
                    };
                });
            }
        }

        if (
            this.session.platform.isToggleMultipleEnrollmentCompletions() &&
            (response.data.archivingDate === null || response.data.archivingDate === undefined)
        ) {
            response.data.archivingDate = this.getDefaultDateOptions();
        }

        // Return the default value for enrollment suspended if not exist or is null
        if (response.data.enrollment && (response.data.enrollment.suspended === null || response.data.enrollment.suspended === undefined)) {
            response.data.enrollment.suspended = true;
        }

        // Return the default value for session filter if not exist
        if (typeof response.data.sessions === 'undefined' && response.data.type === ReportsTypes.USERS_CLASSROOM_SESSIONS) {
            response.data.sessions = new ReportManagerInfoSessionsFilter();
            response.data.sessions.all = true;
            response.data.sessions.entitiesLimits = this.session.platform.getEntitiesLimits().classrooms.sessionLimit;
        }

        // Return the default value for session attendance type filter if not exist
        if (typeof response.data.sessionAttendanceType === 'undefined' && response.data.type === ReportsTypes.USERS_CLASSROOM_SESSIONS) {
            response.data.sessionAttendanceType = this.getDefaultSessionAttendanceType();
        }

        response.data.enrollment = Object.assign({}, this.getDefaultEnrollment(), response.data.enrollment);

        // Sidekiq schedulation fields
        if (this.session.platform.isDatalakeV2Active()) {
            if (response.data.planning.option && !response.data.planning.option.timezone) {
                response.data.planning.option.timezone = this.session.user.getTimezone();
            }
            if (response.data.planning.option && !response.data.planning.option.startHour) {
                response.data.planning.option.startHour = '00:00';
            }
        }

        return response;
    }

    public convertDateObjectToDatetime(d: Date): string {
        return moment(d).format('YYYY-MM-DD HH:mm:ss');
    }

    public convertDateObjectToDate(d: Date): string {
        return moment(d).format('YYYY-MM-DD');
    }

    public convertDateObjectToExportDate(d: Date): string {
        return moment(d).format('YYYYMMDD');
    }

    public abstract getQuery(limit: number, isPreview: boolean, checkPuVisibility: boolean): Promise<string>;

    // Datalake V3 - Snowflake
    public abstract getQuerySnowflake(limit: number,  isPreview: boolean, checkPuVisibility: boolean, fromSchedule: boolean): Promise<string> ;

    public async calculateCourseFilter(ignoreLPs = false, ignoreInscructor = false, checkPuVisibility = true, jsonCourseFilter?: JsonCourseFilter): Promise<string> {
        let fullCourses = '';
        let fullCoursesSubfilter = '';
        let first = true;

        let courseFilter: any = this.info.courses;
        if (typeof jsonCourseFilter !== 'undefined') {
            courseFilter = jsonCourseFilter;
        }
        const allCourses = courseFilter ? courseFilter.all : false;
        const coursesSelection = typeof courseFilter !== 'undefined' ? courseFilter.courses.map(a => a.id) : [];
        const categoriesSelection = typeof courseFilter !== 'undefined' && typeof courseFilter.categories !== 'undefined' ? courseFilter.categories.map(a => a.id) : [];
        let lpSelection = typeof this.info.learningPlans !== 'undefined' && !ignoreLPs ? this.info.learningPlans.learningPlans.map(a => parseInt(a.id as string, 10)) : [];
        let instructorSelection = typeof this.info.courses !== 'undefined' && typeof this.info.courses.instructors !== 'undefined' ? this.info.courses.instructors.map(a => a.id) : [];

        if (typeof jsonCourseFilter !== 'undefined') {
            lpSelection = typeof jsonCourseFilter.learningPlans !== 'undefined' && !ignoreLPs ? jsonCourseFilter.learningPlans.map(a => parseInt(a.id as string, 10)) : [];
            instructorSelection = [];
        }

        if (this.session.user.getLevel() === UserLevels.POWER_USER && checkPuVisibility === true) {
            await this.createPUCourseFilterTable();
            await this.createPUUserFilterTable();
        }

        if (categoriesSelection.length > 0 || (instructorSelection.length > 0 && !ignoreInscructor)) {
            fullCoursesSubfilter = `
            SELECT DISTINCT(${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.idCourse)
            FROM ${TablesList.LEARNING_COURSEUSER_AGGREGATE} AS ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}
            JOIN ${TablesList.LEARNING_COURSE} AS ${TablesListAliases.LEARNING_COURSE} ON ${TablesListAliases.LEARNING_COURSE}.idCourse = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.idCourse
            ` + (this.puCourseTable !== '' ? `JOIN ${this.puCourseTable} ON ${this.puCourseTable}.idCourse = ${TablesListAliases.LEARNING_COURSE}.idCourse` : '') + `
            ` + (this.puUserTable !== '' ? `JOIN ${this.puUserTable} ON ${this.puUserTable}.idst = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.idUser` : '') + `
            WHERE TRUE`;

            if (instructorSelection.length > 0 && !ignoreInscructor) {
                fullCoursesSubfilter += ` AND ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.idUser IN (${instructorSelection.join(',')})
                AND ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.level = ${CourseuserLevels.Teacher}`;
            }

            if (categoriesSelection.length > 0) {
                fullCoursesSubfilter += ` AND ${TablesListAliases.LEARNING_COURSE}.idCategory IN (${categoriesSelection.join(',')})`;
            }
        }

        if (!allCourses) {
            if (coursesSelection.length > 0) {
                fullCourses += `
                SELECT DISTINCT(${TablesListAliases.LEARNING_COURSE}.idCourse)
                FROM ${TablesList.LEARNING_COURSE} AS ${TablesListAliases.LEARNING_COURSE}
                ` + (this.puCourseTable !== '' ? `JOIN ${this.puCourseTable} ON ${this.puCourseTable}.idCourse = ${TablesListAliases.LEARNING_COURSE}.idCourse` : '') + `
                WHERE ${TablesListAliases.LEARNING_COURSE}.idCourse IN (${coursesSelection.join(',')})`;
                if (fullCoursesSubfilter !== '') {
                    fullCourses += ` AND ${TablesListAliases.LEARNING_COURSE}.idCourse IN (${fullCoursesSubfilter})`;
                }
                first = false;
            }
            if (lpSelection.length > 0) {
                if (!first) {
                    fullCourses += `
                    UNION
                    `;
                } else {
                    first = false;
                }

                fullCourses += `
                SELECT DISTINCT(${TablesListAliases.LEARNING_COURSEPATH_COURSES}.id_item)
                FROM ${TablesList.LEARNING_COURSEPATH_COURSES} AS ${TablesListAliases.LEARNING_COURSEPATH_COURSES}
                ` + (this.puCourseTable !== '' ? `JOIN ${this.puCourseTable} ON ${this.puCourseTable}.idCourse = ${TablesListAliases.LEARNING_COURSEPATH_COURSES}.id_item` : '') + `
                WHERE ${TablesListAliases.LEARNING_COURSEPATH_COURSES}.id_path IN (${lpSelection.join(',')})`;
                if (fullCoursesSubfilter !== '') {
                    fullCourses += ` AND ${TablesListAliases.LEARNING_COURSEPATH_COURSES}.id_item IN (${fullCoursesSubfilter})`;
                }
            }
        }

        if (fullCourses === '' && this.puCourseTable !== '' && fullCoursesSubfilter === '') {
            fullCourses = `SELECT idCourse FROM ${this.puCourseTable}`;
        }

        if (fullCourses === '') {
            fullCourses = fullCoursesSubfilter;
        }

        if (typeof jsonCourseFilter !== 'undefined') {
            let courseSubQuery = `SELECT idCourse FROM ${TablesList.LEARNING_COURSE} WHERE TRUE`;

            if (fullCourses !== '') {
                courseSubQuery += ` AND idCourse IN (${fullCourses})`;
            }

            courseSubQuery += this.buildDateFilter('date_end', jsonCourseFilter.courseExpirationDate, 'AND', true);

            fullCourses = courseSubQuery;
        }

        return fullCourses;
    }

    public async calculateCourseFilterSnowflake(ignoreLPs = false, ignoreInscructor = false, checkPuVisibility = true, jsonCourseFilter?: JsonCourseFilter): Promise<string> {
        let fullCourses = '';
        let fullCoursesSubfilter = '';
        let first = true;
        let isPowerUser = false;
        let joinCoreUserPuTable = '';
        let joinCoreUserPuCourseTable = '';
        let joinCoreUserPuCourseTableWithLP = '';

        let courseFilter: any = this.info.courses;
        if (typeof jsonCourseFilter !== 'undefined') {
            courseFilter = jsonCourseFilter;
        }
        const allCourses = courseFilter ? courseFilter.all : false;
        const coursesSelection = typeof courseFilter !== 'undefined' ? courseFilter.courses.map(a => a.id) : [];
        const categoriesSelection = typeof courseFilter !== 'undefined' && typeof courseFilter.categories !== 'undefined' ? courseFilter.categories.map(a => a.id) : [];
        let lpSelection = typeof this.info.learningPlans !== 'undefined' && !ignoreLPs ? this.info.learningPlans.learningPlans.map(a => parseInt(a.id as string, 10)) : [];
        let instructorSelection = typeof this.info.courses !== 'undefined' && typeof this.info.courses.instructors !== 'undefined' ? this.info.courses.instructors.map(a => a.id) : [];

        if (typeof jsonCourseFilter !== 'undefined') {
            lpSelection = typeof jsonCourseFilter.learningPlans !== 'undefined' && !ignoreLPs ? jsonCourseFilter.learningPlans.map(a => parseInt(a.id as string, 10)) : [];
            instructorSelection = [];
        }

        if (this.session.user.getLevel() === UserLevels.POWER_USER && checkPuVisibility === true) {
            isPowerUser = true;
            joinCoreUserPuTable = `JOIN ${TablesList.CORE_USER_PU} as ${TablesListAliases.CORE_USER_PU}
                ON ${TablesListAliases.CORE_USER_PU}."user_id" = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."iduser"
                AND ${TablesListAliases.CORE_USER_PU}."puser_id" = ${this.session.user.getIdUser()}`;

            joinCoreUserPuCourseTable = `JOIN ${TablesList.CORE_USER_PU_COURSE} as ${TablesListAliases.CORE_USER_PU_COURSE}
                ON ${TablesListAliases.CORE_USER_PU_COURSE}."course_id" = ${TablesListAliases.LEARNING_COURSE}."idcourse"
                AND ${TablesListAliases.CORE_USER_PU_COURSE}."puser_id" = ${this.session.user.getIdUser()}`;

            joinCoreUserPuCourseTableWithLP = `JOIN ${TablesList.CORE_USER_PU_COURSE} as ${TablesListAliases.CORE_USER_PU_COURSE}
                ON ${TablesListAliases.CORE_USER_PU_COURSE}."course_id" = ${TablesListAliases.LEARNING_COURSEPATH_COURSES}."id_item"
                AND ${TablesListAliases.CORE_USER_PU_COURSE}."puser_id" = ${this.session.user.getIdUser()}`;
        }

        if (categoriesSelection.length > 0 || (instructorSelection.length > 0 && !ignoreInscructor)) {
            fullCoursesSubfilter = `
            SELECT DISTINCT(${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."idcourse")
            FROM ${TablesList.LEARNING_COURSEUSER_AGGREGATE} AS ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}
            JOIN ${TablesList.LEARNING_COURSE} AS ${TablesListAliases.LEARNING_COURSE} ON ${TablesListAliases.LEARNING_COURSE}."idcourse" = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."idcourse"
            ` + (isPowerUser ? joinCoreUserPuCourseTable : '') + `
            ` + (isPowerUser ? joinCoreUserPuTable : '') + `
            WHERE TRUE`;

            if (instructorSelection.length > 0 && !ignoreInscructor) {
                fullCoursesSubfilter += ` AND ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."iduser" IN (${instructorSelection.join(',')})
                AND ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."level" = ${CourseuserLevels.Teacher}`;
            }

            if (categoriesSelection.length > 0) {
                fullCoursesSubfilter += ` AND ${TablesListAliases.LEARNING_COURSE}."idcategory" IN (${categoriesSelection.join(',')})`;
            }
        }

        if (!allCourses) {
            if (coursesSelection.length > 0) {
                fullCourses += `
                SELECT DISTINCT(${TablesListAliases.LEARNING_COURSE}."idcourse")
                FROM ${TablesList.LEARNING_COURSE} AS ${TablesListAliases.LEARNING_COURSE}
                ` + (isPowerUser ? joinCoreUserPuCourseTable : '') + `
                WHERE ${TablesListAliases.LEARNING_COURSE}."idcourse" IN (${coursesSelection.join(',')})`;
                if (fullCoursesSubfilter !== '') {
                    fullCourses += ` AND ${TablesListAliases.LEARNING_COURSE}."idcourse" IN (${fullCoursesSubfilter})`;
                }
                first = false;
            }
            if (lpSelection.length > 0) {
                if (!first) {
                    fullCourses += `
                    UNION
                    `;
                } else {
                    first = false;
                }

                fullCourses += `
                SELECT DISTINCT(${TablesListAliases.LEARNING_COURSEPATH_COURSES}."id_item")
                FROM ${TablesList.LEARNING_COURSEPATH_COURSES} AS ${TablesListAliases.LEARNING_COURSEPATH_COURSES}
                ` + (isPowerUser ? joinCoreUserPuCourseTableWithLP : '') + `
                WHERE ${TablesListAliases.LEARNING_COURSEPATH_COURSES}."id_path" IN (${lpSelection.join(',')})`;
                if (fullCoursesSubfilter !== '') {
                    fullCourses += ` AND ${TablesListAliases.LEARNING_COURSEPATH_COURSES}."id_item" IN (${fullCoursesSubfilter})`;
                }
            }
        }

        if (fullCourses === '' && isPowerUser && fullCoursesSubfilter === '') {
            fullCourses = `SELECT "course_id" FROM ${TablesList.CORE_USER_PU_COURSE} where "puser_id" = ${this.session.user.getIdUser()}`;
        }

        if (fullCourses === '') {
            fullCourses = fullCoursesSubfilter;
        }

        if (typeof jsonCourseFilter !== 'undefined') {
            let courseSubQuery = `SELECT "idcourse" FROM ${TablesList.LEARNING_COURSE} WHERE TRUE`;

            if (fullCourses !== '') {
                courseSubQuery += ` AND "idcourse" IN (${fullCourses})`;
            }

            courseSubQuery += this.buildDateFilter('date_end', jsonCourseFilter.courseExpirationDate, 'AND', true);

            fullCourses = courseSubQuery;
        }

        return fullCourses;
    }

    public async calculateGroupFilterSnowflake(checkPuVisibility = true): Promise<string> {
        let joinCoreUserPuTable = '';
        let fullGroups = '';
        let first = true;
        const allUsers = this.info.users ? this.info.users.all : false;
        const groupsSelection = typeof this.info.users !== 'undefined' ? this.info.users.groups.map(a => a.id) : [];
        const branchesSelection = typeof this.info.users !== 'undefined' ? this.info.users.branches : [];

        if (this.session.user.getLevel() === UserLevels.POWER_USER && checkPuVisibility === true) {
            joinCoreUserPuTable = `JOIN ${TablesList.CORE_USER_PU} as ${TablesListAliases.CORE_USER_PU}
                ON ${TablesListAliases.CORE_USER_PU}."user_id" = ${TablesListAliases.CORE_GROUP}."idst"
                AND ${TablesListAliases.CORE_USER_PU}."puser_id" = ${this.session.user.getIdUser()}`;
        }

        if (!allUsers) {
            if (groupsSelection.length > 0) {
                fullGroups += `
                SELECT DISTINCT(${TablesListAliases.CORE_GROUP}."idst")
                FROM ${TablesList.CORE_GROUP} AS ${TablesListAliases.CORE_GROUP}
                ` + joinCoreUserPuTable + `
                WHERE ${TablesListAliases.CORE_GROUP}."idst" IN (${groupsSelection.join(',')})`;

                first = false;
            }

            if (branchesSelection.length > 0) {
                const descendants: number[] = [];
                const noDescendants: number[] = [];

                branchesSelection.forEach(element => {
                    if (element.descendants) {
                        descendants.push(element.id);
                    } else {
                        noDescendants.push(element.id);
                    }
                });

                if (noDescendants.length > 0) {
                    if (!first) {
                        fullGroups += ` UNION `;
                    } else {
                        first = false;
                    }

                    fullGroups += `
                    SELECT DISTINCT(${TablesListAliases.CORE_GROUP}."idst")
                    FROM ${TablesList.CORE_GROUP} AS ${TablesListAliases.CORE_GROUP}
                    ` + joinCoreUserPuTable + `
                    JOIN ${TablesList.CORE_ORG_CHART_TREE} AS ${TablesListAliases.CORE_ORG_CHART_TREE} ON ${TablesListAliases.CORE_ORG_CHART_TREE}."idst_oc" = ${TablesListAliases.CORE_GROUP}."idst"
                    WHERE ${TablesListAliases.CORE_ORG_CHART_TREE}."idorg" IN (${noDescendants.join(',')})`;
                }

                if (descendants.length > 0) {
                    if (!first) {
                        fullGroups += ` UNION `;
                    }

                    fullGroups += `
                    SELECT DISTINCT(${TablesListAliases.CORE_GROUP}."idst")
                    FROM ${TablesList.CORE_ORG_CHART_TREE} AS ${TablesListAliases.CORE_ORG_CHART_TREE}
                    JOIN ${TablesList.CORE_ORG_CHART_TREE} AS ${TablesListAliases.CORE_ORG_CHART_TREE}d ON ${TablesListAliases.CORE_ORG_CHART_TREE}d."ileft" >= ${TablesListAliases.CORE_ORG_CHART_TREE}."ileft"
                                                AND ${TablesListAliases.CORE_ORG_CHART_TREE}d."iright" <= ${TablesListAliases.CORE_ORG_CHART_TREE}."iright"
                    JOIN ${TablesList.CORE_GROUP} AS ${TablesListAliases.CORE_GROUP} ON ${TablesListAliases.CORE_GROUP}."idst" = ${TablesListAliases.CORE_ORG_CHART_TREE}d."idst_oc"
                    ` + joinCoreUserPuTable + `

                    WHERE ${TablesListAliases.CORE_ORG_CHART_TREE}."idorg" IN (${descendants.join(',')})`;
                }
            }
        }

        if (fullGroups === '' && joinCoreUserPuTable !== '') {
            fullGroups = `SELECT "user_id" FROM ${TablesList.CORE_USER_PU} where "puser_id" = ${this.session.user.getIdUser()}`;
        }

        return fullGroups;
    }


    public async calculateAssetFilter(): Promise<number[]> {
        const athena = this.session.getAthena();

        let fullAssets: number[] = [];
        const assetsSelection = this.info.assets ? this.info.assets?.assets.map(a => a.id) : [];
        const channelsSelection = this.info.assets ? this.info.assets.channels.map(c => c.id) : [];

        fullAssets = fullAssets.concat(assetsSelection);

        if (channelsSelection.length > 0) {
            const filterQuery = `SELECT cha.idasset FROM app7020_channel_assets AS cha WHERE cha.idchannel IN (${channelsSelection.join(',')}) AND cha.asset_type = 1`;
            const data = await athena.connection.query(filterQuery);
            const items = data.Items as ReportChannelsFilterRecover[];

            if (items.length > 0) {
                fullAssets = fullAssets.concat(items.map(a => a.idasset));
            }
        }

        fullAssets = Array.from(new Set(fullAssets));
        return fullAssets;
    }

    public async calculateUserFilter(checkPuVisibility = true, jsonUserFilter?: JsonUserFilter): Promise<string> {
        let fullUsers = '';
        let first = true;

        let userFilter = this.info.users;
        if (typeof jsonUserFilter !== 'undefined') {
            userFilter = jsonUserFilter;
        }

        const allUsers = userFilter ? userFilter.all : false;
        const usersSelection = typeof userFilter !== 'undefined' ? userFilter.users.map(a => a.id) : [];
        const groupsSelection = typeof userFilter !== 'undefined' ? userFilter.groups.map(a => a.id) : [];
        const branchesSelection = typeof userFilter !== 'undefined' ? userFilter.branches : [];

        if (this.session.user.getLevel() === UserLevels.POWER_USER && checkPuVisibility === true) {
            await this.createPUUserFilterTable();
        }

        if (!allUsers) {
            if (usersSelection.length > 0) {
                fullUsers += `
                SELECT DISTINCT(${TablesListAliases.CORE_USER}.idst)
                FROM ${TablesList.CORE_USER} AS ${TablesListAliases.CORE_USER}
                ` + (this.puUserTable !== '' ? `JOIN ${this.puUserTable} ON ${this.puUserTable}.idst = ${TablesListAliases.CORE_USER}.idst` : '') + `
                WHERE ${TablesListAliases.CORE_USER}.idst IN (${usersSelection.join(',')})`;
                first = false;
            }

            if (groupsSelection.length > 0) {
                if (!first) {
                    fullUsers += `
                    UNION
                    `;
                } else {
                    first = false;
                }

                fullUsers += `
                SELECT DISTINCT(${TablesListAliases.CORE_GROUP_MEMBERS}.idstMember)
                FROM ${TablesList.CORE_GROUP_MEMBERS} AS ${TablesListAliases.CORE_GROUP_MEMBERS}
                JOIN ${TablesList.CORE_USER} AS ${TablesListAliases.CORE_USER} ON ${TablesListAliases.CORE_USER}.idst = ${TablesListAliases.CORE_GROUP_MEMBERS}.idstMember
                ` + (this.puUserTable !== '' ? `JOIN ${this.puUserTable} ON ${this.puUserTable}.idst = ${TablesListAliases.CORE_USER}.idst` : '') + `
                WHERE ${TablesListAliases.CORE_GROUP_MEMBERS}.idst IN (${groupsSelection.join(',')})`;
            }

            if (branchesSelection.length > 0) {
                const descendants: number[] = [];
                const noDescendants: number[] = [];

                branchesSelection.forEach(element => {
                    if (element.descendants) {
                        descendants.push(element.id);
                    } else {
                        noDescendants.push(element.id);
                    }
                });

                if (noDescendants.length > 0) {
                    if (!first) {
                        fullUsers += `
                        UNION
                        `;
                    } else {
                        first = false;
                    }

                    fullUsers += `
                    SELECT DISTINCT(${TablesListAliases.CORE_GROUP_MEMBERS}.idstMember)
                    FROM ${TablesList.CORE_GROUP_MEMBERS} AS ${TablesListAliases.CORE_GROUP_MEMBERS}
                    JOIN ${TablesList.CORE_USER} AS ${TablesListAliases.CORE_USER} ON ${TablesListAliases.CORE_USER}.idst = ${TablesListAliases.CORE_GROUP_MEMBERS}.idstMember
                    ` + (this.puUserTable !== '' ? `JOIN ${this.puUserTable} ON ${this.puUserTable}.idst = ${TablesListAliases.CORE_USER}.idst` : '') + `
                    JOIN ${TablesList.CORE_ORG_CHART_TREE} AS ${TablesListAliases.CORE_ORG_CHART_TREE} ON ${TablesListAliases.CORE_ORG_CHART_TREE}.idst_oc = ${TablesListAliases.CORE_GROUP_MEMBERS}.idst
                    WHERE ${TablesListAliases.CORE_ORG_CHART_TREE}.idOrg IN (${noDescendants.join(',')})`;
                }

                if (descendants.length > 0) {
                    if (!first) {
                        fullUsers += `
                        UNION
                        `;
                    } else {
                        first = false;
                    }

                    fullUsers += `
                    SELECT DISTINCT(${TablesListAliases.CORE_GROUP_MEMBERS}.idstMember)
                    FROM ${TablesList.CORE_ORG_CHART_TREE} AS ${TablesListAliases.CORE_ORG_CHART_TREE}
                    JOIN ${TablesList.CORE_ORG_CHART_TREE} AS ${TablesListAliases.CORE_ORG_CHART_TREE}d ON ${TablesListAliases.CORE_ORG_CHART_TREE}d.iLeft >= ${TablesListAliases.CORE_ORG_CHART_TREE}.iLeft AND ${TablesListAliases.CORE_ORG_CHART_TREE}d.iRight <= ${TablesListAliases.CORE_ORG_CHART_TREE}.iRight
                    JOIN ${TablesList.CORE_GROUP_MEMBERS} AS ${TablesListAliases.CORE_GROUP_MEMBERS} ON ${TablesListAliases.CORE_GROUP_MEMBERS}.idst = ${TablesListAliases.CORE_ORG_CHART_TREE}d.idst_oc
                    JOIN ${TablesList.CORE_USER} AS ${TablesListAliases.CORE_USER} ON ${TablesListAliases.CORE_USER}.idst = ${TablesListAliases.CORE_GROUP_MEMBERS}.idstMember
                    ` + (this.puUserTable !== '' ? `JOIN ${this.puUserTable} ON ${this.puUserTable}.idst = ${TablesListAliases.CORE_USER}.idst` : '') + `
                    WHERE ${TablesListAliases.CORE_ORG_CHART_TREE}.idOrg IN (${descendants.join(',')})`;
                }
            }
        }

        if (fullUsers === '' && this.puUserTable !== '') {
            fullUsers = `SELECT idst FROM ${this.puUserTable}`;
        }

        // The manager subordinate temporary table is created when a manager exports a report in My Team page
        if (fullUsers === '' && this.managerSubordinatesTable !== '') {
            fullUsers = `SELECT idst FROM ${this.managerSubordinatesTable}`;
        }

        return fullUsers;
    }

    public async calculateUserFilterSnowflake(checkPuVisibility = true, jsonUserFilter?: JsonUserFilter): Promise<string> {
        let fullUsers = '';
        let first = true;
        let isPowerUser = false;
        let joinCoreUserPuTable = '';

        let userFilter = this.info.users;
        if (typeof jsonUserFilter !== 'undefined') {
            userFilter = jsonUserFilter;
        }

        const allUsers = userFilter ? userFilter.all : false;
        const usersSelection = typeof userFilter !== 'undefined' ? userFilter.users.map(a => a.id) : [];
        const groupsSelection = typeof userFilter !== 'undefined' ? userFilter.groups.map(a => a.id) : [];
        const branchesSelection = typeof userFilter !== 'undefined' ? userFilter.branches : [];


        if (this.session.user.getLevel() === UserLevels.POWER_USER && checkPuVisibility === true) {
            isPowerUser = true;
            joinCoreUserPuTable = `JOIN ${TablesList.CORE_USER_PU} as ${TablesListAliases.CORE_USER_PU}
                ON ${TablesListAliases.CORE_USER_PU}."user_id" = ${TablesListAliases.CORE_USER}."idst"
                AND ${TablesListAliases.CORE_USER_PU}."puser_id" = ${this.session.user.getIdUser()}`;
        }

        if (!allUsers) {
            if (usersSelection.length > 0) {
                fullUsers += `
                SELECT DISTINCT(${TablesListAliases.CORE_USER}."idst")
                FROM ${TablesList.CORE_USER} AS ${TablesListAliases.CORE_USER}
                ` + (isPowerUser  ? joinCoreUserPuTable : '') + `
                WHERE ${TablesListAliases.CORE_USER}."idst" IN (${usersSelection.join(',')})`;
                first = false;
            }

            if (groupsSelection.length > 0) {
                if (!first) {
                    fullUsers += `
                    UNION
                    `;
                } else {
                    first = false;
                }

                fullUsers += `
                SELECT DISTINCT(${TablesListAliases.CORE_GROUP_MEMBERS}."idstmember")
                FROM ${TablesList.CORE_GROUP_MEMBERS} AS ${TablesListAliases.CORE_GROUP_MEMBERS}
                JOIN ${TablesList.CORE_USER} AS ${TablesListAliases.CORE_USER} ON ${TablesListAliases.CORE_USER}."idst" = ${TablesListAliases.CORE_GROUP_MEMBERS}."idstmember"
                ` + (isPowerUser  ? joinCoreUserPuTable : '') + `
                WHERE ${TablesListAliases.CORE_GROUP_MEMBERS}."idst" IN (${groupsSelection.join(',')})`;
            }

            if (branchesSelection.length > 0) {
                const descendants: number[] = [];
                const noDescendants: number[] = [];

                branchesSelection.forEach(element => {
                    if (element.descendants) {
                        descendants.push(element.id);
                    } else {
                        noDescendants.push(element.id);
                    }
                });

                if (noDescendants.length > 0) {
                    if (!first) {
                        fullUsers += `
                        UNION
                        `;
                    } else {
                        first = false;
                    }

                    fullUsers += `
                    SELECT DISTINCT(${TablesListAliases.CORE_GROUP_MEMBERS}."idstmember")
                    FROM ${TablesList.CORE_GROUP_MEMBERS} AS ${TablesListAliases.CORE_GROUP_MEMBERS}
                    JOIN ${TablesList.CORE_USER} AS ${TablesListAliases.CORE_USER} ON ${TablesListAliases.CORE_USER}."idst" = ${TablesListAliases.CORE_GROUP_MEMBERS}."idstmember"
                    ` + (isPowerUser  ? joinCoreUserPuTable : '') + `
                    JOIN ${TablesList.CORE_ORG_CHART_TREE} AS ${TablesListAliases.CORE_ORG_CHART_TREE} ON ${TablesListAliases.CORE_ORG_CHART_TREE}."idst_oc" = ${TablesListAliases.CORE_GROUP_MEMBERS}."idst"
                    WHERE ${TablesListAliases.CORE_ORG_CHART_TREE}."idorg" IN (${noDescendants.join(',')})`;
                }

                if (descendants.length > 0) {
                    if (!first) {
                        fullUsers += `
                        UNION
                        `;
                    } else {
                        first = false;
                    }

                    fullUsers += `
                    SELECT DISTINCT(${TablesListAliases.CORE_GROUP_MEMBERS}."idstmember")
                    FROM ${TablesList.CORE_ORG_CHART_TREE} AS ${TablesListAliases.CORE_ORG_CHART_TREE}
                    JOIN ${TablesList.CORE_ORG_CHART_TREE} AS ${TablesListAliases.CORE_ORG_CHART_TREE}d ON ${TablesListAliases.CORE_ORG_CHART_TREE}d."ileft" >= ${TablesListAliases.CORE_ORG_CHART_TREE}."ileft" AND ${TablesListAliases.CORE_ORG_CHART_TREE}d."iright" <= ${TablesListAliases.CORE_ORG_CHART_TREE}."iright"
                    JOIN ${TablesList.CORE_GROUP_MEMBERS} AS ${TablesListAliases.CORE_GROUP_MEMBERS} ON ${TablesListAliases.CORE_GROUP_MEMBERS}."idst" = ${TablesListAliases.CORE_ORG_CHART_TREE}d."idst_oc"
                    JOIN ${TablesList.CORE_USER} AS ${TablesListAliases.CORE_USER} ON ${TablesListAliases.CORE_USER}."idst" = ${TablesListAliases.CORE_GROUP_MEMBERS}."idstmember"
                    ` + (isPowerUser  ? joinCoreUserPuTable : '') + `
                    WHERE ${TablesListAliases.CORE_ORG_CHART_TREE}."idorg" IN (${descendants.join(',')})`;
                }
            }
        }

        if (fullUsers === '' && isPowerUser) {
            fullUsers = `SELECT "user_id" FROM ${TablesList.CORE_USER_PU} where "puser_id" = ${this.session.user.getIdUser()}`;
        }

        // The manager subordinate temporary table is created when a manager exports a report in My Team page
        if (fullUsers === '' && this.managerSubordinatesTable !== '') {
            fullUsers = `SELECT idst FROM ${this.managerSubordinatesTable}`;
        }

        return fullUsers;
    }

    public async createPUUserFilterTable() {
        if (this.puUserTable !== '') {
            return this.puUserTable;
        }

        const hydra = this.session.getHydra();
        const athena = this.session.getAthena();
        const s3 = this.session.getS3();
        this.puUserTable = this.getPuUserTableName();

        for await (const res of hydra.getPuUsers(this.puApiPageSize)) {
            let csv = '';
            res.data.items.forEach(idst => {
                csv += idst.toString() + '\n';
            });
            fs.writeFileSync(`./tmp/${this.puUserTable}.csv`, csv, {flag: 'a'});
        }
        const path = await s3.uploadTempTableFile(this.puUserTable);

        const query = `CREATE EXTERNAL TABLE \`${this.puUserTable}\`(
            \`idst\` int)
          ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
          STORED AS INPUTFORMAT
            'org.apache.hadoop.mapred.TextInputFormat'
          OUTPUTFORMAT
            'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
          LOCATION
            '${path}'
          TBLPROPERTIES (
            'has_encrypted_data'='false')`;

        await athena.connection.query(query);

        fs.unlinkSync(`./tmp/${this.puUserTable}.csv`);
    }

    public async createManagerSubordinatesFilterTable(idManagerUser: number, managerType: number[]): Promise<boolean> {

        if (this.managerSubordinatesTable !== '') {
            return true;
        }

        const athena = this.session.getAthena();
        const s3 = this.session.getS3();
        const hydra = this.session.getHydra();
        const subordinates = await hydra.getUserIdsByManager(idManagerUser, managerType);
        if (subordinates.length === 0) {
            return false;
        }

        this.managerSubordinatesTable = this.getManagerSubordinatesTableName();

        let csv = '';
        subordinates.forEach(subordinate => {
            csv += subordinate.toString() + '\n';
        });

        fs.writeFileSync(`./tmp/${this.managerSubordinatesTable}.csv`, csv, {flag: 'w'});

        const path = await s3.uploadTempTableFile(this.managerSubordinatesTable);

        const query = `CREATE EXTERNAL TABLE \`${this.managerSubordinatesTable}\`(
            \`idst\` int)
          ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
          STORED AS INPUTFORMAT
            'org.apache.hadoop.mapred.TextInputFormat'
          OUTPUTFORMAT
            'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
          LOCATION
            '${path}'
          TBLPROPERTIES (
            'has_encrypted_data'='false')`;

        await athena.connection.query(query);

        fs.unlinkSync(`./tmp/${this.managerSubordinatesTable}.csv`);

        return true;
    }

    public async createManagerSubordinatesFilterTableV3(idManagerUser: number, managerType: number[]): Promise<boolean> {
        if (this.managerSubordinatesTable !== '') {
            return true;
        }
        const hydra = this.session.getHydra();
        const subordinates = await hydra.getUserIdsByManager(idManagerUser, managerType);
        if (subordinates.length === 0) {
            return false;
        }
        this.managerSubordinatesTable = `(SELECT column1 AS idst FROM (VALUES (${subordinates.join('), (')})))`;

        return true;
    }

    public async createPUCourseFilterTable() {
        if (this.puCourseTable !== '') {
            return this.puCourseTable;
        }

        const hydra = this.session.getHydra();
        const athena = this.session.getAthena();
        const s3 = this.session.getS3();
        this.puCourseTable = this.getPuCourseTableName();

        const puCourses = await hydra.getPuCourses();
        let csv = '';

        puCourses.data.forEach(idCourse => {
            csv += idCourse.toString() + '\n';
        });

        fs.writeFileSync(`./tmp/${this.puCourseTable}.csv`, csv, {flag: 'w'});

        const path = await s3.uploadTempTableFile(this.puCourseTable);

        const query = `CREATE EXTERNAL TABLE \`${this.puCourseTable}\`(
            \`idCourse\` int)
          ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
          STORED AS INPUTFORMAT
            'org.apache.hadoop.mapred.TextInputFormat'
          OUTPUTFORMAT
            'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
          LOCATION
            '${path}'
          TBLPROPERTIES (
            'has_encrypted_data'='false')`;

        await athena.connection.query(query);

        fs.unlinkSync(`./tmp/${this.puCourseTable}.csv`);
    }

    public getPuUserTableName() {
        return `pu_${this.session.user.getIdUser()}_${Date.now()}_${Math.floor(Math.random() * 10)}${Math.floor(Math.random() * 10)}${Math.floor(Math.random() * 10)}_users`;
    }

    public getPuCourseTableName() {
        return `pu_${this.session.user.getIdUser()}_${Date.now()}_${Math.floor(Math.random() * 10)}${Math.floor(Math.random() * 10)}${Math.floor(Math.random() * 10)}_courses`;
    }

    public getPuUserBranchesTableName() {
        return `pu_${this.session.user.getIdUser()}_${Date.now()}_${Math.floor(Math.random() * 10)}${Math.floor(Math.random() * 10)}${Math.floor(Math.random() * 10)}_user_branches`;
    }

    public getManagerSubordinatesTableName() {
        return `man_${this.session.user.getIdUser()}_${Date.now()}_${Math.floor(Math.random() * 10)}${Math.floor(Math.random() * 10)}${Math.floor(Math.random() * 10)}_subordinates`;
    }

    public async dropTemporaryTables() {
        const athena = this.session.getAthena();
        const s3 = this.session.getS3();
        if (this.puUserTable !== '') {
            try {
                await athena.connection.query(`DROP TABLE IF EXISTS ${this.puUserTable}`);
            } catch (error: any) {
                // TODO: Add to the version cleaning lambda also the drop o fthe tables for the PUs or to a new separated lambda
                // We will just ignore it, we will drop the temp table during the step function run (like for GLUE table versions)
                // In this way we will not block the report extraction for problems in dropping the table
            }
            await s3.deleteTempTableFile(this.puUserTable);
        }
        if (this.puCourseTable !== '') {
            try {
                await athena.connection.query(`DROP TABLE IF EXISTS ${this.puCourseTable}`);
            } catch (error: any) {
                // We will just ignore it, we will drop the temp table during the step function run (like for GLUE table versions)
                // In this way we will not block the report extraction for problems in dropping the table
            }
            await s3.deleteTempTableFile(this.puCourseTable);
        }
        if (this.puUserBranchesTable !== '') {
            try {
                await athena.connection.query(`DROP TABLE IF EXISTS ${this.puUserBranchesTable}`);
            } catch (error: any) {
                // We will just ignore it, we will drop the temp table during the step function run (like for GLUE table versions)
                // In this way we will not block the report extraction for problems in dropping the table
            }
        }
        if (this.managerSubordinatesTable !== '') {
            try {
                await athena.connection.query(`DROP TABLE IF EXISTS ${this.managerSubordinatesTable}`);
            } catch (error: any) {
                // We will just ignore it, we will drop the temp table during the step function run (like for GLUE table versions)
                // In this way we will not block the report extraction for problems in dropping the table
            }
            await s3.deleteTempTableFile(this.managerSubordinatesTable);
        }
    }

    public async calculateGroupsFilter(): Promise<string> {
        let fullGroups = '';
        let first = true;
        const allUsers = this.info.users ? this.info.users.all : false;
        const groupsSelection = typeof this.info.users !== 'undefined' ? this.info.users.groups.map(a => a.id) : [];
        const branchesSelection = typeof this.info.users !== 'undefined' ? this.info.users.branches : [];

        if (this.session.user.getLevel() === UserLevels.POWER_USER) {
            await this.createPUUserFilterTable();
        }

        if (!allUsers) {
            if (groupsSelection.length > 0) {
                fullGroups += `
                SELECT DISTINCT(${TablesListAliases.CORE_GROUP}.idst)
                FROM ${TablesList.CORE_GROUP} AS ${TablesListAliases.CORE_GROUP}
                ` + (this.puUserTable !== '' ? `JOIN ${this.puUserTable} ON ${this.puUserTable}.idst = ${TablesListAliases.CORE_GROUP}.idst` : '') + `
                WHERE ${TablesListAliases.CORE_GROUP}.idst IN (${groupsSelection.join(',')})`;

                first = false;
            }

            if (branchesSelection.length > 0) {
                const descendants: number[] = [];
                const noDescendants: number[] = [];

                branchesSelection.forEach(element => {
                    if (element.descendants) {
                        descendants.push(element.id);
                    } else {
                        noDescendants.push(element.id);
                    }
                });

                if (noDescendants.length > 0) {
                    if (!first) {
                        fullGroups += `
                        UNION
                        `;
                    } else {
                        first = false;
                    }

                    fullGroups += `
                    SELECT DISTINCT(${TablesListAliases.CORE_GROUP}.idst)
                    FROM ${TablesList.CORE_GROUP} AS ${TablesListAliases.CORE_GROUP}
                    ` + (this.puUserTable !== '' ? `JOIN ${this.puUserTable} ON ${this.puUserTable}.idst = ${TablesListAliases.CORE_GROUP}.idst` : '') + `
                    JOIN ${TablesList.CORE_ORG_CHART_TREE} AS ${TablesListAliases.CORE_ORG_CHART_TREE} ON ${TablesListAliases.CORE_ORG_CHART_TREE}.idst_oc = ${TablesListAliases.CORE_GROUP}.idst
                    WHERE ${TablesListAliases.CORE_ORG_CHART_TREE}.idOrg IN (${noDescendants.join(',')})`;
                }

                if (descendants.length > 0) {
                    if (!first) {
                        fullGroups += `
                        UNION
                        `;
                    } else {
                        first = false;
                    }

                    fullGroups += `
                    SELECT DISTINCT(${TablesListAliases.CORE_GROUP}.idst)
                    FROM ${TablesList.CORE_ORG_CHART_TREE} AS ${TablesListAliases.CORE_ORG_CHART_TREE}
                    JOIN ${TablesList.CORE_ORG_CHART_TREE} AS ${TablesListAliases.CORE_ORG_CHART_TREE}d ON ${TablesListAliases.CORE_ORG_CHART_TREE}d.iLeft >= ${TablesListAliases.CORE_ORG_CHART_TREE}.iLeft AND ${TablesListAliases.CORE_ORG_CHART_TREE}d.iRight <= ${TablesListAliases.CORE_ORG_CHART_TREE}.iRight
                    JOIN ${TablesList.CORE_GROUP} AS ${TablesListAliases.CORE_GROUP} ON ${TablesListAliases.CORE_GROUP}.idst = ${TablesListAliases.CORE_ORG_CHART_TREE}d.idst_oc
                    ` + (this.puUserTable !== '' ? `JOIN ${this.puUserTable} ON ${this.puUserTable}.idst = ${TablesListAliases.CORE_GROUP}.idst` : '') + `
                    WHERE ${TablesListAliases.CORE_ORG_CHART_TREE}.idOrg IN (${descendants.join(',')})`;
                }
            }
        }

        if (fullGroups === '' && this.puUserTable !== '') {
            fullGroups = `SELECT idst FROM ${this.puUserTable}`;
        }

        return fullGroups;
    }

    public async createPuUserBranchesTable(): Promise<string> {
        if (this.puUserBranchesTable !== '') {
            return this.puUserBranchesTable;
        }

        await this.createPUUserFilterTable();

        this.puUserBranchesTable = this.getPuUserBranchesTableName();

        const query = `
            CREATE TABLE ${this.puUserBranchesTable} AS
            SELECT "cu"."idst",
                        "cocp"."lang_code",
                        ARRAY_JOIN(ARRAY_AGG(DISTINCT("cocp"."path") ORDER BY "cocp"."path"), ', ') AS "branches",
                        ARRAY_JOIN(ARRAY_AGG(DISTINCT("cocp"."code") ORDER BY "cocp"."code"), ', ') AS "codes"
            FROM core_user cu
            JOIN ${this.puUserTable} as pu_users on "pu_users"."idst" = "cu"."idst"
            JOIN core_group_members cgm
                ON "cgm"."idstMember" = "cu"."idst"
            JOIN ${this.puUserTable} as pu_branches
                ON "pu_branches"."idst" = "cgm"."idst"
            JOIN
                 (
                     select "idorg", "idst_oc" as "id" from core_org_chart_tree where "ileft" <> 1
                     union
                     select "idorg", "idst_ocd" as "id" from core_org_chart_tree where "ileft" <> 1
                 ) coct ON "coct"."id" = "cgm"."idst"
            JOIN core_org_chart_paths cocp
                ON "cocp"."idOrg" = "coct"."idOrg" and "cocp"."lang_code" = '${this.session.user.getLang()}'
            GROUP BY  "cu"."idst", "cocp"."lang_code"`;

        const athena = this.session.getAthena();
        await athena.connection.query(query);

        return this.puUserBranchesTable;
    }

    public createUserBranchesTable(): string {
        const paths = `IF("cocp1"."path" IS NOT NULL AND "cocp1"."path" != '', "cocp1"."path", IF("cocp2"."path" IS NOT NULL AND "cocp2"."path" != '', "cocp2"."path", "cocp3"."path"))`;
        const codes = `IF("cocp1"."code" IS NOT NULL AND "cocp1"."code" != '', "cocp1"."code", IF("cocp2"."code" IS NOT NULL AND "cocp2"."code" != '', "cocp2"."code", "cocp3"."code"))`;
        return `
            (select "cgm"."idstmember" as "idst",
                   ARRAY_JOIN(ARRAY_AGG(DISTINCT(${paths}) ORDER BY ${paths}), ', ') AS "branches",
                   ARRAY_JOIN(ARRAY_AGG(DISTINCT(${codes}) ORDER BY ${codes}), ', ') AS "codes"
            from core_group cg
            join core_group_members cgm
                ON ("cgm"."idst" = "cg"."idst" AND REGEXP_LIKE("cg"."groupid", '\\/oc[d]?_.*'))
            join
                (
                    select "idorg", "idst_oc" as "id" from core_org_chart_tree where "ileft" <> 1
                        union
                    select "idorg", "idst_ocd" as "id" from core_org_chart_tree where "ileft" <> 1
                ) coct ON "coct"."id" = "cgm"."idst"
            left join core_org_chart_paths cocp1 ON "cocp1"."idorg" = "coct"."idorg" and "cocp1"."lang_code" = '${this.session.user.getLang()}'
            left join core_org_chart_paths cocp2 ON "cocp2"."idorg" = "coct"."idorg" and "cocp2"."lang_code" = '${this.session.platform.getDefaultLanguage()}'
            left join core_org_chart_paths cocp3 ON "cocp3"."idorg" = "coct"."idorg" and "cocp3"."lang_code" = 'english'
            group by "cgm"."idstmember")`;
    }

    public async saveNewReport(platform: string, title: string, description: string, idUser: number, queryBuilderId?: string): Promise<string> {
        const dynamo = this.session.getDynamo();

        try {
            let queryBuilder = undefined;
            if (queryBuilderId) {
                queryBuilder = await dynamo.getCustomReportTypesById(queryBuilderId) as CustomReportType;
            }
            let report = this.getReportDefaultStructure(new ReportManagerInfo(), title, platform, idUser, description, queryBuilder);
            report.vILTUpdated = true;
            try {
                report = await this.onBeforeSaveNewReport(report);
            } catch (e: any) {
                this.logger.errorWithStack(`Cannot execute on before save new report - the report will be saved anyway.`, e);
            }

            await dynamo.createOrEditReport(report);
            return report.idReport;
        } catch (err: any) {
            this.logger.errorWithStack('Error on saving a new report', err);
            throw new Error('Cannot save a new report');
        }
    }

    /**
     * Step that can populate more fields of a report that is being created for the first time in dynamo
     * @param report{ReportManagerInfo} the report model
     */
    public async onBeforeSaveNewReport(report: ReportManagerInfo): Promise<ReportManagerInfo> {
        return report;
    }

    public dataResponse(data: any): ReportManagerDataResponse {
        const response = new ReportManagerDataResponse();
        response.data = data;

        return response;
    }

    public async delete(): Promise<void> {
        const dynamo = this.session.getDynamo();
        const hydra = this.session.getHydra();

        try {
            if (this.info.idReport !== '') {
                const report = await dynamo.getReport(this.info.idReport) as ReportManagerInfo;
                await dynamo.deleteReport(this.info.idReport);

                if (this.session.platform.isDatalakeV2Active() && report.planning.active) {
                    const sidekiqScheduler: SidekiqScheduler = new SidekiqScheduler(this.session.platform.getPlatformBaseUrl());
                    await sidekiqScheduler.removeScheduling(report.idReport);
                }

                const payload = {
                    entity_id: this.info.idReport,
                    entity_name: report.title ?? '',
                    entity_attributes: {
                        type: report.type ?? '',
                        description: report.description ?? '',
                        source: 'new_reports',
                        active: report.planning.active,
                        recipients: report.planning.option ? report.planning.option.recipients : undefined,
                        startHour: report.planning.option ? report.planning.option.startHour : undefined,
                        timezone: report.planning.option ? report.planning.option.timezone : undefined,
                        every: report.planning.option ? report.planning.option.every : undefined,
                        timeFrame: report.planning.option ? report.planning.option.timeFrame : undefined,
                        scheduleFrom: report.planning.option ? report.planning.option.scheduleFrom : undefined,
                    },
                    event_name: 'delete-custom-report',
                };
                await hydra.generateEventOnEventBus(payload);

            } else {
                throw new Error('Report not found!');
            }
        } catch (err: any) {
            throw err;
        }
    }

    public async updateReport(data: ReportManagerInfo): Promise<ReportManagerInfoResponse> {
        const reportId = new ReportId(this.info.idReport, this.info.platform);
        const updateReport = new ReportUpdate(
            loggerFactory.buildLogger('[ReportUpdate]', this.session.platform.getPlatformBaseUrl()),
            this.session.hydra,
            this.session.user,
            this.session.platform
        );
        data = await updateReport.execute(reportId, false, data);
        this.loadInfo(data);

        return await this.getInfo(data);
    }

    protected abstract getSortingOptions(): SortingOptions;

    /**
     * Get the default value for the course expiration date
     */
    protected getDefaultCourseExpDate(): DateOptionsValueDescriptor {
        return {
            any: true,
            days: 1,
            type: '',
            operator: '',
            to: '',
            from: ''
        };
    }

    /**
     * Return the order by clause based on the order by selection.
     * If the field selected for the ordering is not available for the user the default ordering field will be applied.
     * @param select The fields list in the select clause
     * @param translations List of the translation used for the columns names
     * @param extraFields Object with the extra fields of the platform
     * @param fromSchedule If the report is from scheduling not use the sort in the query
     */
    protected addOrderByClause(select: string[], translations: { [key: string]: string }, extraFields?: { user?: UserExtraFields[], course?: CourseExtraFields[], userCourse?: CourseuserExtraFields[], webinar?: CourseExtraFields[], classroom?: CourseExtraFields[], transcripts?: TranscriptsExtraFields[], learningPlan?: LearningPlanExtraFields[] }, fromSchedule = false): string {
        // Ignore sorting if redis setting was enabled
        if (this.session.platform.getIgnoreOrderByClause()) {
            return '';
        }

        // get the default sorting options
        const defaultSortingOptions = this.getSortingOptions();

        // order by initialized with the default sorting options
        let orderField = defaultSortingOptions.selectedField;
        let orderDirection = defaultSortingOptions.orderBy;

        const isAdditionalFieldSelected = this.info.sortingOptions.selectedField.indexOf('_extrafield_') !== -1;
        let additionalFieldNameSelected = '';
        let additionalFieldType = '';

        if (this.info.sortingOptions.selector !== 'default') {
            // if the order by field is an additional, retrieve the name of the field that match the field in the select
            if (isAdditionalFieldSelected && extraFields) {
                // get the id of the additional field
                const extraFieldId = this.info.sortingOptions.selectedField.split('_extrafield_')[1];
                const extraFieldInt = parseInt(extraFieldId, 10);
                let additionalFieldSelected;
                // extract the name of the additional field for user - course - usercourse
                if (this.isUserExtraField(this.info.sortingOptions.selectedField)) {
                    additionalFieldSelected = extraFields.user?.find(element => (element as UserExtraFields).id === extraFieldId);
                    if (additionalFieldSelected) {
                        additionalFieldNameSelected = additionalFieldSelected.title;
                        additionalFieldType = additionalFieldSelected.type;
                    }
                } else if (this.isCourseExtraField(this.info.sortingOptions.selectedField)) {
                    additionalFieldSelected = extraFields.course?.find(element => (element as CourseExtraFields).id === extraFieldInt);
                    if (additionalFieldSelected) {
                        additionalFieldNameSelected = additionalFieldSelected.name.value;
                        additionalFieldType = additionalFieldSelected.type;
                    }
                } else if (this.isCourseUserExtraField(this.info.sortingOptions.selectedField)) {
                    additionalFieldSelected = extraFields.userCourse?.find(element => (element as CourseuserExtraFields).id === extraFieldInt);
                    if (additionalFieldSelected) {
                        additionalFieldNameSelected = additionalFieldSelected.name;
                        additionalFieldType = additionalFieldSelected.type;
                    }
                } else if (this.isWebinarExtraField(this.info.sortingOptions.selectedField)) {
                    additionalFieldSelected = extraFields.webinar?.find(element => (element as CourseExtraFields).id === extraFieldInt);
                    if (additionalFieldSelected) {
                        additionalFieldNameSelected = additionalFieldSelected.name.value;
                        additionalFieldType = additionalFieldSelected.type;
                    }
                } else if (this.isClassroomExtraField(this.info.sortingOptions.selectedField)) {
                    additionalFieldSelected = extraFields.classroom?.find(element => (element as CourseExtraFields).id === extraFieldInt);
                    if (additionalFieldSelected) {
                        additionalFieldNameSelected = additionalFieldSelected.name.value;
                        additionalFieldType = additionalFieldSelected.type;
                    }
                } else if (this.isExternalActivityExtraField(this.info.sortingOptions.selectedField)) {
                    additionalFieldSelected = extraFields.transcripts?.find(element => (element as TranscriptsExtraFields).id === extraFieldInt);
                    if (additionalFieldSelected) {
                        additionalFieldNameSelected = additionalFieldSelected.title;
                        additionalFieldType = additionalFieldSelected.type;
                    }
                } else if (this.isLearningPlanExtraField(this.info.sortingOptions.selectedField)) {
                    additionalFieldSelected = extraFields.learningPlan?.find(element => (element as LearningPlanExtraFields).id === extraFieldInt);
                    if (additionalFieldSelected) {
                        additionalFieldNameSelected = additionalFieldSelected.name.value;
                        additionalFieldType = additionalFieldSelected.type;
                    }
                }
                // else if (this.isLearningPlanExtraField(this.info.sortingOptions.selectedField)) {
                //     additionalFieldSelected = extraFields.lp?.find(element => (element as CourseExtraFields).id === extraFieldInt);
                //     if (additionalFieldSelected) {
                //         additionalFieldNameSelected = additionalFieldSelected.name.value;
                //         additionalFieldType = additionalFieldSelected.type;
                //     }
                // }
            }

            // check if the order by field is in the select field, otherwise use the default
            if (this.info.sortingOptions.selectedField in translations) {
                orderField = this.info.sortingOptions.selectedField;
                orderDirection = this.info.sortingOptions.orderBy;
            }
        }

        // check if the order by field is of type string or additional field
        let addLowerFunction = false;
        if (fieldListTypeString.indexOf(orderField) !== -1 || (additionalFieldNameSelected && additionalFieldType !== 'date')) {
            addLowerFunction = !addLowerFunction;
        }
        // get the translated version of the order field or assign the name of the extrafield
        orderField = translations[orderField];
        if (additionalFieldNameSelected) {
            orderField = additionalFieldNameSelected;
            orderDirection = this.info.sortingOptions.orderBy;
        }
        const nullsLast = this.session.platform.isDatalakeV3ToggleActive() ? ' NULLS LAST' : '';
        this.querySorting = ` ORDER BY ${addLowerFunction ? 'LOWER' : ''}(${this.renderStringInQuerySelect(orderField)}) ${orderDirection}${nullsLast}`;
        if (orderField !== defaultSortingOptions.selectedField) {
            orderField = defaultSortingOptions.selectedField;
            let addLowerFunction = fieldListTypeString.includes(orderField);
            orderField = translations[orderField];
            this.querySorting += `, ${addLowerFunction ? 'LOWER' : ''}(${this.renderStringInQuerySelect(orderField)}) ${defaultSortingOptions.orderBy}${nullsLast}`;
        }
        this.querySelect = select;

        return fromSchedule ? '' : this.querySorting;
    }

    /**
     * Set the sortingOptions object with the input passed
     * @param sortingOptions The object that describes a sortingOptions
     */
    public abstract setSortingOptions(item: SortingOptions): void;


    /**
     * Delete a list of reports, in case of a PU only report created by him will be deleted
     * @param list Array of the reports to delete
     * @param session Session manager for connection management
     */
    public static async deleteReports(list: string[], session: SessionManager): Promise<MassDeleteResponse> {
        const logger: SessionLoggerService = httpContext.get('logger');
        const dynamo = session.getDynamo();
        const hydra = session.getHydra();
        const response: MassDeleteResponse = {
            success: false,
            data: {
                deleted: [],
                notDeleted: [],
                deletingErrors: []
            }
        };

        const reportToDelete: string[] = [];
        let userReports: string[] = [];

        if (session.user.isPowerUser()) {
            try {
                userReports = await dynamo.getUserIdReports(session.user.getIdUser());
            } catch (error: any) {
                throw(error);
            }
        } else {
            try {
                userReports = await dynamo.getAllIdReports();
            } catch (error: any) {
                throw(error);
            }
        }

        for (const idReport of list) {
            if (userReports.includes(idReport)) {
                reportToDelete.push(idReport);
            } else {
                response.data.notDeleted.push(idReport);
            }
        }

        for (const idReport of reportToDelete) {
            try {
                await dynamo.deleteReport(idReport);
                const report = await dynamo.getReport(idReport) as ReportManagerInfo;
                const payload = {
                    entity_id: idReport,
                    entity_name: report.title ?? '',
                    entity_attributes: {
                        type: report.type ?? '',
                        description: report.description ?? '',
                        source: 'new_reports',
                        active: report.planning.active,
                        recipients: report.planning.option ? report.planning.option.recipients : undefined,
                        startHour: report.planning.option ? report.planning.option.startHour : undefined,
                        timezone: report.planning.option ? report.planning.option.timezone : undefined,
                        every: report.planning.option ? report.planning.option.every : undefined,
                        timeFrame: report.planning.option ? report.planning.option.timeFrame : undefined,
                        scheduleFrom: report.planning.option ? report.planning.option.scheduleFrom : undefined,
                    },
                    event_name: 'delete-custom-report',
                };
                await hydra.generateEventOnEventBus(payload);

                response.data.deleted.push(idReport);
            } catch (error: any) {
                logger.errorWithStack('Error during report deletion', error);
                response.data.deletingErrors.push(idReport);
            }
        }

        response.success = true;

        return response;
    }

    public abstract getAvailablesFields(): Promise<ReportAvailablesFields>;

    /**
     * Get all fields without extra fields
     */
    public abstract getBaseAvailableFields(): Promise<ReportAvailablesFields>;

    public translateField(field: string): string {
        switch (field) {
            default:
                return field;
        }
    }

    /**
     * If DatalakeV3 is active the field is being convert to lowercase with s/e "
     * you can also pass a value like table.Field
     * @param value
     * @protected
     */
    protected convertToDatalakeV3 (value: string): string {
        // If toggle off, or it ends with ) as a function do not convert or the conversion in lowercase is not needed
        if (!this.session.platform.isDatalakeV3ToggleActive() || value.endsWith(')')) {
            return value;
        }
        let table = '';
        if (value.indexOf('.')) {
            table = value.substring(0, value.indexOf('.') + 1);
            value = value.substring(value.indexOf('.') + 1);
        }
        value = value.toLowerCase();
        if (value.startsWith('"') && value.endsWith('"')) {
            return table + value;
        }
        return table + '"' + value + '"';
    }

    protected composeTableField (table: string, field: string): string {
        if (!this.session.platform.isDatalakeV3ToggleActive()) {
            return `${table}.${field}`;
        }

        return `${table}.${this.convertToDatalakeV3(field)}`;
    }

    public buildDateFilter(column: string, filter: DateOptionsValueDescriptor, operand = 'AND', isDatetime = false, preserveColumnCase = false): string {
        if (filter.any || column === '') {
            return '';
        }
        column = preserveColumnCase ? column : this.convertToDatalakeV3(column);

        switch (filter.operator) {
            case 'isAfter':
                switch (filter.type) {
                    case 'relative':
                        const date = new Date();
                        date.setDate(date.getDate() - filter.days);
                        return ' ' + operand + ' ' + column + " > TIMESTAMP '" + this.convertDateObjectToDate(date) + (isDatetime ? ' 23:59:59' : '') + "'";
                    case 'absolute':
                        if (filter.to !== '') {
                            return ' ' + operand + ' ' + column + " > TIMESTAMP '" + filter.to + (isDatetime ? ' 23:59:59' : '') + "'";
                        }
                        return '';
                    default:
                        return '';
                }
            case 'isBefore':
                switch (filter.type) {
                    case 'relative':
                        const date = new Date();
                        date.setDate(date.getDate() - filter.days);
                        return ' ' + operand + ' ' + column + " < TIMESTAMP '" + this.convertDateObjectToDate(date) + (isDatetime ? ' 00:00:00' : '') + "'";
                    case 'absolute':
                        if (filter.to !== '') {
                            return ' ' + operand + ' ' + column + " < TIMESTAMP '" + filter.to + (isDatetime ? ' 00:00:00' : '') + "'";
                        }
                        return '';
                    default:
                        return '';
                }
            case 'range':
                if (filter.from !== '' && filter.to !== '') {
                    return ' ' + operand + ' ' + column + " BETWEEN TIMESTAMP '" + filter.from + (isDatetime ? ' 00:00:00' : '') + "' AND TIMESTAMP '" + filter.to + (isDatetime ? ' 23:59:59' : '') + "'";
                }
                return '';
            case 'expiringIn':
                const date = new Date();
                const dateTo = new Date();
                dateTo.setDate(dateTo.getDate() + filter.days);
                return ' ' + operand + ' ' + column + " BETWEEN TIMESTAMP '" + this.convertDateObjectToDate(date) + " 00:00:00' AND TIMESTAMP '" + this.convertDateObjectToDate(dateTo) + " 23:59:59'";
            default:
                return '';
        }
    }

    /**
     * Get the default value for the planning
     */
    protected getDefaultPlanningFields(): Planning {
        return {
            active: false,
            option: {
                isPaused: false,
                recipients: [],
                every: 1,
                timeFrame: TimeFrameOptions.days,
                scheduleFrom: '',
                startHour: '00:00',
                timezone: this.session.user.getTimezone(),
            }
        };
    }

    /**
     * Return the report name prepared to be available has a file name suitably for a file name
     * @param length The lenght of the returned string, 0 for no cut of the string
     */
    public getExportReportName(length = 10): string {
        const placeholder = 'report';
        let str = slugify(this.info.title, {replacement: '_', remove: /[*+~.()'"!:@?]/g});
        // check the length of the slug result, if zero use the placeholder
        if (str.length === 0) {
            str = placeholder;
        }
        if (length > 0) {
            return str.substr(0, length);
        }
        return str;
    }

    /**
     * Get the sql like LIMIT clause
     * @param limit The numeric limit to apply to the LIMIT clause
     */
    protected getQueryExportLimit(customLimit: number): string {
        return ' LIMIT ' + customLimit;
    }

    public async loadTranslations(allFields = false): Promise<{ [key: string]: string }> {
        const hydra = this.session.getHydra();
        const toTranslate: Translations = {
            translations: {},
            lang_code: this.session.user.getLangCode()
        };

        let fields: string[] = [];

        if (allFields === true) {
            fields = fields.concat(this.allFields.assets ? this.allFields.assets : []);
            fields = fields.concat(this.allFields.badge ? this.allFields.badge : []);
            fields = fields.concat(this.allFields.badgeAssignment ? this.allFields.badgeAssignment : []);
            fields = fields.concat(this.allFields.certifications ? this.allFields.certifications : []);
            fields = fields.concat(this.allFields.contentPartners ? this.allFields.contentPartners : []);
            fields = fields.concat(this.allFields.course ? this.allFields.course : []);
            fields = fields.concat(this.allFields.courseuser ? this.allFields.courseuser : []);
            fields = fields.concat(this.allFields.ecommerceTransaction ? this.allFields.ecommerceTransaction : []);
            fields = fields.concat(this.allFields.ecommerceTransactionItem ? this.allFields.ecommerceTransactionItem : []);
            fields = fields.concat(this.allFields.externalTraining ? this.allFields.externalTraining : []);
            fields = fields.concat(this.allFields.group ? this.allFields.group : []);
            fields = fields.concat(this.allFields.lp ? this.allFields.lp : []);
            fields = fields.concat(this.allFields.lpenrollment ? this.allFields.lpenrollment : []);
            fields = fields.concat(this.allFields.learningPlansStatistics ? this.allFields.learningPlansStatistics : []);
            fields = fields.concat(this.allFields.courseEnrollments ? this.allFields.courseEnrollments : []);
            fields = fields.concat(this.allFields.session ? this.allFields.session : []);
            fields = fields.concat(this.allFields.event ? this.allFields.event : []);
            fields = fields.concat(this.allFields.enrollment ? this.allFields.enrollment : []);
            fields = fields.concat(this.allFields.statistics ? this.allFields.statistics : []);
            fields = fields.concat(this.allFields.usageStatistics ? this.allFields.usageStatistics : []);
            fields = fields.concat(this.allFields.mobileAppStatistics ? this.allFields.mobileAppStatistics : []);
            fields = fields.concat(this.allFields.flowStatistics ? this.allFields.flowStatistics : []);
            fields = fields.concat(this.allFields.flowMsTeamsStatistics ? this.allFields.flowMsTeamsStatistics : []);
            fields = fields.concat(this.allFields.trainingMaterials ? this.allFields.trainingMaterials : []);
            fields = fields.concat(this.allFields.user ? this.allFields.user : []);
            fields = fields.concat(this.allFields.webinarSessionUser ? this.allFields.webinarSessionUser : []);
            fields = fields.concat(this.allFields.survey ? this.allFields.survey : []);
            fields = fields.concat(this.allFields.surveyQuestionAnswer ? this.allFields.surveyQuestionAnswer : []);
        } else {
            fields = fields.concat(this.info.fields);
        }

        if (fields) {
            for (const field of fields) {
                switch (field) {
                    case FieldsList.USER_DEACTIVATED:
                    case FieldsList.USER_EMAIL_VALIDATION_STATUS:
                    case FieldsList.COURSE_E_SIGNATURE:
                    case FieldsList.COURSE_EXPIRED:
                    case FieldsList.CONTENT_PARTNERS_AFFILIATE:
                    case FieldsList.USER_MANAGER_PERMISSIONS:
                    case FieldsList.USER_AUTH_APP_PAIRED:
                    case FieldsList.WATCHED:
                    case FieldsList.NOT_WATCHED:
                    case FieldsList.STATS_USER_FLOW_YES_NO:
                    case FieldsList.STATS_USER_FLOW_MS_TEAMS_YES_NO:
                    case FieldsList.STATS_COURSE_ACCESS_FROM_MOBILE:
                    case FieldsList.ENROLLMENT_ARCHIVED:
                    case FieldsList.QUESTION_MANDATORY:
                        toTranslate.translations[FieldTranslation.YES] = fieldTranslationsKey[FieldTranslation.YES];
                        toTranslate.translations[FieldTranslation.NO] = fieldTranslationsKey[FieldTranslation.NO];
                        toTranslate.translations[field] = fieldTranslationsKey[field];
                        break;
                    case FieldsList.USER_LEVEL:
                        toTranslate.translations[FieldTranslation.USER_LEVEL_USER] = fieldTranslationsKey[FieldTranslation.USER_LEVEL_USER];
                        toTranslate.translations[FieldTranslation.USER_LEVEL_POWERUSER] = fieldTranslationsKey[FieldTranslation.USER_LEVEL_POWERUSER];
                        toTranslate.translations[FieldTranslation.USER_LEVEL_GODADMIN] = fieldTranslationsKey[FieldTranslation.USER_LEVEL_GODADMIN];
                        toTranslate.translations[field] = fieldTranslationsKey[field];
                        break;
                    case FieldsList.COURSE_STATUS:
                        toTranslate.translations[FieldTranslation.COURSE_STATUS_PREPARATION] = fieldTranslationsKey[FieldTranslation.COURSE_STATUS_PREPARATION];
                        toTranslate.translations[FieldTranslation.COURSE_STATUS_EFFECTIVE] = fieldTranslationsKey[FieldTranslation.COURSE_STATUS_EFFECTIVE];
                        toTranslate.translations[field] = fieldTranslationsKey[field];
                        break;
                    case FieldsList.COURSE_TYPE:
                        toTranslate.translations[FieldTranslation.COURSE_TYPE_ELEARNING] = fieldTranslationsKey[FieldTranslation.COURSE_TYPE_ELEARNING];
                        toTranslate.translations[FieldTranslation.COURSE_TYPE_CLASSROOM] = fieldTranslationsKey[FieldTranslation.COURSE_TYPE_CLASSROOM];
                        toTranslate.translations[FieldTranslation.COURSE_TYPE_WEBINAR] = fieldTranslationsKey[FieldTranslation.COURSE_TYPE_WEBINAR];
                        toTranslate.translations[field] = fieldTranslationsKey[field];
                        break;
                    case FieldsList.ECOMMERCE_TRANSACTION_PAYMENT_STATUS:
                        toTranslate.translations[FieldTranslation.PAYMENT_STATUS_CANCELED] = fieldTranslationsKey[FieldTranslation.PAYMENT_STATUS_CANCELED];
                        toTranslate.translations[FieldTranslation.PAYMENT_STATUS_PENDING] = fieldTranslationsKey[FieldTranslation.PAYMENT_STATUS_PENDING];
                        toTranslate.translations[FieldTranslation.PAYMENT_STATUS_SUCCESSFUL] = fieldTranslationsKey[FieldTranslation.PAYMENT_STATUS_SUCCESSFUL];
                        toTranslate.translations[FieldTranslation.PAYMENT_STATUS_FAILED] = fieldTranslationsKey[FieldTranslation.PAYMENT_STATUS_FAILED];
                        toTranslate.translations[field] = fieldTranslationsKey[field];
                        break;
                    case FieldsList.ECOMMERCE_TRANSACTION_ITEM_TYPE:
                        toTranslate.translations[FieldTranslation.COURSE] = fieldTranslationsKey[FieldTranslation.COURSE];
                        toTranslate.translations[FieldTranslation.COURSEPATH] = fieldTranslationsKey[FieldTranslation.COURSEPATH];
                        toTranslate.translations[FieldTranslation.COURSESEATS] = fieldTranslationsKey[FieldTranslation.COURSESEATS];
                        toTranslate.translations[FieldTranslation.SUBSCRIPTION_PLAN] = fieldTranslationsKey[FieldTranslation.SUBSCRIPTION_PLAN];
                        toTranslate.translations[field] = fieldTranslationsKey[field];
                        break;
                    case FieldsList.COURSEUSER_LEVEL:
                    case FieldsList.WEBINAR_SESSION_USER_LEVEL:
                    case FieldsList.ENROLLMENT_USER_COURSE_LEVEL:
                        toTranslate.translations[FieldTranslation.COURSEUSER_LEVEL_STUDENT] = fieldTranslationsKey[FieldTranslation.COURSEUSER_LEVEL_STUDENT];
                        toTranslate.translations[FieldTranslation.COURSEUSER_LEVEL_TUTOR] = fieldTranslationsKey[FieldTranslation.COURSEUSER_LEVEL_TUTOR];
                        toTranslate.translations[FieldTranslation.COURSEUSER_LEVEL_TEACHER] = fieldTranslationsKey[FieldTranslation.COURSEUSER_LEVEL_TEACHER];
                        toTranslate.translations[field] = fieldTranslationsKey[field];
                        break;
                    case FieldsList.COURSEUSER_STATUS:
                    case FieldsList.LP_ENROLLMENT_STATUS:
                    case FieldsList.COURSE_ENROLLMENT_STATUS:
                    case FieldsList.WEBINAR_SESSION_USER_STATUS:
                    case FieldsList.WEBINAR_SESSION_USER_ENROLLMENT_STATUS:
                    case FieldsList.ENROLLMENT_ENROLLMENT_STATUS:
                    case FieldsList.ENROLLMENT_USER_SESSION_STATUS:
                        toTranslate.translations[FieldTranslation.COURSEUSER_STATUS_WAITING_LIST] = fieldTranslationsKey[FieldTranslation.COURSEUSER_STATUS_WAITING_LIST];
                        toTranslate.translations[FieldTranslation.COURSEUSER_STATUS_CONFIRMED] = fieldTranslationsKey[FieldTranslation.COURSEUSER_STATUS_CONFIRMED];
                        toTranslate.translations[FieldTranslation.COURSEUSER_STATUS_SUBSCRIBED] = fieldTranslationsKey[FieldTranslation.COURSEUSER_STATUS_SUBSCRIBED];
                        toTranslate.translations[FieldTranslation.COURSEUSER_STATUS_IN_PROGRESS] = fieldTranslationsKey[FieldTranslation.COURSEUSER_STATUS_IN_PROGRESS];
                        toTranslate.translations[FieldTranslation.COURSEUSER_STATUS_COMPLETED] = fieldTranslationsKey[FieldTranslation.COURSEUSER_STATUS_COMPLETED];
                        toTranslate.translations[FieldTranslation.COURSEUSER_STATUS_SUSPENDED] = fieldTranslationsKey[FieldTranslation.COURSEUSER_STATUS_SUSPENDED];
                        toTranslate.translations[FieldTranslation.COURSEUSER_STATUS_OVERBOOKING] = fieldTranslationsKey[FieldTranslation.COURSEUSER_STATUS_OVERBOOKING];
                        toTranslate.translations[FieldTranslation.COURSEUSER_STATUS_WAITING] = fieldTranslationsKey[FieldTranslation.COURSEUSER_STATUS_WAITING];
                        toTranslate.translations[FieldTranslation.COURSEUSER_STATUS_ENROLLMENTS_TO_CONFIRM] = fieldTranslationsKey[FieldTranslation.COURSEUSER_STATUS_ENROLLMENTS_TO_CONFIRM];
                        toTranslate.translations[FieldTranslation.COURSEUSER_STATUS_ENROLLED] = fieldTranslationsKey[FieldTranslation.COURSEUSER_STATUS_ENROLLED];
                        toTranslate.translations[field] = fieldTranslationsKey[field];
                        // DOC-27534 - https://docebo.atlassian.net/browse/DOC-27534
                        if (this.info.type === ReportsTypes.USERS_COURSES || this.info.type === ReportsTypes.MANAGER_USERS_COURSES) {
                            toTranslate.translations[field] = fieldTranslationsKey[FieldTranslation.COURSEUSER_STATUS_NEW_TRANSLATION];
                        }
                        break;
                    case FieldsList.WEBINAR_SESSION_USER_EVAL_STATUS:
                    case FieldsList.ENROLLMENT_EVALUATION_STATUS:
                        toTranslate.translations[FieldTranslation.WEBINAR_SESSION_USER_EVAL_STATUS_FAILED] = fieldTranslationsKey[FieldTranslation.WEBINAR_SESSION_USER_EVAL_STATUS_FAILED];
                        toTranslate.translations[FieldTranslation.WEBINAR_SESSION_USER_EVAL_STATUS_PASSED] = fieldTranslationsKey[FieldTranslation.WEBINAR_SESSION_USER_EVAL_STATUS_PASSED];
                        toTranslate.translations[field] = fieldTranslationsKey[field];
                        break;
                    case FieldsList.COURSEUSER_ASSIGNMENT_TYPE:
                    case FieldsList.LP_ENROLLMENT_ASSIGNMENT_TYPE:
                        toTranslate.translations[FieldTranslation.ASSIGNMENT_TYPE_MANDATORY] = fieldTranslationsKey[FieldTranslation.ASSIGNMENT_TYPE_MANDATORY];
                        toTranslate.translations[FieldTranslation.ASSIGNMENT_TYPE_REQUIRED] = fieldTranslationsKey[FieldTranslation.ASSIGNMENT_TYPE_REQUIRED];
                        toTranslate.translations[FieldTranslation.ASSIGNMENT_TYPE_RECOMMENDED] = fieldTranslationsKey[FieldTranslation.ASSIGNMENT_TYPE_RECOMMENDED];
                        toTranslate.translations[FieldTranslation.ASSIGNMENT_TYPE_OPTIONAL] = fieldTranslationsKey[FieldTranslation.ASSIGNMENT_TYPE_OPTIONAL];
                        toTranslate.translations[field] = fieldTranslationsKey[field];
                        break;
                    case FieldsList.LO_BOOKMARK:
                        toTranslate.translations[FieldTranslation.LO_BOOKMARK_START] = fieldTranslationsKey[FieldTranslation.LO_BOOKMARK_START];
                        toTranslate.translations[FieldTranslation.LO_BOOKMARK_FINAL] = fieldTranslationsKey[FieldTranslation.LO_BOOKMARK_FINAL];
                        toTranslate.translations[field] = fieldTranslationsKey[field];
                        break;
                    case FieldsList.LO_STATUS:
                        toTranslate.translations[FieldTranslation.LO_STATUS_COMPLETED] = fieldTranslationsKey[FieldTranslation.LO_STATUS_COMPLETED];
                        toTranslate.translations[FieldTranslation.LO_STATUS_FAILED] = fieldTranslationsKey[FieldTranslation.LO_STATUS_FAILED];
                        toTranslate.translations[FieldTranslation.LO_STATUS_IN_ITINERE] = fieldTranslationsKey[FieldTranslation.LO_STATUS_IN_ITINERE];
                        toTranslate.translations[FieldTranslation.LO_STATUS_NOT_STARTED] = fieldTranslationsKey[FieldTranslation.LO_STATUS_NOT_STARTED];
                        toTranslate.translations[field] = fieldTranslationsKey[field];
                        break;
                    case FieldsList.LO_TYPE:
                        toTranslate.translations[FieldTranslation.LO_TYPE_AUTHORING] = fieldTranslationsKey[FieldTranslation.LO_TYPE_AUTHORING];
                        toTranslate.translations[FieldTranslation.LO_TYPE_DELIVERABLE] = fieldTranslationsKey[FieldTranslation.LO_TYPE_DELIVERABLE];
                        toTranslate.translations[FieldTranslation.LO_TYPE_FILE] = fieldTranslationsKey[FieldTranslation.LO_TYPE_FILE];
                        toTranslate.translations[FieldTranslation.LO_TYPE_HTMLPAGE] = fieldTranslationsKey[FieldTranslation.LO_TYPE_HTMLPAGE];
                        toTranslate.translations[FieldTranslation.LO_TYPE_POLL] = fieldTranslationsKey[FieldTranslation.LO_TYPE_POLL];
                        toTranslate.translations[FieldTranslation.LO_TYPE_SCORM] = fieldTranslationsKey[FieldTranslation.LO_TYPE_SCORM];
                        toTranslate.translations[FieldTranslation.LO_TYPE_TEST] = fieldTranslationsKey[FieldTranslation.LO_TYPE_TEST];
                        toTranslate.translations[FieldTranslation.LO_TYPE_TINCAN] = fieldTranslationsKey[FieldTranslation.LO_TYPE_TINCAN];
                        toTranslate.translations[FieldTranslation.LO_TYPE_VIDEO] = fieldTranslationsKey[FieldTranslation.LO_TYPE_VIDEO];
                        toTranslate.translations[FieldTranslation.LO_TYPE_AICC] = fieldTranslationsKey[FieldTranslation.LO_TYPE_AICC];
                        toTranslate.translations[FieldTranslation.LO_TYPE_ELUCIDAT] = fieldTranslationsKey[FieldTranslation.LO_TYPE_ELUCIDAT];
                        toTranslate.translations[FieldTranslation.LO_TYPE_GOOGLEDRIVE] = fieldTranslationsKey[FieldTranslation.LO_TYPE_GOOGLEDRIVE];
                        toTranslate.translations[FieldTranslation.LO_TYPE_LTI] = fieldTranslationsKey[FieldTranslation.LO_TYPE_LTI];

                        toTranslate.translations[field] = fieldTranslationsKey[field];
                        break;
                    case FieldsList.CERTIFICATION_DURATION:
                        toTranslate.translations[FieldTranslation.DAYS] = fieldTranslationsKey[FieldTranslation.DAYS];
                        toTranslate.translations[FieldTranslation.WEEKS] = fieldTranslationsKey[FieldTranslation.WEEKS];
                        toTranslate.translations[FieldTranslation.MONTHS] = fieldTranslationsKey[FieldTranslation.MONTHS];
                        toTranslate.translations[FieldTranslation.YEARS] = fieldTranslationsKey[FieldTranslation.YEARS];
                        toTranslate.translations[FieldTranslation.NEVER] = fieldTranslationsKey[FieldTranslation.NEVER];
                        toTranslate.translations[field] = fieldTranslationsKey[field];
                        break;
                    case FieldsList.CERTIFICATION_STATUS:
                        toTranslate.translations[FieldTranslation.CERTIFICATION_ACTIVE] = fieldTranslationsKey[FieldTranslation.CERTIFICATION_ACTIVE];
                        toTranslate.translations[FieldTranslation.CERTIFICATION_EXPIRED] = fieldTranslationsKey[FieldTranslation.CERTIFICATION_EXPIRED];
                        toTranslate.translations[FieldTranslation.CERTIFICATION_ARCHIVED] = fieldTranslationsKey[FieldTranslation.CERTIFICATION_ARCHIVED];
                        toTranslate.translations[field] = fieldTranslationsKey[field];
                        break;
                    case FieldsList.EXTERNAL_TRAINING_STATUS:
                        toTranslate.translations[FieldTranslation.EXTERNAL_TRAINING_STATUS_APPROVED] = fieldTranslationsKey[FieldTranslation.EXTERNAL_TRAINING_STATUS_APPROVED];
                        toTranslate.translations[FieldTranslation.EXTERNAL_TRAINING_STATUS_WAITING] = fieldTranslationsKey[FieldTranslation.EXTERNAL_TRAINING_STATUS_WAITING];
                        toTranslate.translations[FieldTranslation.EXTERNAL_TRAINING_STATUS_REJECTED] = fieldTranslationsKey[FieldTranslation.EXTERNAL_TRAINING_STATUS_REJECTED];
                        toTranslate.translations[field] = fieldTranslationsKey[field];
                        break;
                    case FieldsList.ECOMMERCE_TRANSACTION_PAYMENT_METHOD:
                        toTranslate.translations[FieldTranslation.FREE_PURCHASE] = fieldTranslationsKey[FieldTranslation.FREE_PURCHASE];
                        toTranslate.translations[field] = fieldTranslationsKey[field];
                        break;
                    case FieldsList.ASSET_TYPE:
                        toTranslate.translations[FieldTranslation.VIDEO] = fieldTranslationsKey[FieldTranslation.VIDEO];
                        toTranslate.translations[FieldTranslation.DOC] = fieldTranslationsKey[FieldTranslation.DOC];
                        toTranslate.translations[FieldTranslation.EXCEL] = fieldTranslationsKey[FieldTranslation.EXCEL];
                        toTranslate.translations[FieldTranslation.PPT] = fieldTranslationsKey[FieldTranslation.PPT];
                        toTranslate.translations[FieldTranslation.PDF] = fieldTranslationsKey[FieldTranslation.PDF];
                        toTranslate.translations[FieldTranslation.TEXT] = fieldTranslationsKey[FieldTranslation.TEXT];
                        toTranslate.translations[FieldTranslation.IMAGE] = fieldTranslationsKey[FieldTranslation.IMAGE];
                        toTranslate.translations[FieldTranslation.QUESTION] = fieldTranslationsKey[FieldTranslation.QUESTION];
                        toTranslate.translations[FieldTranslation.RESPONSE] = fieldTranslationsKey[FieldTranslation.RESPONSE];
                        toTranslate.translations[FieldTranslation.OTHER] = fieldTranslationsKey[FieldTranslation.OTHER];
                        toTranslate.translations[FieldTranslation.DEFAULT_OTHER] = fieldTranslationsKey[FieldTranslation.DEFAULT_OTHER];
                        toTranslate.translations[FieldTranslation.DEFAULT_MUSIC] = fieldTranslationsKey[FieldTranslation.DEFAULT_MUSIC];
                        toTranslate.translations[FieldTranslation.DEFAULT_ARCHIVE] = fieldTranslationsKey[FieldTranslation.DEFAULT_ARCHIVE];
                        toTranslate.translations[FieldTranslation.LINKS] = fieldTranslationsKey[FieldTranslation.LINKS];
                        toTranslate.translations[FieldTranslation.GOOGLE_DRIVE_DOCS] = fieldTranslationsKey[FieldTranslation.GOOGLE_DRIVE_DOCS];
                        toTranslate.translations[FieldTranslation.GOOGLE_DRIVE_SHEETS] = fieldTranslationsKey[FieldTranslation.GOOGLE_DRIVE_SHEETS];
                        toTranslate.translations[FieldTranslation.GOOGLE_DRIVE_SLIDES] = fieldTranslationsKey[FieldTranslation.GOOGLE_DRIVE_SLIDES];
                        toTranslate.translations[FieldTranslation.PLAYLIST] = fieldTranslationsKey[FieldTranslation.PLAYLIST];
                        toTranslate.translations[FieldTranslation.YOUTUBE] = fieldTranslationsKey[FieldTranslation.YOUTUBE];
                        toTranslate.translations[FieldTranslation.VIMEO] = fieldTranslationsKey[FieldTranslation.VIMEO];
                        toTranslate.translations[FieldTranslation.WISTIA] = fieldTranslationsKey[FieldTranslation.WISTIA];
                        toTranslate.translations[field] = fieldTranslationsKey[field];
                        break;
                    case FieldsList.SESSION_ATTENDANCE_TYPE:
                    case FieldsList.SESSION_EVENT_TYPE:
                        toTranslate.translations[FieldTranslation.SESSION_ATTENDANCE_TYPE_BLEENDED] = fieldTranslationsKey[FieldTranslation.SESSION_ATTENDANCE_TYPE_BLEENDED];
                        toTranslate.translations[FieldTranslation.SESSION_ATTENDANCE_TYPE_FLEXIBLE] = fieldTranslationsKey[FieldTranslation.SESSION_ATTENDANCE_TYPE_FLEXIBLE];
                        toTranslate.translations[FieldTranslation.SESSION_ATTENDANCE_TYPE_FULLONLINE] = fieldTranslationsKey[FieldTranslation.SESSION_ATTENDANCE_TYPE_FULLONLINE];
                        toTranslate.translations[FieldTranslation.SESSION_ATTENDANCE_TYPE_FULLONSITE] = fieldTranslationsKey[FieldTranslation.SESSION_ATTENDANCE_TYPE_FULLONSITE];
                        toTranslate.translations[field] = fieldTranslationsKey[field];
                        break;
                    case FieldsList.ENROLLMENT_USER_SESSION_EVENT_ATTENDANCE_STATUS:
                        toTranslate.translations[FieldTranslation.ENROLLMENT_USER_SESSION_EVENT_ATTENDANCE_STATUS_PRESENT] = fieldTranslationsKey[FieldTranslation.ENROLLMENT_USER_SESSION_EVENT_ATTENDANCE_STATUS_PRESENT];
                        toTranslate.translations[FieldTranslation.ENROLLMENT_USER_SESSION_EVENT_ATTENDANCE_STATUS_ABSENT] = fieldTranslationsKey[FieldTranslation.ENROLLMENT_USER_SESSION_EVENT_ATTENDANCE_STATUS_ABSENT];
                        toTranslate.translations[FieldTranslation.ENROLLMENT_USER_SESSION_EVENT_ATTENDANCE_STATUS_NOT_SET] = fieldTranslationsKey[FieldTranslation.ENROLLMENT_USER_SESSION_EVENT_ATTENDANCE_STATUS_NOT_SET];
                        toTranslate.translations[field] = fieldTranslationsKey[field];
                        break;
                    case FieldsList.SURVEY_TRACKING_TYPE:
                        toTranslate.translations[FieldTranslation.LOCAL_TRACKING] = fieldTranslationsKey[FieldTranslation.LOCAL_TRACKING];
                        toTranslate.translations[FieldTranslation.SHARED_TRACKING] = fieldTranslationsKey[FieldTranslation.SHARED_TRACKING];
                        toTranslate.translations[field] = fieldTranslationsKey[field];
                        break;
                    case FieldsList.QUESTION_TYPE:
                        toTranslate.translations[FieldTranslation.CHOICE] = fieldTranslationsKey[FieldTranslation.CHOICE];
                        toTranslate.translations[FieldTranslation.CHOICE_MULTIPLE] = fieldTranslationsKey[FieldTranslation.CHOICE_MULTIPLE];
                        toTranslate.translations[FieldTranslation.INLINE_CHOICE] = fieldTranslationsKey[FieldTranslation.INLINE_CHOICE];
                        toTranslate.translations[FieldTranslation.EXTENDED_TEXT] = fieldTranslationsKey[FieldTranslation.EXTENDED_TEXT];
                        toTranslate.translations[FieldTranslation.LIKERT_SCALE] = fieldTranslationsKey[FieldTranslation.LIKERT_SCALE];
                        toTranslate.translations[field] = fieldTranslationsKey[field];
                        break;
                    case FieldsList.LP_STATUS:
                        toTranslate.translations[FieldTranslation.LP_UNDER_MAINTENANCE] = fieldTranslationsKey[FieldTranslation.LP_UNDER_MAINTENANCE];
                        toTranslate.translations[FieldTranslation.LP_PUBLISHED] = fieldTranslationsKey[FieldTranslation.LP_PUBLISHED];
                        toTranslate.translations[field] = fieldTranslationsKey[field];
                        break;
                    case FieldsList.LP_STAT_DURATION:
                    case FieldsList.LP_STAT_DURATION_MANDATORY:
                    case FieldsList.LP_STAT_DURATION_OPTIONAL:
                        toTranslate.translations[FieldTranslation.HR] = fieldTranslationsKey[FieldTranslation.HR];
                        toTranslate.translations[FieldTranslation.MIN] = fieldTranslationsKey[FieldTranslation.MIN];
                        toTranslate.translations[field] = fieldTranslationsKey[field];
                        break;
                    case FieldsList.USER_ID:
                    case FieldsList.USER_USERID:
                    case FieldsList.USER_FIRSTNAME:
                    case FieldsList.USER_LASTNAME:
                    case FieldsList.USER_FULLNAME:
                    case FieldsList.USER_EMAIL:
                    case FieldsList.USER_EXPIRATION:
                    case FieldsList.USER_SUSPEND_DATE:
                    case FieldsList.USER_REGISTER_DATE:
                    case FieldsList.USER_LAST_ACCESS_DATE:
                    case FieldsList.USER_BRANCH_NAME:
                    case FieldsList.USER_BRANCH_PATH:
                    case FieldsList.USER_BRANCHES_CODES:
                    case FieldsList.USER_TIMEZONE:
                    case FieldsList.USER_LANGUAGE:
                    case FieldsList.USER_DIRECT_MANAGER:
                    case FieldsList.COURSE_ID:
                    case FieldsList.COURSE_CODE:
                    case FieldsList.COURSE_NAME:
                    case FieldsList.COURSE_CATEGORY_CODE:
                    case FieldsList.COURSE_CATEGORY_NAME:
                    case FieldsList.COURSE_CREDITS:
                    case FieldsList.COURSE_DURATION:
                    case FieldsList.COURSE_DATE_BEGIN:
                    case FieldsList.COURSE_DATE_END:
                    case FieldsList.COURSE_CREATION_DATE:
                    case FieldsList.COURSE_E_SIGNATURE_HASH:
                    case FieldsList.COURSE_LANGUAGE:
                    case FieldsList.LP_COURSE_LANGUAGE:
                    case FieldsList.COURSE_ENROLLMENT_DATE_INSCR:
                    case FieldsList.COURSE_ENROLLMENT_DATE_COMPLETE:
                    case FieldsList.COURSE_ENROLLMENT_DATE_BEGIN_VALIDITY:
                    case FieldsList.COURSE_ENROLLMENT_DATE_EXPIRE_VALIDITY:
                    case FieldsList.COURSEUSER_DATE_INSCR:
                    case FieldsList.COURSEUSER_DATE_FIRST_ACCESS:
                    case FieldsList.COURSEUSER_DATE_LAST_ACCESS:
                    case FieldsList.COURSEUSER_DATE_COMPLETE:
                    case FieldsList.COURSEUSER_EXPIRATION_DATE:
                    case FieldsList.COURSEUSER_DATE_BEGIN_VALIDITY:
                    case FieldsList.COURSEUSER_DATE_EXPIRE_VALIDITY:
                    case FieldsList.COURSEUSER_SCORE_GIVEN:
                    case FieldsList.COURSEUSER_INITIAL_SCORE_GIVEN:
                    case FieldsList.COURSEUSER_DAYS_LEFT:
                    case FieldsList.ENROLLMENT_ARCHIVING_DATE:
                    case FieldsList.GROUP_GROUP_OR_BRANCH_NAME:
                    case FieldsList.GROUP_MEMBERS_COUNT:
                    case FieldsList.STATS_USER_COURSE_COMPLETION_PERCENTAGE:
                    case FieldsList.STATS_TOTAL_SESSIONS_IN_COURSE:
                    case FieldsList.STATS_NUMBER_OF_ACTIONS:
                    case FieldsList.STATS_ENROLLED_USERS:
                    case FieldsList.STATS_USERS_ENROLLED_IN_COURSE:
                    case FieldsList.STATS_NOT_STARTED_USERS:
                    case FieldsList.STATS_NOT_STARTED_USERS_PERCENTAGE:
                    case FieldsList.STATS_IN_PROGRESS_USERS:
                    case FieldsList.STATS_IN_PROGRESS_USERS_PERCENTAGE:
                    case FieldsList.STATS_COMPLETED_USERS:
                    case FieldsList.STATS_COMPLETED_USERS_PERCENTAGE:
                    case FieldsList.STATS_PATH_ENROLLED_USERS:
                    case FieldsList.STATS_PATH_NOT_STARTED_USERS:
                    case FieldsList.STATS_PATH_NOT_STARTED_USERS_PERCENTAGE:
                    case FieldsList.STATS_PATH_IN_PROGRESS_USERS:
                    case FieldsList.STATS_PATH_IN_PROGRESS_USERS_PERCENTAGE:
                    case FieldsList.STATS_PATH_COMPLETED_USERS:
                    case FieldsList.STATS_PATH_COMPLETED_USERS_PERCENTAGE:
                    case FieldsList.STATS_TOTAL_TIME_IN_COURSE:
                    case FieldsList.STATS_COURSE_RATING:
                    case FieldsList.STATS_ACTIVE:
                    case FieldsList.STATS_EXPIRED:
                    case FieldsList.STATS_ISSUED:
                    case FieldsList.STATS_ARCHIVED:
                    case FieldsList.WEBINAR_SESSION_USER_ENROLL_DATE:
                    case FieldsList.WEBINAR_SESSION_USER_LEARN_EVAL:
                    case FieldsList.WEBINAR_SESSION_USER_INSTRUCTOR_FEEDBACK:
                    case FieldsList.WEBINAR_SESSION_NAME:
                    case FieldsList.WEBINAR_SESSION_EVALUATION_SCORE_BASE:
                    case FieldsList.WEBINAR_SESSION_START_DATE:
                    case FieldsList.WEBINAR_SESSION_END_DATE:
                    case FieldsList.WEBINAR_SESSION_WEBINAR_TOOL:
                    case FieldsList.WEBINAR_SESSION_TOOL_TIME_IN_SESSION:
                    case FieldsList.WEBINAR_SESSION_SESSION_TIME:
                    case FieldsList.LP_NAME:
                    case FieldsList.LP_CODE:
                    case FieldsList.LP_CREDITS:
                    case FieldsList.LP_ENROLLMENT_DATE:
                    case FieldsList.LP_ENROLLMENT_COMPLETION_DATE:
                    case FieldsList.LP_UUID:
                    case FieldsList.LP_LAST_EDIT:
                    case FieldsList.LP_CREATION_DATE:
                    case FieldsList.LP_DESCRIPTION:
                    case FieldsList.LP_ASSOCIATED_COURSES:
                    case FieldsList.LP_MANDATORY_ASSOCIATED_COURSES:
                    case FieldsList.LP_LANGUAGE:
                    case FieldsList.LP_ENROLLMENT_START_OF_VALIDITY:
                    case FieldsList.LP_ENROLLMENT_END_OF_VALIDITY:
                    case FieldsList.LP_STAT_PROGRESS_PERCENTAGE_MANDATORY:
                    case FieldsList.LP_STAT_PROGRESS_PERCENTAGE_OPTIONAL:
                    case FieldsList.COURSE_UNIQUE_ID:
                    case FieldsList.COURSE_SKILLS:
                    case FieldsList.LO_DATE_ATTEMPT:
                    case FieldsList.LO_DATE_COMPLETE:
                    case FieldsList.LO_FIRST_ATTEMPT:
                    case FieldsList.LO_SCORE:
                    case FieldsList.LO_TITLE:
                    case FieldsList.LO_VERSION:
                    case FieldsList.SESSION_NAME:
                    case FieldsList.SESSION_CODE:
                    case FieldsList.SESSION_START_DATE:
                    case FieldsList.SESSION_END_DATE:
                    case FieldsList.SESSION_EVALUATION_SCORE_BASE:
                    case FieldsList.SESSION_TIME_SESSION:
                    case FieldsList.SESSION_UNIQUE_ID:
                    case FieldsList.SESSION_INTERNAL_ID:
                    case FieldsList.ENROLLMENT_DATE:
                    case FieldsList.ENROLLMENT_LEARNER_EVALUATION:
                    case FieldsList.ENROLLMENT_INSTRUCTOR_FEEDBACK:
                    case FieldsList.ENROLLMENT_ATTENDANCE:
                    case FieldsList.CERTIFICATION_TITLE:
                    case FieldsList.CERTIFICATION_TO_RENEW_IN:
                    case FieldsList.CERTIFICATION_ISSUED_ON:
                    case FieldsList.CERTIFICATION_COMPLETED_ACTIVITY:
                    case FieldsList.CERTIFICATION_DESCRIPTION:
                    case FieldsList.CERTIFICATION_CODE:
                    case FieldsList.BADGE_DESCRIPTION:
                    case FieldsList.BADGE_NAME:
                    case FieldsList.BADGE_SCORE:
                    case FieldsList.BADGE_ISSUED_ON:
                    case FieldsList.EXTERNAL_TRAINING_COURSE_NAME:
                    case FieldsList.EXTERNAL_TRAINING_COURSE_TYPE:
                    case FieldsList.EXTERNAL_TRAINING_SCORE:
                    case FieldsList.EXTERNAL_TRAINING_DATE:
                    case FieldsList.EXTERNAL_TRAINING_DATE_START:
                    case FieldsList.EXTERNAL_TRAINING_CREDITS:
                    case FieldsList.EXTERNAL_TRAINING_TRAINING_INSTITUTE:
                    case FieldsList.EXTERNAL_TRAINING_CERTIFICATE:
                    case FieldsList.ECOMMERCE_TRANSACTION_ADDRESS_1:
                    case FieldsList.ECOMMERCE_TRANSACTION_ADDRESS_2:
                    case FieldsList.ECOMMERCE_TRANSACTION_CITY:
                    case FieldsList.ECOMMERCE_TRANSACTION_COMPANY_NAME:
                    case FieldsList.ECOMMERCE_TRANSACTION_COUPON_CODE:
                    case FieldsList.ECOMMERCE_TRANSACTION_COUPON_DESCRIPTION:
                    case FieldsList.ECOMMERCE_TRANSACTION_DISCOUNT:
                    case FieldsList.ECOMMERCE_TRANSACTION_EXTERNAL_TRANSACTION_ID:
                    case FieldsList.ECOMMERCE_TRANSACTION_PAYMENT_DATE:
                    case FieldsList.ECOMMERCE_TRANSACTION_PAYMENT_STATUS:
                    case FieldsList.ECOMMERCE_TRANSACTION_PRICE:
                    case FieldsList.ECOMMERCE_TRANSACTION_QUANTITY:
                    case FieldsList.ECOMMERCE_TRANSACTION_STATE:
                    case FieldsList.ECOMMERCE_TRANSACTION_SUBTOTAL_PRICE:
                    case FieldsList.ECOMMERCE_TRANSACTION_TOTAL_PRICE:
                    case FieldsList.ECOMMERCE_TRANSACTION_TRANSACTION_CREATION_DATE:
                    case FieldsList.ECOMMERCE_TRANSACTION_TRANSACTION_ID:
                    case FieldsList.ECOMMERCE_TRANSACTION_VAT_NUMBER:
                    case FieldsList.ECOMMERCE_TRANSACTION_ZIP_CODE:
                    case FieldsList.ECOMMERCE_TRANSACTION_ITEM_COURSE_LP_CODE:
                    case FieldsList.ECOMMERCE_TRANSACTION_ITEM_COURSE_LP_NAME:
                    case FieldsList.ECOMMERCE_TRANSACTION_ITEM_START_DATE:
                    case FieldsList.ECOMMERCE_TRANSACTION_ITEM_END_DATE:
                    case FieldsList.ECOMMERCE_TRANSACTION_ITEM_ILT_WEBINAR_SESSION_NAME:
                    case FieldsList.ECOMMERCE_TRANSACTION_ITEM_ILT_LOCATION:
                    case FieldsList.ECOMMERCE_TRANSACTION_ITEM_TYPE:
                    case FieldsList.CONTENT_PARTNERS_REFERRAL_LINK_CODE:
                    case FieldsList.CONTENT_PARTNERS_REFERRAL_LINK_SOURCE:
                    case FieldsList.ASSET_NAME:
                    case FieldsList.CHANNELS:
                    case FieldsList.PUBLISHED_BY:
                    case FieldsList.PUBLISHED_ON:
                    case FieldsList.ANSWER_DISLIKES:
                    case FieldsList.ANSWER_LIKES:
                    case FieldsList.ANSWERS:
                    case FieldsList.ASSET_RATING:
                    case FieldsList.AVERAGE_REACTION_TIME:
                    case FieldsList.BEST_ANSWERS:
                    case FieldsList.GLOBAL_WATCH_RATE:
                    case FieldsList.INVITED_PEOPLE:
                    case FieldsList.QUESTIONS:
                    case FieldsList.TOTAL_VIEWS:
                    case FieldsList.INVOLVED_CHANNELS:
                    case FieldsList.PUBLISHED_ASSETS:
                    case FieldsList.UNPUBLISHED_ASSETS:
                    case FieldsList.PRIVATE_ASSETS:
                    case FieldsList.UPLOADED_ASSETS:
                    case FieldsList.STATS_SESSION_TIME:
                    case FieldsList.WEBINAR_SESSION_USER_SUBSCRIBE_DATE:
                    case FieldsList.WEBINAR_SESSION_USER_COMPLETE_DATE:
                    case FieldsList.ENROLLMENT_USER_SESSION_SUBSCRIBE_DATE:
                    case FieldsList.ENROLLMENT_USER_SESSION_COMPLETE_DATE:
                    case FieldsList.STATS_USER_FLOW:
                    case FieldsList.STATS_USER_FLOW_PERCENTAGE:
                    case FieldsList.STATS_USER_FLOW_MS_TEAMS:
                    case FieldsList.STATS_USER_FLOW_MS_TEAMS_PERCENTAGE:
                    case FieldsList.STATS_ACCESS_FROM_MOBILE:
                    case FieldsList.STATS_PERCENTAGE_ACCESS_FROM_MOBILE:
                    case FieldsList.STATS_USER_COURSE_FLOW_PERCENTAGE:
                    case FieldsList.STATS_USER_COURSE_TIME_SPENT_BY_FLOW:
                    case FieldsList.STATS_USER_COURSE_FLOW_MS_TEAMS_PERCENTAGE:
                    case FieldsList.STATS_USER_COURSE_TIME_SPENT_BY_FLOW_MS_TEAMS:
                    case FieldsList.STATS_PERCENTAGE_OF_COURSE_FROM_MOBILE:
                    case FieldsList.STATS_TIME_SPENT_FROM_MOBILE:
                    case FieldsList.LAST_EDIT_BY:
                    case FieldsList.ASSET_AVERAGE_REVIEW:
                    case FieldsList.ASSET_DESCRIPTION:
                    case FieldsList.ASSET_TAG:
                    case FieldsList.ASSET_SKILL:
                    case FieldsList.ASSET_LAST_ACCESS:
                    case FieldsList.ASSET_FIRST_ACCESS:
                    case FieldsList.ASSET_NUMBER_ACCESS:
                    case FieldsList.SESSION_INSTRUCTOR_USERIDS:
                    case FieldsList.SESSION_INSTRUCTOR_FULLNAMES:
                    case FieldsList.SESSION_MAXIMUM_ENROLLMENTS:
                    case FieldsList.ENROLLMENT_USER_SESSION_EVENT_ATTENDANCE_HOURS:
                    case FieldsList.SESSION_EVENT_NAME:
                    case FieldsList.SESSION_EVENT_ID:
                    case FieldsList.SESSION_EVENT_DATE:
                    case FieldsList.SESSION_EVENT_START_DATE:
                    case FieldsList.SESSION_EVENT_DURATION:
                    case FieldsList.SESSION_EVENT_TIMEZONE:
                    case FieldsList.SESSION_EVENT_INSTRUCTOR_USER_NAME:
                    case FieldsList.SESSION_EVENT_INSTRUCTOR_FULLNAME:
                    case FieldsList.SESSION_INSTRUCTOR_LIST:
                    case FieldsList.SESSION_MINIMUM_ENROLLMENTS:
                    case FieldsList.SESSION_MAXIMUM_ENROLLMENTS:
                    case FieldsList.SESSION_COMPLETION_RATE:
                    case FieldsList.SESSION_HOURS:
                    case FieldsList.EVENT_INSTRUCTORS_LIST:
                    case FieldsList.EVENT_ATTENDANCE_STATUS_NOT_SET:
                    case FieldsList.EVENT_ATTENDANCE_STATUS_ABSENT_PERC:
                    case FieldsList.EVENT_ATTENDANCE_STATUS_PRESENT_PERC:
                    case FieldsList.EVENT_AVERAGE_SCORE:
                    case FieldsList.SESSION_USER_ENROLLED:
                    case FieldsList.SESSION_USER_COMPLETED:
                    case FieldsList.SESSION_USER_WAITING:
                    case FieldsList.SESSION_USER_IN_PROGRESS:
                    case FieldsList.SESSION_EVALUATION_STATUS_NOT_SET:
                    case FieldsList.SESSION_EVALUATION_STATUS_NOT_PASSED:
                    case FieldsList.SESSION_EVALUATION_STATUS_PASSED:
                    case FieldsList.SESSION_ENROLLED_USERS:
                    case FieldsList.SESSION_SESSION_TIME:
                    case FieldsList.SESSION_TRAINING_MATERIAL_TIME:
                    case FieldsList.SURVEY_ID:
                    case FieldsList.SURVEY_TITLE:
                    case FieldsList.SURVEY_DESCRIPTION:
                    case FieldsList.SURVEY_COMPLETION_ID:
                    case FieldsList.SURVEY_COMPLETION_DATE:
                    case FieldsList.QUESTION_ID:
                    case FieldsList.QUESTION:
                    case FieldsList.ANSWER_USER:
                        toTranslate.translations[field] = fieldTranslationsKey[field];
                        break;
                    case FieldsList.SESSION_COMPLETION_MODE:
                        toTranslate.translations[FieldTranslation.SESSION_MANUAL] = fieldTranslationsKey[FieldTranslation.SESSION_MANUAL];
                        toTranslate.translations[FieldTranslation.SESSION_EVALUATION_BASED] = fieldTranslationsKey[FieldTranslation.SESSION_EVALUATION_BASED];
                        toTranslate.translations[FieldTranslation.SESSION_ATTENDANCE_BASED] = fieldTranslationsKey[FieldTranslation.SESSION_ATTENDANCE_BASED];
                        toTranslate.translations[FieldTranslation.SESSION_TRAINING_MATERIAL_BASED] = fieldTranslationsKey[FieldTranslation.SESSION_TRAINING_MATERIAL_BASED];
                        toTranslate.translations[field] = fieldTranslationsKey[field];
                        break;
                    case FieldsList.LP_ENROLLMENT_COMPLETION_PERCENTAGE:
                        if (this.session.platform.isToggleNewLearningPlanManagementAndReportEnhancement()) {
                            toTranslate.translations[field] = fieldTranslationsKey[field];
                        } else {
                            toTranslate.translations[field] = fieldTranslationsKey[FieldTranslation.LP_ENROLLMENT_COMPLETION_PERCENTAGE_OLD_TRANSLATION];
                        }
                        break;
                    default:
                        if (this.isUserExtraField(field) || this.isCourseExtraField(field)) {
                            toTranslate.translations[FieldTranslation.YES] = fieldTranslationsKey[FieldTranslation.YES];
                            toTranslate.translations[FieldTranslation.NO] = fieldTranslationsKey[FieldTranslation.NO];
                        }
                        break;
                }
            }
        }
        const translations = await hydra.getTranslations(toTranslate);
        const translationsData = translations.data;

        Object.entries(translationsData).forEach(([key, value], index) => {
            let newValue = value;
            // check if the translation value is a duplicate only if is a selected field
            // (Is possible we have two columns contains the same translation value but
            // is not possible we have two different header column with same translation value)
            if (fields.includes(key)) {
                let count = 1;
                while (this.existsDuplicate(translationsData, index, newValue)) {
                    newValue = `${value} ${count}`;
                    count = count + 1;
                }
            }
            translationsData[key] = newValue;
        });
        if (fields.includes(FieldsList.USER_DIRECT_MANAGER)) {
            const managers = await hydra.getManagerTypes([1]);
            if (managers.data.items.length > 0 && managers.data.items[0].manager_type_name) {
                translationsData[FieldsList.USER_DIRECT_MANAGER] = managers.data.items[0].manager_type_name;
            }
        }
        return translationsData;
    }

    /**
     * Check if in the object translation there is another previous field with same translation.
     * @param translation
     * @param indexValue
     * @param valueToCheck
     * @private
     */
    protected existsDuplicate(translation: object, indexValue, valueToCheck: string): boolean {
        for (const [index, [, value]] of Object.entries(Object.entries(translation))) {
            if (index >= indexValue) {
                return false;
            }

            if (value.toLowerCase() === valueToCheck.toLowerCase() && index !== indexValue) {
                return true;
            }
        }
        return false;
    }

    public abstract parseLegacyReport(legacyReport: LegacyReport, platform: string, visibilityRules: VisibilityRule[]): ReportManagerInfo;

    protected abstract getReportDefaultStructure(report: ReportManagerInfo, title: string, platform: string, idUser: number, description: string, queryBuilder?: CustomReportType): ReportManagerInfo;

    public async cloneReport(session: SessionManager, title: string, description: string, queryBuilderId?: string): Promise<string> {
        const clone = new ReportClone(session.user);
        const reportId = new ReportId(this.info.idReport, this.info.platform);
        const clonedReport = await clone.execute(reportId, title, description, queryBuilderId);

        return clonedReport.Id.ReportId;
    }

    /**
     * Set the common fields between the report types (name, dates, author and so on)
     * @param report The new report
     * @param legacyReport The legacy report to import
     * @param visibilityRules The legacy visibility rules referred to legacy reports
     */
    protected setCommonFieldsBetweenReportTypes(report: ReportManagerInfo, legacyReport: LegacyReport, visibilityRules: VisibilityRule[]): ReportManagerInfo {
        report.creationDate = legacyReport.creation_date;
        report.author = +legacyReport.author;
        report.lastEditBy.idUser = legacyReport.last_edit_by ? +legacyReport.last_edit_by : +legacyReport.author;
        report.lastEdit = legacyReport.last_edit;
        report.importedFromLegacyId = legacyReport.id_filter;

        const MY_NAMESPACE = '1b671a64-40d5-491e-99b0-da01ff1f3341';
        report.idReport = v5(`${legacyReport.id_filter}${report.platform}`, MY_NAMESPACE);

        // manage the visibility section
        switch (legacyReport.visibility_type) {
            case 'private':
                report.visibility.type = VisibilityTypes.ALL_GODADMINS;
                break;
            case 'public':
                report.visibility.type = VisibilityTypes.ALL_GODADMINS_AND_PU;
                break;
            case 'selection':
                report.visibility.type = VisibilityTypes.ALL_GODADMINS_AND_SELECTED_PU;
                try {
                    report.visibility = this.setCustomVisibilityFields(legacyReport.id_filter, report.visibility, visibilityRules);
                } catch (e: any) {
                    this.logger.debug(`Can not set a custom selection for visibility - fallback to Godadmin - limit exceeded for report ${report.importedFromLegacyId}`);
                    report.visibility.type = VisibilityTypes.ALL_GODADMINS;
                }

                break;
            default:
                report.visibility.type = VisibilityTypes.ALL_GODADMINS;
                break;
        }

        return report;
    }

    /**
     * Set the custom visibility selection for the new report based on the legacy report configuration
     * @param legacyReportId The id of the legacy report
     * @param visibility The visibility descriptor for aamon
     * @param visibilityRules The legacy visibility rules
     */
    private setCustomVisibilityFields(legacyReportId: string, visibility: ReportManagerInfoVisibility, visibilityRules: VisibilityRule[]): ReportManagerInfoVisibility {
        for (const visibilityRule of visibilityRules) {
            if (visibilityRule.id_report !== legacyReportId) continue;
            switch (visibilityRule.member_type) {
                case 'user':
                    visibility.users.push({id: +visibilityRule.member_id});
                    break;
                case 'group':
                    visibility.groups.push({id: +visibilityRule.member_id});
                    break;
                case 'branch':
                    visibility.branches.push({
                        id: +visibilityRule.member_id,
                        descendants: visibilityRule.select_state === '2'
                    });

                    break;
            }

        }

        if (visibility.users.length > 100 || visibility.groups.length > 50 || visibility.branches.length > 50) {
            visibility.users = visibility.groups = visibility.branches = [];

            throw new Error('Limit exceeded for visibility custom selections');
        }

        return visibility;
    }

    /**
     * Import the users filter
     * @param filterData The legacy filter json
     * @param report The aamon report
     * @param reportId The legacy report id
     */
    protected legacyUserImport(filterData: any, report: ReportManagerInfo, reportId: string): void {
        if ((!filterData.users || filterData.users.length === 0) &&
            (!filterData.groups || filterData.groups.length === 0) &&
            (!filterData.orgchartnodes || filterData.orgchartnodes.length === 0)) {
            this.logger.error(`Invalid users selection - empty selection for id: ${reportId}`);
            throw new Error('Invalid users selection');
        }
        if (report.users) {
            report.users.all = false;
            report.users.users = !filterData.users ? [] : filterData.users.map((userId: string) => {
                return {id: +userId};
            });
            report.users.groups = !filterData.groups ? [] : filterData.groups.map((groupId: string) => {
                return {id: +groupId};
            });
            report.users.branches = !filterData.orgchartnodes ? [] : filterData.orgchartnodes.map((branch: any) => {
                return {
                    id: branch.key,
                    descendants: branch.selectState === 2,
                };
            });

            // detect all selector status
            const entitiesLimits = this.session.platform.getEntitiesLimits();
            if (report.users.users.length > (entitiesLimits.users.usersLimit as number) ||
                report.users.groups.length > (entitiesLimits.users.groupsLimit as number) ||
                report.users.branches.length > (entitiesLimits.users.branchesLimit as number)) {
                report.users.all = true;
                report.users.users = report.users.branches = report.users.groups = [];
                this.logger.debug(`User selector fallback to all - limits exceeded for the report ${reportId}`);
            }

            // in both cases parse the opposite value from legacy (in certifications users different name)
            report.users.hideDeactivated = (filterData.filters?.consider_suspended_users || filterData.filters?.show_suspended_users) === '1' ? false : true;
            report.users.showOnlyLearners = filterData.filters?.consider_other_than_students === '1' ? false : true;

            // check if there is a custom field value, else return empty string
            const legacyCustomFields = filterData.filters?.custom_fields ? filterData.filters?.custom_fields : '';
            const isLegacyUserAddFields = Object.keys(legacyCustomFields).reduce((acc, curr) => acc + legacyCustomFields[curr], '');
            report.users.isUserAddFields = isLegacyUserAddFields.length > 0;
        }

        // parse the user additional fields (dropdown)
        if (filterData.filters?.custom_fields) {
            report.userAdditionalFieldsFilter = {};
            for (const key in filterData.filters.custom_fields) {
                // check if the user additional field is populated
                if (filterData.filters.custom_fields[key]) {
                    try {
                        report.userAdditionalFieldsFilter[parseInt(key, 10), 10] = filterData.filters.custom_fields[key];
                    } catch (e: any) {
                        this.logger.errorWithStack(`Can not parse the user additional field ${key} fo the report ${reportId}`, e);
                    }
                }
            }
        }

    }

    /**
     * Import the courses filter
     * @param filterData The legacy filter json
     * @param report The aamon report
     * @param reportId The legacy report id
     */
    protected legacyCourseImport(filterData: any, report: ReportManagerInfo, reportId: string): void {
        if (!filterData.courses) {
            this.logger.error(`Invalid courses selection - no course field for id: ${reportId}`);
            throw new Error('Invalid courses selection');
        }
        if (report.courses) {
            report.courses.all = filterData.courses.length === 0 || filterData.courses.length > 50;
            report.courses.courses = report.courses.all ? [] : filterData.courses.map((course: string) => {
                return {
                    id: +course
                };
            });
        }
    }

    /**
     * Import the certifications filter
     * @param filterData The legacy filter json
     * @param report The aamon report
     * @param reportId The legacy report id
     */
    protected legacyCertificationsImport(filterData: any, report: ReportManagerInfo, reportId: string): void {
        if (!filterData.certifications) {
            this.logger.error(`Invalid certifications selection - no certifications field for id: ${reportId}`);
            throw new Error('Invalid certifications selection');
        }
        if (report.certifications) {
            report.certifications.all = filterData.certifications.length === 0 || filterData.certifications.length > 100;
            report.certifications.certifications = report.certifications.all ? [] : filterData.certifications.map((certification: number) => {
                return {
                    id: certification
                };
            });
        }
    }

    /**
     * Import the learning plans filter
     * @param filterData The legacy filter json
     * @param report The aamon report
     * @param reportId The legacy report id
     */
    protected legacyLearningPlansImport(filterData: any, report: ReportManagerInfo, reportId: string): void {
        if (!filterData.plans) {
            this.logger.error(`Invalid learning plans selection - no plans field for id: ${reportId}`);
            throw new Error('Invalid learning plans selection');
        }
        if (report.learningPlans) {
            report.learningPlans.all = filterData.plans.length === 0 || filterData.plans.length > 100;
            report.learningPlans.learningPlans = report.learningPlans.all ? [] : filterData.plans.map((course: string) => {
                return {
                    id: +course
                };
            });
        }
    }

    /**
     * Import the badges filter
     * @param filterData The badge filter json
     * @param report The aamon report
     * @param reportId The legacy report id
     */
    protected legacyBadgesImport(filterData: any, report: ReportManagerInfo, reportId: string): void {
        if (!filterData.badges) {
            this.logger.error(`Invalid badges selection field for id: ${reportId}`);
            throw new Error('Invalid badges selection');
        }
        if (report.badges) {
            report.badges.all = filterData.badges.length === 0 || filterData.badges.length > 100;
            report.badges.badges = report.badges.all ? [] : filterData.badges.map((badge: string) => {
                return {
                    id: +badge
                };
            });
        }
    }

    /**
     * Import the assets filter
     * @param filterData The assets filter json
     * @param report The aamon report
     * @param reportId The legacy report id
     */
    protected legacyAssetsImport(filterData: any, report: ReportManagerInfo, reportId: string): void {

        if (!filterData.assets) {
            this.logger.error(`Invalid asset selection field for id: ${reportId}`);
            throw new Error('Invalid asset selection');
        }
        if (report.assets) {
            report.assets.all = filterData.assets.length === 0;
            report.assets.assets = filterData.assets.map((asset: string) => {
                return {
                    id: +asset
                };
            });

        }
    }

    /**
     * Import the channels filter
     * @param filterData The channels filter json
     * @param report The aamon report
     * @param reportId The legacy report id
     */
    protected legacyChannelsImport(filterData: any, report: ReportManagerInfo, reportId: string): void {

        if (!filterData.channels) {
            this.logger.error(`Invalid channel selection field for id: ${reportId}`);
            throw new Error('Invalid channel selection');
        }
        if (report.assets) {
            report.assets.all = filterData.channels.length === 0;
            report.assets.channels = filterData.channels.map((channel: string) => {
                return {
                    id: +channel
                };
            });

        }
    }

    /**
     * Return the field of the entity passed in input if the order by key is of type entity, otherwise undefined
     * @param legacyOrderByField legacy field describing the order by
     * @param entity the entity eg. user, course, stat etc...
     */
    private extractLegacyOrderingFieldByEntity(legacyOrderByField: any, entity: string): string | undefined {
        let orderKeyToMatch = undefined;
        if (legacyOrderByField && legacyOrderByField.orderBy) {
            const orderBySplitted = legacyOrderByField.orderBy.split('.');
            if (orderBySplitted.length === 2 && orderBySplitted[0] === entity) {
                orderKeyToMatch = orderBySplitted[1];
            } else if (orderBySplitted.length === 3 && orderBySplitted[0] === entity) {
                orderKeyToMatch = `${orderBySplitted[1]}.${orderBySplitted[2]}`;
            }
        }

        return orderKeyToMatch;
    }

    protected mapUserSelectedFields(legacyFields: any, legacyOrderByField: any, mandatoryFields: { [key: string]: string } = {}, type?: string): FieldsDescriptor {
        if (!legacyFields) {
            return {
                fields: []
            };
        }
        const mappedFields: string[] = [];
        let legacyOrderField: LegacyOrderByParsed | undefined;

        let userFieldsLegacyMapper: { [key: string]: string } = {};

        userFieldsLegacyMapper = {
            userid: FieldsList.USER_USERID, // -> it's a default value
            idst: FieldsList.USER_ID,
            firstname: FieldsList.USER_FIRSTNAME,
            lastname: FieldsList.USER_LASTNAME,
            fullname: FieldsList.USER_FULLNAME,
            email: FieldsList.USER_EMAIL,
            register_date: FieldsList.USER_REGISTER_DATE,
            lastenter: FieldsList.USER_LAST_ACCESS_DATE,
            valid: FieldsList.USER_DEACTIVATED,
            suspend_date: FieldsList.USER_SUSPEND_DATE,
            expiration: FieldsList.USER_EXPIRATION,
            email_status: FieldsList.USER_EMAIL_VALIDATION_STATUS,
            groups_list: FieldsList.USER_BRANCH_PATH
        };

        if (type === ReportsTypes.USER_CONTRIBUTIONS) {
            userFieldsLegacyMapper = {
                suspend_date: FieldsList.USER_SUSPEND_DATE,
                firstname: FieldsList.USER_FIRSTNAME,
                lastname: FieldsList.USER_LASTNAME,
                fullname: FieldsList.USER_FULLNAME,
                email: FieldsList.USER_EMAIL,
                unique_id: FieldsList.USER_ID,
                branches: FieldsList.USER_BRANCH_PATH,
                creation_date: FieldsList.USER_REGISTER_DATE,
                email_validation_status: FieldsList.USER_EMAIL_VALIDATION_STATUS,
                expiry: FieldsList.USER_EXPIRATION,
                last_enter_date: FieldsList.USER_LAST_ACCESS_DATE,
                active_date: FieldsList.USER_DEACTIVATED,
                role: FieldsList.USER_LEVEL,
            };
        }

        const orderEntitySwitcher = (type !== ReportsTypes.USER_CONTRIBUTIONS) ? 'user' : 'us';

        // detect if the ordering field is a field of the user
        const orderField = this.extractLegacyOrderingFieldByEntity(legacyOrderByField, orderEntitySwitcher);

        for (const field in legacyFields) {
            let aamonField = '';
            if (userFieldsLegacyMapper[field]) {
                aamonField = userFieldsLegacyMapper[field];
                if (!mandatoryFields[aamonField]) mappedFields.push(aamonField);
            } else if (/^\d+$/.test(field)) {
                // this is an additional field, in aamon is with this syntax
                aamonField = `user_extrafield_${field}`;
                mappedFields.push(aamonField);
            }

            // set the ordering
            if (orderField === field && aamonField) {
                legacyOrderField = {
                    field: aamonField,
                    direction: legacyOrderByField.type === 'ASC' ? 'asc' : 'desc',
                };
            }
        }

        return {
            fields: mappedFields,
            orderByDescriptor: legacyOrderField ? legacyOrderField : undefined,
        };
    }

    protected mapCourseSelectedFields(legacyFields: any, legacyOrderByField: any, mandatoryFields: { [key: string]: string } = {}): FieldsDescriptor {
        if (!legacyFields) {
            return {
                fields: []
            };
        }
        const mappedFields: string[] = [];
        let legacyOrderField: LegacyOrderByParsed | undefined;

        const courseFieldsLegacyMapper: { [key: string]: string } = {
            'name': FieldsList.COURSE_NAME, // -> default
            'course_internal_id': FieldsList.COURSE_ID,
            'uidCourse': FieldsList.COURSE_UNIQUE_ID, // UNIQUE ID COURSE (uidCourse in db) phrase: "Course Unique ID" module: "course"
            'coursesCategory.translation': FieldsList.COURSE_CATEGORY_NAME,
            'categoryCode': FieldsList.COURSE_CATEGORY_CODE,
            'code': FieldsList.COURSE_CODE,
            'status': FieldsList.COURSE_STATUS,
            'credits': FieldsList.COURSE_CREDITS,
            'date_begin': FieldsList.COURSE_DATE_BEGIN,
            'date_end': FieldsList.COURSE_DATE_END,
            'course_type': FieldsList.COURSE_TYPE,
            'duration': FieldsList.COURSE_DURATION,
            'expired': FieldsList.COURSE_EXPIRED,
            // 'group_description': -> missing in aamon (exist in groups-courses legacy report)
        };

        // detect if the ordering field is a field of the user
        const orderField = this.extractLegacyOrderingFieldByEntity(legacyOrderByField, 'course');

        for (const field in legacyFields) {
            let aamonField = '';
            if (courseFieldsLegacyMapper[field]) {
                aamonField = courseFieldsLegacyMapper[field];
                if (!mandatoryFields[aamonField]) mappedFields.push(aamonField);
            } else if (/^\d+$/.test(field)) {
                // this is an additional field, in aamon is with this syntax
                const aamonField = `course_extrafield_${field}`;
                mappedFields.push(aamonField);
            }
            // set the ordering
            if (orderField === field && aamonField) {
                legacyOrderField = {
                    field: aamonField,
                    direction: legacyOrderByField.type === 'ASC' ? 'asc' : 'desc',
                };
            }
        }

        return {
            fields: mappedFields,
            orderByDescriptor: legacyOrderField ? legacyOrderField : undefined,
        };
    }

    protected mapEnrollmentSelectedFields(legacyFields: any, legacyOrderByField: any, mandatoryFields: { [key: string]: string } = {}): FieldsDescriptor {
        if (!legacyFields) {
            return {
                fields: []
            };
        }
        const mappedFields: string[] = [];
        let legacyOrderField: LegacyOrderByParsed | undefined;

        const courseUserFieldsLegacyMapper: { [key: string]: string } = {
            level: FieldsList.COURSEUSER_LEVEL,
            date_inscr: FieldsList.COURSEUSER_DATE_INSCR,
            date_first_access: FieldsList.COURSEUSER_DATE_FIRST_ACCESS,
            date_last_access: FieldsList.COURSEUSER_DATE_LAST_ACCESS,
            date_complete: FieldsList.COURSEUSER_DATE_COMPLETE,
            status: FieldsList.COURSEUSER_STATUS,
            date_begin_validity: FieldsList.COURSEUSER_DATE_BEGIN_VALIDITY,
            date_expire_validity: FieldsList.COURSEUSER_DATE_EXPIRE_VALIDITY,
            score_given: FieldsList.COURSEUSER_SCORE_GIVEN,
            initial_score_given: FieldsList.COURSEUSER_INITIAL_SCORE_GIVEN,
            // subscription_code: FieldsList.COURSEUSER_INITIAL_SCORE_GIVEN, -> missing in aamon
            // subscription_code_set: FieldsList.COURSEUSER_INITIAL_SCORE_GIVEN, -> missing in aamon
            course_completion_percentage: FieldsList.STATS_USER_COURSE_COMPLETION_PERCENTAGE,
            number_of_sessions: FieldsList.STATS_TOTAL_SESSIONS_IN_COURSE,
            // total_time_in_s4bdp_sessions: FieldsList.STATS_TOTAL_TIME_IN_COURSE,
            total_time_in_course: FieldsList.STATS_TOTAL_TIME_IN_COURSE, // same key
            issued_on: FieldsList.CERTIFICATION_ISSUED_ON,
            to_renew_in: FieldsList.CERTIFICATION_TO_RENEW_IN,
            completed_activity: FieldsList.CERTIFICATION_COMPLETED_ACTIVITY,
        };

        // detect if the ordering field is a field of the user
        const orderField = this.extractLegacyOrderingFieldByEntity(legacyOrderByField, 'enrollment');

        for (const field in legacyFields) {
            let aamonField = '';
            if (courseUserFieldsLegacyMapper[field]) {
                aamonField = courseUserFieldsLegacyMapper[field];
                if (!mandatoryFields[aamonField]) mappedFields.push(aamonField);
            } else if (/^\d+$/.test(field)) {
                // this is an additional field, in aamon is with this syntax
                const aamonField = `courseuser_extrafield_${field}`;
                mappedFields.push(aamonField);
            }

            // set the ordering
            if (orderField === field && aamonField) {
                legacyOrderField = {
                    field: aamonField,
                    direction: legacyOrderByField.type === 'ASC' ? 'asc' : 'desc',
                };
            }
        }

        return {
            fields: mappedFields,
            orderByDescriptor: legacyOrderField ? legacyOrderField : undefined,
        };
    }

    protected mapLearningPlanSelectedFields(legacyFields: any, legacyOrderByField: any, mandatoryFields: { [key: string]: string } = {}): FieldsDescriptor {
        if (!legacyFields) {
            return {
                fields: []
            };
        }
        const mappedFields: string[] = [];
        let legacyOrderField: LegacyOrderByParsed | undefined;

        const lpFieldsLegacyMapper: { [key: string]: string } = {
            plan_code: FieldsList.LP_CODE,
            plan_name: FieldsList.LP_NAME,
            plan_credits: FieldsList.LP_CREDITS,
        };

        // detect if the ordering field is a field of the user
        const orderField = this.extractLegacyOrderingFieldByEntity(legacyOrderByField, 'coursepaths');

        for (const field in legacyFields) {
            let aamonField = '';
            if (lpFieldsLegacyMapper[field]) {
                aamonField = lpFieldsLegacyMapper[field];
                if (!mandatoryFields[aamonField]) mappedFields.push(aamonField);
            }

            // set the ordering
            if (orderField === field && aamonField) {
                legacyOrderField = {
                    field: aamonField,
                    direction: legacyOrderByField.type === 'ASC' ? 'asc' : 'desc',
                };
            }
        }

        return {
            fields: mappedFields,
            orderByDescriptor: legacyOrderField ? legacyOrderField : undefined,
        };
    }

    protected mapCertificationSelectedFields(legacyFields: any, legacyOrderByField: any, mandatoryFields: { [key: string]: string } = {}): FieldsDescriptor {
        if (!legacyFields) {
            return {
                fields: []
            };
        }
        const mappedFields: string[] = [];
        let legacyOrderField: LegacyOrderByParsed | undefined;

        const lpFieldsLegacyMapper: { [key: string]: string } = {
            title: FieldsList.CERTIFICATION_TITLE,
            code: FieldsList.CERTIFICATION_CODE,
            description: FieldsList.CERTIFICATION_DESCRIPTION,
            expiration: FieldsList.CERTIFICATION_DURATION,
        };

        // detect if the ordering field is a field of the user
        const orderField = this.extractLegacyOrderingFieldByEntity(legacyOrderByField, 'certification');

        for (const field in legacyFields) {
            let aamonField = '';
            if (lpFieldsLegacyMapper[field]) {
                aamonField = lpFieldsLegacyMapper[field];
                if (!mandatoryFields[aamonField]) mappedFields.push(aamonField);
            }

            // set the ordering
            if (orderField === field && aamonField) {
                legacyOrderField = {
                    field: aamonField,
                    direction: legacyOrderByField.type === 'ASC' ? 'asc' : 'desc',
                };
            }
        }

        return {
            fields: mappedFields,
            orderByDescriptor: legacyOrderField ? legacyOrderField : undefined,
        };
    }

    protected mapLearningPlanEnrollmentSelectedFields(legacyFields: any, legacyOrderByField: any, mandatoryFields: { [key: string]: string } = {}): FieldsDescriptor {
        if (!legacyFields) {
            return {
                fields: []
            };
        }
        const mappedFields: string[] = [];
        let legacyOrderField: LegacyOrderByParsed | undefined;

        const lpFieldsLegacyMapper: { [key: string]: string } = {
            plan_subDate: FieldsList.LP_ENROLLMENT_DATE,
            plan_compDate: FieldsList.LP_ENROLLMENT_COMPLETION_DATE,
            plan_compStatus: FieldsList.LP_ENROLLMENT_STATUS,
            plan_compPercent: FieldsList.LP_ENROLLMENT_COMPLETION_PERCENTAGE
        };

        // detect if the ordering field is a field of the user
        const orderField = this.extractLegacyOrderingFieldByEntity(legacyOrderByField, 'plansUsers');

        for (const field in legacyFields) {
            let aamonField = '';
            if (lpFieldsLegacyMapper[field]) {
                aamonField = lpFieldsLegacyMapper[field];
                if (!mandatoryFields[aamonField]) mappedFields.push(aamonField);
            }

            // set the ordering
            if (orderField === field && aamonField) {
                legacyOrderField = {
                    field: aamonField,
                    direction: legacyOrderByField.type === 'ASC' ? 'asc' : 'desc',
                };
            }
        }

        return {
            fields: mappedFields,
            orderByDescriptor: legacyOrderField ? legacyOrderField : undefined,
        };
    }

    protected mapStatsSelectedFields(legacyFields: any, legacyOrderByField: any, mandatoryFields: { [key: string]: string } = {}): FieldsDescriptor {
        if (!legacyFields) {
            return {
                fields: []
            };
        }
        const mappedFields: string[] = [];
        let legacyOrderField: LegacyOrderByParsed | undefined;

        // detect if the ordering field is a field of the user
        const orderField = this.extractLegacyOrderingFieldByEntity(legacyOrderByField, 'stats');
        const showPercentage = legacyFields.show_percents;

        const statsFieldsLegacyMapper: { [key: string]: string } = {
            total_subscribed_users: FieldsList.STATS_ENROLLED_USERS,
            number_of_users_who_has_not_started_yet: showPercentage ? FieldsList.STATS_NOT_STARTED_USERS_PERCENTAGE : FieldsList.STATS_NOT_STARTED_USERS,
            number_of_users_in_progress: showPercentage ? FieldsList.STATS_IN_PROGRESS_USERS_PERCENTAGE : FieldsList.STATS_IN_PROGRESS_USERS,
            number_of_users_completed: showPercentage ? FieldsList.STATS_COMPLETED_USERS_PERCENTAGE : FieldsList.STATS_COMPLETED_USERS,
            total_time_spent_by_users_in_the_course: FieldsList.STATS_TOTAL_TIME_IN_COURSE,
            rate_average: FieldsList.STATS_COURSE_RATING,
            active: FieldsList.STATS_ACTIVE,
            expired: FieldsList.STATS_EXPIRED,
            issued: FieldsList.STATS_ISSUED,
        };

        for (const field in legacyFields) {
            let aamonField = '';
            if (statsFieldsLegacyMapper[field]) {
                aamonField = statsFieldsLegacyMapper[field];
                if (!mandatoryFields[aamonField]) mappedFields.push(aamonField);
            }

            // set the ordering
            if (orderField === field && aamonField) {
                legacyOrderField = {
                    field: aamonField,
                    direction: legacyOrderByField.type === 'ASC' ? 'asc' : 'desc',
                };
            }
        }

        return {
            fields: mappedFields,
            orderByDescriptor: legacyOrderField ? legacyOrderField : undefined,
        };
    }

    protected mapGroupSelectedFields(legacyFields: any, legacyOrderByField: any, mandatoryFields: { [key: string]: string } = {}): FieldsDescriptor {
        if (!legacyFields) {
            return {
                fields: []
            };
        }
        const mappedFields: string[] = [];
        let legacyOrderField: LegacyOrderByParsed | undefined;

        // detect if the ordering field is a field of the user
        const orderField = this.extractLegacyOrderingFieldByEntity(legacyOrderByField, 'group');

        // no idea of the reason but there's a course_name in the group object
        if (legacyFields.course_name || legacyFields.group_name) {
            if (!mandatoryFields[FieldsList.GROUP_GROUP_OR_BRANCH_NAME]) mappedFields.push(FieldsList.GROUP_GROUP_OR_BRANCH_NAME);
            if (orderField === 'course_name' || orderField === 'group_name') {
                legacyOrderField = {
                    field: FieldsList.GROUP_GROUP_OR_BRANCH_NAME,
                    direction: legacyOrderByField.type === 'ASC' ? 'asc' : 'desc',
                };
            }
        }
        if (legacyFields.total_users_in_group) {
            if (!mandatoryFields[FieldsList.GROUP_MEMBERS_COUNT]) mappedFields.push(FieldsList.GROUP_MEMBERS_COUNT);
            if (orderField === 'total_users_in_group') {
                legacyOrderField = {
                    field: FieldsList.GROUP_MEMBERS_COUNT,
                    direction: legacyOrderByField.type === 'ASC' ? 'asc' : 'desc',
                };
            }
        }

        return {
            fields: mappedFields,
            orderByDescriptor: legacyOrderField ? legacyOrderField : undefined,
        };
    }

    protected mapILTSessionSelectedFields(legacyFields: any, legacyOrderByField: any, mandatoryFields: { [key: string]: string } = {}): FieldsDescriptor {
        if (!legacyFields) {
            return {
                fields: []
            };
        }
        const mappedFields: string[] = [];
        let legacyOrderField: LegacyOrderByParsed | undefined;

        const iltSessionFieldsLegacyMapper: { [key: string]: string } = {
            uid_session: FieldsList.SESSION_UNIQUE_ID,
            name: FieldsList.SESSION_NAME,
            score_base: FieldsList.SESSION_EVALUATION_SCORE_BASE,
            date_begin: FieldsList.SESSION_START_DATE,
            date_end: FieldsList.SESSION_END_DATE,
            total_hours: FieldsList.SESSION_TIME_SESSION,
        };

        // detect if the ordering field is a field of the user
        const orderField = this.extractLegacyOrderingFieldByEntity(legacyOrderByField, 'session');

        for (const field in legacyFields) {
            let aamonField = '';
            if (iltSessionFieldsLegacyMapper[field]) {
                aamonField = iltSessionFieldsLegacyMapper[field];
                if (!mandatoryFields[aamonField]) mappedFields.push(aamonField);
            } else if (/^\d+$/.test(field)) {
                // this is a classroom additional field, in aamon is with this syntax
                const aamonField = `classroom_extrafield_${field}`;
                mappedFields.push(aamonField);
            }

            // set the ordering
            if (orderField === field && aamonField) {
                legacyOrderField = {
                    field: aamonField,
                    direction: legacyOrderByField.type === 'ASC' ? 'asc' : 'desc',
                };
            }
        }

        return {
            fields: mappedFields,
            orderByDescriptor: legacyOrderField ? legacyOrderField : undefined,
        };
    }

    protected mapBadgesSelectedFields(legacyFields: any, legacyOrderByField: any, mandatoryFields: { [key: string]: string } = {}): FieldsDescriptor {
        if (!legacyFields) {
            return {
                fields: []
            };
        }
        const mappedFields: string[] = [];
        let legacyOrderField: LegacyOrderByParsed | undefined;

        const badgesFieldsLegacyMapper: { [key: string]: string } = {
            name: FieldsList.BADGE_NAME,
            description: FieldsList.BADGE_DESCRIPTION,
            score: FieldsList.BADGE_SCORE
        };

        // detect if the ordering field is a field of the badges
        const orderField = this.extractLegacyOrderingFieldByEntity(legacyOrderByField, 'badges');

        for (const field in legacyFields) {
            let aamonField = '';
            if (badgesFieldsLegacyMapper[field]) {
                aamonField = badgesFieldsLegacyMapper[field];
                if (!mandatoryFields[aamonField]) mappedFields.push(aamonField);
            }

            // set the ordering
            if (orderField === field && aamonField) {
                legacyOrderField = {
                    field: aamonField,
                    direction: legacyOrderByField.type === 'ASC' ? 'asc' : 'desc',
                };
            }
        }

        return {
            fields: mappedFields,
            orderByDescriptor: legacyOrderField ? legacyOrderField : undefined,
        };
    }

    protected mapBadgeAssignmentSelectedFields(legacyFields: any, legacyOrderByField: any, mandatoryFields: { [key: string]: string } = {}): FieldsDescriptor {
        if (!legacyFields) {
            return {
                fields: []
            };
        }
        const mappedFields: string[] = [];
        let legacyOrderField: LegacyOrderByParsed | undefined;

        const badgeAssignmentFieldsLegacyMapper: { [key: string]: string } = {
            issued_on: FieldsList.BADGE_ISSUED_ON,
        };

        // detect if the ordering field is a field of the badges assignment
        const orderField = this.extractLegacyOrderingFieldByEntity(legacyOrderByField, 'assignment');

        for (const field in legacyFields) {
            let aamonField = '';
            if (badgeAssignmentFieldsLegacyMapper[field]) {
                aamonField = badgeAssignmentFieldsLegacyMapper[field];
                if (!mandatoryFields[aamonField]) mappedFields.push(aamonField);
            }

            // set the ordering
            if (orderField === field && aamonField) {
                legacyOrderField = {
                    field: aamonField,
                    direction: legacyOrderByField.type === 'ASC' ? 'asc' : 'desc',
                };
            }
        }

        return {
            fields: mappedFields,
            orderByDescriptor: legacyOrderField ? legacyOrderField : undefined,
        };
    }

    protected mapExternalTrainingSelectedFields(legacyFields: any): string[] {
        const fields: string[] = [];
        if (!legacyFields) {
            return fields;
        }

        const externalTrainingFieldsLegacyMapper: { [key: string]: string } = {
            course_name: FieldsList.EXTERNAL_TRAINING_COURSE_NAME,
            course_type: FieldsList.EXTERNAL_TRAINING_COURSE_TYPE,
            score: FieldsList.EXTERNAL_TRAINING_SCORE,
            to_date: FieldsList.EXTERNAL_TRAINING_DATE,
            credits: FieldsList.EXTERNAL_TRAINING_CREDITS,
            training_institute: FieldsList.EXTERNAL_TRAINING_TRAINING_INSTITUTE,
            certificate: FieldsList.EXTERNAL_TRAINING_CERTIFICATE,
            status: FieldsList.EXTERNAL_TRAINING_STATUS
        };

        for (const field in legacyFields) {
            let aamonField = '';
            if (externalTrainingFieldsLegacyMapper[field]) {
                aamonField = externalTrainingFieldsLegacyMapper[field];
                fields.push(aamonField);
            } else if (/^\d+$/.test(field)) {
                fields.push(`external_activity_extrafield_${field}`);
            }
        }

        return fields;
    }

    protected mapEcommerceTransactionSelectedFields(legacyFields: any, legacyOrderByField: any, mandatoryFields: { [key: string]: string } = {}): FieldsDescriptor {
        if (!legacyFields) {
            return {
                fields: []
            };
        }
        const mappedFields: string[] = [];
        let legacyOrderField: LegacyOrderByParsed | undefined;

        const ecommerceTransactionFieldsLegacyMapper: { [key: string]: string } = {
            bill_address1: FieldsList.ECOMMERCE_TRANSACTION_ADDRESS_1,
            bill_address2: FieldsList.ECOMMERCE_TRANSACTION_ADDRESS_2,
            bill_city: FieldsList.ECOMMERCE_TRANSACTION_CITY,
            bill_company_name: FieldsList.ECOMMERCE_TRANSACTION_COMPANY_NAME,
            coupon_code: FieldsList.ECOMMERCE_TRANSACTION_COUPON_CODE,
            coupon_description: FieldsList.ECOMMERCE_TRANSACTION_COUPON_DESCRIPTION,
            discount: FieldsList.ECOMMERCE_TRANSACTION_DISCOUNT,
            payment_txn_id: FieldsList.ECOMMERCE_TRANSACTION_EXTERNAL_TRANSACTION_ID,
            date_activated: FieldsList.ECOMMERCE_TRANSACTION_PAYMENT_DATE,
            payment_type: FieldsList.ECOMMERCE_TRANSACTION_PAYMENT_METHOD,
            paid: FieldsList.ECOMMERCE_TRANSACTION_PAYMENT_STATUS,
            single_price: FieldsList.ECOMMERCE_TRANSACTION_PRICE,
            quantity: FieldsList.ECOMMERCE_TRANSACTION_QUANTITY,
            bill_state: FieldsList.ECOMMERCE_TRANSACTION_STATE,
            total_price: FieldsList.ECOMMERCE_TRANSACTION_TOTAL_PRICE,
            date_creation: FieldsList.ECOMMERCE_TRANSACTION_TRANSACTION_CREATION_DATE,
            id_trans: FieldsList.ECOMMERCE_TRANSACTION_TRANSACTION_ID,
            bill_vat_number: FieldsList.ECOMMERCE_TRANSACTION_VAT_NUMBER,
            bill_zip: FieldsList.ECOMMERCE_TRANSACTION_ZIP_CODE
        };


        // detect if the ordering field is a field of the badges
        const orderField = this.extractLegacyOrderingFieldByEntity(legacyOrderByField, 'ecommerce');

        for (const field in legacyFields) {
            let aamonField = '';
            if (ecommerceTransactionFieldsLegacyMapper[field]) {
                aamonField = ecommerceTransactionFieldsLegacyMapper[field];
                if (!mandatoryFields[aamonField]) mappedFields.push(aamonField);
            }

            // set the ordering
            if (orderField === field && aamonField) {
                legacyOrderField = {
                    field: aamonField,
                    direction: legacyOrderByField.type === 'ASC' ? 'asc' : 'desc',
                };
            }
        }

        return {
            fields: mappedFields,
            orderByDescriptor: legacyOrderField ? legacyOrderField : undefined,
        };
    }

    protected mapEcommerceTransactionItemSelectedFields(legacyFields: any, legacyOrderByField: any, mandatoryFields: { [key: string]: string } = {}): FieldsDescriptor {
        if (!legacyFields) {
            return {
                fields: []
            };
        }
        const mappedFields: string[] = [];
        let legacyOrderField: LegacyOrderByParsed | undefined;

        const ecommerceTransactionItemFieldsLegacyMapper: { [key: string]: string } = {
            item_code: FieldsList.ECOMMERCE_TRANSACTION_ITEM_COURSE_LP_CODE,
            item_name: FieldsList.ECOMMERCE_TRANSACTION_ITEM_COURSE_LP_NAME,
            session_start_date: FieldsList.ECOMMERCE_TRANSACTION_ITEM_START_DATE,
            session_end_date: FieldsList.ECOMMERCE_TRANSACTION_ITEM_END_DATE,
            session_name: FieldsList.ECOMMERCE_TRANSACTION_ITEM_ILT_WEBINAR_SESSION_NAME,
            location: FieldsList.ECOMMERCE_TRANSACTION_ITEM_ILT_LOCATION,
            item_type: FieldsList.ECOMMERCE_TRANSACTION_ITEM_TYPE
        };

        // detect if the ordering field is a field of the badges
        const orderField = this.extractLegacyOrderingFieldByEntity(legacyOrderByField, 'ecommerce_item');

        for (const field in legacyFields) {
            let aamonField = '';
            if (ecommerceTransactionItemFieldsLegacyMapper[field]) {
                aamonField = ecommerceTransactionItemFieldsLegacyMapper[field];
                if (!mandatoryFields[aamonField]) mappedFields.push(aamonField);
            }

            // set the ordering
            if (orderField === field && aamonField) {
                legacyOrderField = {
                    field: aamonField,
                    direction: legacyOrderByField.type === 'ASC' ? 'asc' : 'desc',
                };
            }
        }

        return {
            fields: mappedFields,
            orderByDescriptor: legacyOrderField ? legacyOrderField : undefined,
        };
    }

    protected mapContentPartnersSelectedFields(legacyFields: any, legacyOrderByField: any, mandatoryFields: { [key: string]: string } = {}): FieldsDescriptor {
        if (!legacyFields) {
            return {
                fields: []
            };
        }
        const mappedFields: string[] = [];
        let legacyOrderField: LegacyOrderByParsed | undefined;

        const contentPartnersFieldsLegacyMapper: { [key: string]: string } = {
            user_is_affiliate: FieldsList.CONTENT_PARTNERS_AFFILIATE,
            cp_referral_code: FieldsList.CONTENT_PARTNERS_REFERRAL_LINK_CODE,
            cp_name: FieldsList.CONTENT_PARTNERS_REFERRAL_LINK_SOURCE
        };

        // detect if the ordering field is a field of the badges
        const orderField = this.extractLegacyOrderingFieldByEntity(legacyOrderByField, 'content_partner');

        for (const field in legacyFields) {
            let aamonField = '';
            if (contentPartnersFieldsLegacyMapper[field]) {
                aamonField = contentPartnersFieldsLegacyMapper[field];
                if (!mandatoryFields[aamonField]) mappedFields.push(aamonField);
            }

            // set the ordering
            if (orderField === field && aamonField) {
                legacyOrderField = {
                    field: aamonField,
                    direction: legacyOrderByField.type === 'ASC' ? 'asc' : 'desc',
                };
            }
        }

        return {
            fields: mappedFields,
            orderByDescriptor: legacyOrderField ? legacyOrderField : undefined,
        };
    }

    protected mapAssetSelectedFields(legacyFields: any, legacyOrderByField: any, mandatoryFields: { [key: string]: string } = {}): FieldsDescriptor {
        if (!legacyFields) {
            return {
                fields: []
            };
        }
        const mappedFields: string[] = [];
        let legacyOrderField: LegacyOrderByParsed | undefined;

        const assetsFieldsLegacyMapper: { [key: string]: string } = {
            title: FieldsList.ASSET_NAME,
            published_by: FieldsList.PUBLISHED_BY,
            published_on: FieldsList.PUBLISHED_ON,
            channels: FieldsList.CHANNELS,
        };

        // detect if the ordering field is a field of the badges
        const orderField = this.extractLegacyOrderingFieldByEntity(legacyOrderByField, 'asset');

        for (const field in legacyFields) {
            let aamonField = '';
            if (assetsFieldsLegacyMapper[field]) {
                aamonField = assetsFieldsLegacyMapper[field];
                if (!mandatoryFields[aamonField]) mappedFields.push(aamonField);
            }

            // set the ordering
            if (orderField === field && aamonField) {
                legacyOrderField = {
                    field: aamonField,
                    direction: legacyOrderByField.type === 'ASC' ? 'asc' : 'desc',
                };
            }
        }

        return {
            fields: mappedFields,
            orderByDescriptor: legacyOrderField ? legacyOrderField : undefined,
        };
    }

    protected mapStatSelectedFields(legacyFields: any, legacyOrderByField: any, mandatoryFields: { [key: string]: string } = {}): FieldsDescriptor {
        if (!legacyFields) {
            return {
                fields: []
            };
        }
        const mappedFields: string[] = [];
        let legacyOrderField: LegacyOrderByParsed | undefined;

        const statsFieldsLegacyMapper: { [key: string]: string } = {
            total_views: FieldsList.TOTAL_VIEWS,
            asset_rating: FieldsList.ASSET_RATING,
            questions: FieldsList.QUESTIONS,
            answers: FieldsList.ANSWERS,
            best_answers: FieldsList.BEST_ANSWERS,
            answers_likes: FieldsList.ANSWER_LIKES,
            answers_dislikes: FieldsList.ANSWER_DISLIKES,
            total_invited_people: FieldsList.INVITED_PEOPLE,
            global_watch_rate: FieldsList.GLOBAL_WATCH_RATE,
            average_reaction_time: FieldsList.AVERAGE_REACTION_TIME,
            watched: FieldsList.WATCHED,
            not_watched: FieldsList.NOT_WATCHED
        };

        // detect if the ordering field is a field of the badges
        const orderField = this.extractLegacyOrderingFieldByEntity(legacyOrderByField, 'stat');

        for (const field in legacyFields) {
            let aamonField = '';
            if (statsFieldsLegacyMapper[field]) {
                aamonField = statsFieldsLegacyMapper[field];
                if (!mandatoryFields[aamonField]) mappedFields.push(aamonField);
            }

            // set the ordering
            if (orderField === field && aamonField) {
                legacyOrderField = {
                    field: aamonField,
                    direction: legacyOrderByField.type === 'ASC' ? 'asc' : 'desc',
                };
            }
        }

        return {
            fields: mappedFields,
            orderByDescriptor: legacyOrderField ? legacyOrderField : undefined,
        };
    }

    protected mapContributionSelectedFields(legacyFields: any, legacyOrderByField: any, mandatoryFields: { [key: string]: string } = {}): FieldsDescriptor {
        if (!legacyFields) {
            return {
                fields: []
            };
        }
        const mappedFields: string[] = [];
        let legacyOrderField: LegacyOrderByParsed | undefined;

        const statsFieldsLegacyMapper: { [key: string]: string } = {
            number_of_uploaded_assets: FieldsList.UPLOADED_ASSETS,
            involved_channels: FieldsList.INVOLVED_CHANNELS,
            published_status: FieldsList.PUBLISHED_ASSETS,
            unpublished_status: FieldsList.UNPUBLISHED_ASSETS,
            private_assets: FieldsList.PRIVATE_ASSETS
        };

        // detect if the ordering field is a field of the assignment
        const orderField = this.extractLegacyOrderingFieldByEntity(legacyOrderByField, 'as');

        for (const field in legacyFields) {
            let aamonField = '';
            if (statsFieldsLegacyMapper[field]) {
                aamonField = statsFieldsLegacyMapper[field];
                if (!mandatoryFields[aamonField]) mappedFields.push(aamonField);
            }

            // set the ordering
            if (orderField === field && aamonField) {
                legacyOrderField = {
                    field: aamonField,
                    direction: legacyOrderByField.type === 'ASC' ? 'asc' : 'desc',
                };
            }
        }

        return {
            fields: mappedFields,
            orderByDescriptor: legacyOrderField ? legacyOrderField : undefined,
        };
    }

    mapLegacyFieldsToAmmonByEntity(legacyFields: any, legacyOrderByField: any, mandatoryFields: { [key: string]: string } = {}, fieldsMapper: { [key: string]: string }, legacyEntityName: string, additionalFieldPrefix?: string): FieldsDescriptor {
        if (!legacyFields) {
            return {
                fields: []
            };
        }
        const mappedFields: string[] = [];
        let legacyOrderField: LegacyOrderByParsed | undefined;

        // detect if the ordering field is a field of the user
        const orderField = this.extractLegacyOrderingFieldByEntity(legacyOrderByField, legacyEntityName);

        for (const field in legacyFields) {
            let aamonField = '';
            if (fieldsMapper[field]) {
                aamonField = fieldsMapper[field];
                if (!mandatoryFields[aamonField]) mappedFields.push(aamonField);
            } else if (additionalFieldPrefix && /^\d+$/.test(field)) {
                // this is an additional field, in aamon is with this syntax
                const aamonField = `${additionalFieldPrefix}${field}`;
                mappedFields.push(aamonField);
            }

            // set the ordering
            if (orderField === field && aamonField) {
                legacyOrderField = {
                    field: aamonField,
                    direction: legacyOrderByField.type === 'ASC' ? 'asc' : 'desc',
                };
            }
        }


        return {
            fields: mappedFields,
            orderByDescriptor: legacyOrderField ? legacyOrderField : undefined,
        };
    }

    /**
     * Extract the enrollments preferences from the legacy selection to aamon
     * @param filters {any} The filters object from legacy
     * @param report {ReportManagerInfo} The report
     */
    protected extractLegacyEnrollmentStatus(filters: any, report: ReportManagerInfo) {
        if (filters.subscription_status !== 'all') {

            // reset the default field
            (report.enrollment as Enrollment).completed = false;
            (report.enrollment as Enrollment).inProgress = false;
            (report.enrollment as Enrollment).notStarted = false;
            (report.enrollment as Enrollment).waitingList = false;
            (report.enrollment as Enrollment).suspended = false;

            switch (filters.subscription_status) {
                case 'not_started':
                    (report.enrollment as Enrollment).notStarted = true;
                    break;
                case 'in_progress':
                    (report.enrollment as Enrollment).inProgress = true;
                    break;
                case 'completed':
                    (report.enrollment as Enrollment).completed = true;
                    break;
            }
        }
    }

    protected mapTrainingMaterialFields(legacyFields: any, legacyOrderByField: any, mandatoryFields: { [key: string]: string } = {}): FieldsDescriptor {
        if (!legacyFields) {
            return {
                fields: []
            };
        }
        const mappedFields: string[] = [];
        let legacyOrderField: LegacyOrderByParsed | undefined;

        // detect if the ordering field is a field of the user
        const orderField = this.extractLegacyOrderingFieldByEntity(legacyOrderByField, 'learning_object');

        const trainingFieldsLegacyMapper: { [key: string]: string } = {
            objectType: FieldsList.LO_TYPE,
            firstAttempt: FieldsList.LO_FIRST_ATTEMPT,
            dateAttempt: FieldsList.LO_DATE_ATTEMPT,
            dateLoComplete: FieldsList.LO_DATE_COMPLETE,
            status: FieldsList.LO_STATUS,
            score: FieldsList.LO_SCORE,
            version: FieldsList.LO_VERSION
        };

        for (const field in legacyFields) {
            let aamonField = '';
            if (trainingFieldsLegacyMapper[field]) {
                aamonField = trainingFieldsLegacyMapper[field];
                if (!mandatoryFields[aamonField]) mappedFields.push(aamonField);
            }

            // set the ordering
            if (orderField === field && aamonField) {
                legacyOrderField = {
                    field: aamonField,
                    direction: legacyOrderByField.type === 'ASC' ? 'asc' : 'desc',
                };
            }
        }

        return {
            fields: mappedFields,
            orderByDescriptor: legacyOrderField ? legacyOrderField : undefined,
        };
    }

    /**
     * Manage the EnrollmentDate, CompletionDate and the condition between them
     */
    protected composeDateOptionsFilter(fieldDateInscr = 'date_inscr', fieldDateComplete = 'date_complete'): string {

        let dateOptionsSql = '';
        let enrollmentSql = '';
        let completionSql = '';

        if (this.info.enrollmentDate) {
            enrollmentSql = this.buildDateFilter(fieldDateInscr, this.info.enrollmentDate, '', true);
        }
        if (this.info.completionDate) {
            completionSql = this.buildDateFilter(fieldDateComplete, this.info.completionDate, '', true);
        }

        // manage the OR / AND condition between dates
        if (enrollmentSql !== '' && completionSql !== '' && this.info.conditions !== DateOptions.CONDITIONS) {
            dateOptionsSql += ` AND (${enrollmentSql} OR ${completionSql})`;
        } else if (enrollmentSql !== '' || completionSql !== '') {
            dateOptionsSql += enrollmentSql !== '' ? ` AND${enrollmentSql}` : '';
            dateOptionsSql += completionSql !== '' ? ` AND${completionSql}` : '';
        }
        return dateOptionsSql;
    }

    /**
     * Manage the EnrollmentDate, CompletionDate and the condition between them
     */
    protected composeLearningPlanDateOptionsFilter(fieldAssignDate: string, fieldDateComplete: string, addCompletionSqlCondition: string, preserveColumnCase = false): string {

        let dateOptionsSql = '';
        let enrollmentSql = '';
        let completionSql = '';

        if (this.info.enrollmentDate) {
            enrollmentSql = this.buildDateFilter(fieldAssignDate, this.info.enrollmentDate, '', true);
        }
        if (this.info.completionDate) {
            completionSql = this.buildDateFilter(fieldDateComplete, this.info.completionDate, '', true, preserveColumnCase);
            if (completionSql !== '') {
                completionSql = `(${completionSql} AND ${addCompletionSqlCondition})`;
            }
        }

        // manage the OR / AND condition between dates
        if (enrollmentSql !== '' && completionSql !== '' && this.info.conditions !== DateOptions.CONDITIONS) {
            dateOptionsSql += ` AND (${enrollmentSql} OR ${completionSql})`;
        } else if (enrollmentSql !== '' || completionSql !== '') {
            dateOptionsSql += enrollmentSql !== '' ? ` AND${enrollmentSql}` : '';
            dateOptionsSql += completionSql !== '' ? ` AND${completionSql}` : '';
        }
        return dateOptionsSql;
    }


    /**
     * Manage the Session Start Date, Session End Date and the condition between them
     */
    protected composeSessionDateOptionsFilter(dateBegin: string, dateEnd: string): string {
        let dateOptionsSql = '';
        let startDateSql = '';
        let endDateSql = '';
        if (!this.info.sessionDates) return dateOptionsSql;

        if (this.info.sessionDates.startDate) {
            startDateSql = this.buildDateFilter(`${dateBegin}`, this.info.sessionDates.startDate, '', true);
        }
        if (this.info.sessionDates.endDate) {
            endDateSql = this.buildDateFilter(`${dateEnd}`, this.info.sessionDates.endDate, '', true);
        }

        // manage the OR / AND condition between dates
        if (startDateSql !== '' && endDateSql !== '' && this.info.sessionDates.conditions !== DateOptions.CONDITIONS) {
            dateOptionsSql += ` (${startDateSql} OR ${endDateSql})`;
        } else if (startDateSql !== '' && endDateSql !== '') {
            dateOptionsSql += ` (${startDateSql} AND ${endDateSql})`;
        } else if (startDateSql !== '' || endDateSql !== '') {
            dateOptionsSql += startDateSql !== '' ? `${startDateSql}` : '';
            dateOptionsSql += endDateSql !== '' ? ` ${endDateSql}` : '';
        }

        return dateOptionsSql;
    }

    protected composeSessionAttendanceFilter(table: string, field = 'attendance_type'): string {
        if (this.info.sessionAttendanceType && (this.info.sessionAttendanceType.blended || this.info.sessionAttendanceType.flexible || this.info.sessionAttendanceType.fullOnline || this.info.sessionAttendanceType.fullOnsite)) {
            const attendancesTypes: string[] = [];

            if (this.info.sessionAttendanceType.blended) {
                attendancesTypes.push(AttendancesTypes.BLENDED);
            }
            if (this.info.sessionAttendanceType.flexible) {
                attendancesTypes.push(AttendancesTypes.FLEXIBLE);
            }
            if (this.info.sessionAttendanceType.fullOnline) {
                attendancesTypes.push(AttendancesTypes.FULLONLINE);
            }
            if (this.info.sessionAttendanceType.fullOnsite) {
                attendancesTypes.push(AttendancesTypes.FULLONSITE);
            }

            if (attendancesTypes.length > 0 && attendancesTypes.length !== 4) {
                return `${this.composeTableField(table, field)} IN ('${attendancesTypes.join("','")}')`;
            }
        }

        return '';
    }

    /**
     * Manage the EnrollmentDate, CompletionDate, EnrollmentArchivedDate and the condition between them
     */
    protected composeDateOptionsWithArchivedEnrollmentFilter(fieldDateInscr: string, fieldDateComplete: string, fieldDateArchived: string): string {

        let dateOptionsSql = '';
        let enrollmentSql = '';
        let completionSql = '';
        let archivedSql = '';

        if (this.info.enrollmentDate) {
            enrollmentSql = this.buildDateFilter(fieldDateInscr, this.info.enrollmentDate, '', true);
        }
        if (this.info.completionDate) {
            completionSql = this.buildDateFilter(fieldDateComplete, this.info.completionDate, '', true);
        }
        if (this.info.archivingDate) {
            archivedSql = this.buildDateFilter(fieldDateArchived, this.info.archivingDate, '', true);
        }

        const isAllDateOptionsFiltersAtLeastOneCondition = enrollmentSql !== '' && completionSql !== '' && archivedSql !== '' && this.info.conditions !== DateOptions.CONDITIONS;
        const isEnrollmentAndCompletionDateFiltersAtLeastOneCondition = enrollmentSql !== '' && completionSql !== '' && this.info.conditions !== DateOptions.CONDITIONS;
        const isEnrollmentAndArchivedDateFiltersAtLeastOneCondition = enrollmentSql !== '' && archivedSql !== '' && this.info.conditions !== DateOptions.CONDITIONS;
        const isCompletionAndArchivedDateFiltersAtLeastOneCondition = completionSql !== '' && archivedSql !== '' && this.info.conditions !== DateOptions.CONDITIONS;
        const appendDateFilterInAndCondition = enrollmentSql !== '' || completionSql !== '' || archivedSql !== '';

        // manage the OR / AND condition between dates
        switch (true) {
            case isAllDateOptionsFiltersAtLeastOneCondition:
                dateOptionsSql += ` AND (${enrollmentSql} OR${completionSql} OR${archivedSql})`;
                break;
            case isEnrollmentAndCompletionDateFiltersAtLeastOneCondition:
                dateOptionsSql += ` AND (${enrollmentSql} OR${completionSql})`;
                break;
            case isEnrollmentAndArchivedDateFiltersAtLeastOneCondition:
                dateOptionsSql += 'AND (';
                dateOptionsSql += enrollmentSql !== '' ? `${enrollmentSql}` : '';
                dateOptionsSql += archivedSql !== '' ? ` OR${archivedSql}` : '';
                dateOptionsSql += ')';
                break;
            case isCompletionAndArchivedDateFiltersAtLeastOneCondition:
                dateOptionsSql += 'AND (';
                dateOptionsSql += completionSql !== '' ? `${completionSql}` : '';
                dateOptionsSql += archivedSql !== '' ? ` OR${archivedSql}` : '';
                dateOptionsSql += ')';
                break;
            case appendDateFilterInAndCondition:
                dateOptionsSql += enrollmentSql !== '' ? ` AND${enrollmentSql}` : '';
                dateOptionsSql += completionSql !== '' ? ` AND${completionSql}` : '';
                dateOptionsSql += archivedSql !== '' ? ` AND${archivedSql}` : '';
                break;
            default: break;
        }

        return dateOptionsSql;
    }


    /**
     * Manage the CreationDateOpts, ExpirationDateOpts and the condition between them
     */
    protected composeReportUsersDateOptionsFilter(): string {

        let dateOptionsSql = '';
        let creationSql = '';
        let expirationSql = '';

        if (this.info.creationDateOpts) {
            creationSql = this.buildDateFilter('register_date', this.info.creationDateOpts, '', true);
        }
        if (this.info.expirationDateOpts) {
            expirationSql = this.buildDateFilter('expiration', this.info.expirationDateOpts, '', true);
        }

        // manage the OR / AND condition between dates
        if (creationSql !== '' && expirationSql !== '' && this.info.conditions !== DateOptions.CONDITIONS) {
            dateOptionsSql += ` AND (${creationSql} OR ${expirationSql})`;
        } else if (creationSql !== '' || expirationSql !== '') {
            dateOptionsSql += creationSql !== '' ? ` AND${creationSql}` : '';
            dateOptionsSql += expirationSql !== '' ? ` AND${expirationSql}` : '';
        }
        return dateOptionsSql;
    }


    /**
     * check if there is a custom translation for the additional field and match with the lms language
     * @param field The additional field
     */
    protected setAdditionalFieldTranslation(field: any): string {
        const langCode = this.session.user.getLangCode();
        const additionalField = (field.name.values[langCode] && field.name.values[langCode] !== '')
            ? field.name.values[langCode]
            : field.name.value;

        return additionalField;
    }

    protected async getAvailableUserExtraFields(): Promise<ReportField[]> {
        const result: ReportField[] = [];
        const userExtraFields = await this.session.getHydra().getUserExtraFields();
        for (const field of userExtraFields.data.items) {
            const fieldKey = 'user_extrafield_' + field.id;
            result.push({
                field: fieldKey,
                idLabel: (field.title || fieldKey),
                mandatory: false,
                isAdditionalField: true,
                translation: field.title
            });
        }
        return result;
    }

    protected async getAvailableCourseExtraFields(): Promise<ReportField[]> {
        const result: ReportField[] = [];
        const courseExtraFields = await this.session.getHydra().getCourseExtraFields();
        for (const field of courseExtraFields.data.items) {
            result.push({
                field: 'course_extrafield_' + field.id,
                idLabel: this.setAdditionalFieldTranslation(field),
                mandatory: false,
                isAdditionalField: true,
                translation: this.setAdditionalFieldTranslation(field)
            });
        }
        return result;
    }

    protected async getAvailableEnrollmentExtraFields(): Promise<ReportField[]> {
        const result: ReportField[] = [];
        const courseuserExtraFields = await this.session.getHydra().getCourseuserExtraFields();
        for (const field of courseuserExtraFields.data) {
            result.push({
                field: 'courseuser_extrafield_' + field.id,
                idLabel: field.name,
                mandatory: false,
                isAdditionalField: true,
                translation: field.name
            });
        }
        return result;
    }

    protected async getAvailableILTExtraFields(): Promise<ReportField[]> {
        const result: ReportField[] = [];
        const sessionExtraFields = await this.session.getHydra().getILTExtraFields();
        for (const field of sessionExtraFields.data.items) {
            const fieldKey = 'classroom_extrafield_' + field.id;
            result.push({
                field: fieldKey,
                idLabel: this.setAdditionalFieldTranslation(field) || fieldKey,
                mandatory: false,
                isAdditionalField: true,
                translation: this.setAdditionalFieldTranslation(field)
            });
        }
        return result;
    }

    protected async getAvailableTranscriptExtraFields(): Promise<ReportField[]> {
        const result: ReportField[] = [];
        const transcriptExtraFields = await this.session.getHydra().getTranscriptExtraFields();
        for (const field of transcriptExtraFields.data.items) {
            const fieldKey = 'external_activity_extrafield_' + field.id;
            result.push({
                field: fieldKey,
                idLabel: (field.title || fieldKey),
                mandatory: false,
                isAdditionalField: true,
                translation: field.title
            });
        }
        return result;
    }

    protected async getAvailableLearningPlanExtraFields(): Promise<ReportField[]> {
        const result: ReportField[] = [];
        const learningPlanExtraFields = await this.session.getHydra().getLearningPlanExtraFields();
        for (const field of learningPlanExtraFields.data.items) {
            const fieldKey = 'lp_extrafield_' + field.id;
            result.push({
                field: fieldKey,
                idLabel: this.setAdditionalFieldTranslation(field) || fieldKey,
                mandatory: false,
                isAdditionalField: true,
                translation: this.setAdditionalFieldTranslation(field)
            });
        }
        return result;
    }

    /**
     * Check if an user additional field was already present in the Athena DB
     * @param idField The id of the field to check
     */
    public async checkUserAdditionalFieldInAthena(idField: number): Promise<boolean> {
        if (typeof this.athenaUserAdditionalFields === 'undefined') {
            // Cache the user additional fields for the current call
            const athena = this.session.getAthena();
            const query = `SELECT id_field AS id FROM ${TablesList.CORE_USER_FIELD}`;
            let data: AthenaFieldsDataResponse;
            try {
                data = await athena.connection.query(query) as AthenaFieldsDataResponse;
            } catch (e: any) {
                throw (e);
            }
            this.athenaUserAdditionalFields = data.Items.map(a => parseInt(a.id, 10));
        }

        if (typeof this.athenaUserAdditionalFields !== 'undefined' && this.athenaUserAdditionalFields.length > 0) {
            return this.athenaUserAdditionalFields.indexOf(idField) >= 0;
        }

        return false;
    }

    /**
     * Check if an course additional field was already present in the Athena DB
     * @param idField The id of the field to check
     */
    public async checkCourseAdditionalFieldInAthena(idField: number): Promise<boolean> {
        if (typeof this.athenaCourseAdditionalFields === 'undefined') {
            // Cache the user additional fields for the current call
            const athena = this.session.getAthena();
            const query = `SELECT id_field AS id FROM ${TablesList.LEARNING_COURSE_FIELD}`;
            let data: AthenaFieldsDataResponse;
            try {
                data = await athena.connection.query(query) as AthenaFieldsDataResponse;
            } catch (e: any) {
                throw (e);
            }
            this.athenaCourseAdditionalFields = data.Items.map(a => parseInt(a.id, 10));
        }

        if (typeof this.athenaCourseAdditionalFields !== 'undefined' && this.athenaCourseAdditionalFields.length > 0) {
            return this.athenaCourseAdditionalFields.indexOf(idField) >= 0;
        }

        return false;
    }

    /**
     * Check if an enrollment additional field was already present in the Athena DB
     * @param idField The id of the field to check
     */
    public async checkEnrollmentAdditionalFieldInAthena(idField: number): Promise<boolean> {
        if (typeof this.athenaEnrollmentAdditionalFields === 'undefined') {
            // Cache the user additional fields for the current call
            const athena = this.session.getAthena();
            const query = `SELECT id FROM ${TablesList.LEARNING_ENROLLMENT_FIELDS}`;
            let data: AthenaFieldsDataResponse;
            try {
                data = await athena.connection.query(query) as AthenaFieldsDataResponse;
            } catch (e: any) {
                throw (e);
            }
            this.athenaEnrollmentAdditionalFields = data.Items.map(a => parseInt(a.id, 10));
        }

        if (typeof this.athenaEnrollmentAdditionalFields !== 'undefined' && this.athenaEnrollmentAdditionalFields.length > 0) {
            return this.athenaEnrollmentAdditionalFields.indexOf(idField) >= 0;
        }

        return false;
    }

    /**
     * Check if an external activity additional field was already present in the Athena DB
     * @param idField The id of the field to check
     */
    public async checkExternalActivityAdditionalFieldInAthena(idField: number): Promise<boolean> {
        if (typeof this.athenaTranscriptsAdditionalFields === 'undefined') {
            // Cache the user additional fields for the current call
            const athena = this.session.getAthena();
            const query = `SELECT id_field AS id FROM ${TablesList.TRANSCRIPTS_FIELD}`;
            let data: AthenaFieldsDataResponse;
            try {
                data = await athena.connection.query(query) as AthenaFieldsDataResponse;
            } catch (e: any) {
                throw (e);
            }
            this.athenaTranscriptsAdditionalFields = data.Items.map(a => parseInt(a.id, 10));
        }

        if (typeof this.athenaTranscriptsAdditionalFields !== 'undefined' && this.athenaTranscriptsAdditionalFields.length > 0) {
            return this.athenaTranscriptsAdditionalFields.indexOf(idField) >= 0;
        }

        return false;
    }

    protected async getAdditionalFieldsFilters(userExtraFields: UserExtraFieldsResponse, jsonUserFilter?: JsonUserFilter): Promise<string[]> {
        const res: string[] = [];
        let userAdditionalFieldsFilter = this.info.userAdditionalFieldsFilter;
        if (typeof jsonUserFilter !== 'undefined') {
            userAdditionalFieldsFilter = jsonUserFilter.userAdditionalFieldsFilter;
        }
        if (this.info && userAdditionalFieldsFilter) {
            for (const userField of userExtraFields.data.items) {
                const fieldId = parseInt(userField.id, 10);
                if (fieldId in userAdditionalFieldsFilter && userField.type === 'dropdown') {
                    if ((!this.session.platform.isDatalakeV3ToggleActive() && await this.checkUserAdditionalFieldInAthena(fieldId) === true) || this.session.platform.isDatalakeV3ToggleActive()) {
                        res.push(`AND ${TablesListAliases.CORE_USER_FIELD_VALUE}.field_${fieldId} = ${userAdditionalFieldsFilter[fieldId]}`);
                    }
                }
            }
        }

        return res;
    }

    protected getCertificationsFilterSnowflake(): string[] {
        if (!this.info.certifications) {
            return [];
        }
        const where = [];
        const archivedCertificationsFilter = ` OR ${TablesListAliases.CERTIFICATION_USER}."archived" = 1`;

        // Active or Expired filter
        if (this.info.certifications.activeCertifications && this.info.certifications.expiredCertifications) {
            // Fine in this way, we don't need any filter in this case
        } else if (this.info.certifications.activeCertifications) {

            let activeCertificationsFilter = `${TablesListAliases.CERTIFICATION_USER}."on_datetime" <= CURRENT_TIMESTAMP
                AND (${TablesListAliases.CERTIFICATION_USER}."expire_at" > CURRENT_TIMESTAMP OR ${TablesListAliases.CERTIFICATION_USER}."expire_at" IS NULL)`;

            // If archivedCertification filter is enabled append it to the activeCertification filter (in OR condition)
            if (this.info.certifications.archivedCertifications) {
                activeCertificationsFilter = activeCertificationsFilter + archivedCertificationsFilter;
            }
            where.push(`AND (${activeCertificationsFilter})`);

        } else if (this.info.certifications.expiredCertifications) {
            let expiredCertificationsFilter = `${TablesListAliases.CERTIFICATION_USER}."expire_at" < CURRENT_TIMESTAMP`;

            // Append archivedCertifications to expiredCertificationsFilter
            if (this.info.certifications.archivedCertifications) {
                expiredCertificationsFilter = expiredCertificationsFilter + archivedCertificationsFilter;
            }
            where.push(`AND (${expiredCertificationsFilter})`);
        }

        // Archived Certifications filter
        // Filter only for archived records
        if (this.info.certifications.archivedCertifications && !this.info.certifications.activeCertifications && !this.info.certifications.expiredCertifications) {
            where.push(`AND ${TablesListAliases.CERTIFICATION_USER}."archived" = 1`);
        }
        // Exclude the archived records (case when active and expired certification filters are enabled)
        if (!this.info.certifications.archivedCertifications) {
            where.push(`AND ${TablesListAliases.CERTIFICATION_USER}."archived" = 0`);
        }

        // Dates filter
        let certificationDateFilter = '';
        let expirationDateFilter = '';

        if (!this.info.certifications.certificationDate.any) {
            certificationDateFilter = this.buildDateFilter(TablesListAliases.CERTIFICATION_USER + '.on_datetime', this.info.certifications.certificationDate, '', true);
        }

        if (!this.info.certifications.certificationExpirationDate.any) {
            expirationDateFilter = this.buildDateFilter(TablesListAliases.CERTIFICATION_USER + '.expire_at', this.info.certifications.certificationExpirationDate, '', true);
        }

        if (certificationDateFilter !== '' && expirationDateFilter !== '' && this.info.certifications.conditions !== DateOptions.CONDITIONS) {
            where.push(`AND (${certificationDateFilter} OR ${expirationDateFilter})`);
        } else if (certificationDateFilter !== '' && expirationDateFilter !== '') {
            where.push(`AND (${certificationDateFilter} AND ${expirationDateFilter})`);
        } else if (certificationDateFilter !== '') {
            where.push(`AND ${certificationDateFilter}`);
        } else if (expirationDateFilter !== '') {
            where.push(`AND ${expirationDateFilter}`);
        }
        return where;
    }

    protected getAssetsFiltersSnowflake(): string {
        const allAssets = this.info.assets ? this.info.assets.all : false;
        if (allAssets) return '';
        const assetsSelection = this.info.assets ? this.info.assets?.assets.map(a => a.id) : [];
        const channelsSelection = this.info.assets ? this.info.assets.channels.map(c => c.id) : [];
        let assetChannelFilter = '';
        if (channelsSelection.length > 0) {
            const filterQuery = `SELECT cha."idasset"
                                 FROM app7020_channel_assets AS cha
                                 WHERE cha."idchannel" IN (${channelsSelection.join(',')})
                                   AND cha."asset_type" = 1`;
            assetChannelFilter += `${TablesListAliases.APP7020_CONTENT}."id" IN (${filterQuery})`;
        }
        if (assetsSelection.length > 0) {
            if (assetChannelFilter.length > 0) assetChannelFilter += ' OR ';
            assetChannelFilter += ` ${TablesListAliases.APP7020_CONTENT}."id" IN (${assetsSelection.join(',')})`;
        }
        return assetChannelFilter.length > 0 ? ` AND (${assetChannelFilter})` : ' AND FALSE';
    }

    protected getAdditionalUsersFieldsFiltersSnowflake(queryModel: any, userExtraFields: UserExtraFieldsResponse, jsonUserFilter?: JsonUserFilter): void {
        const res: string[] = [];
        const {from, cte} = queryModel;
        let userAdditionalFieldsFilter = this.info.userAdditionalFieldsFilter;
        if (typeof jsonUserFilter !== 'undefined') {
            userAdditionalFieldsFilter = jsonUserFilter.userAdditionalFieldsFilter;
        }
        let withTbale = `${TablesList.CORE_USER_FIELD_FILTER_WITH} AS (
            SELECT DISTINCT "id_user"
            FROM ${TablesList.CORE_USER_FIELD_VALUE}
            GROUP BY "id_user"`;

        const fieldConditions = [];
        for (const userField of userExtraFields.data.items) {
            const fieldId = parseInt(userField.id, 10);
            if (fieldId in userAdditionalFieldsFilter && userField.type === 'dropdown') {
                if (fieldConditions.length > 0) {
                    fieldConditions.push(` OR ("id_field" = ${fieldId} AND "field_value" = ${userAdditionalFieldsFilter[fieldId]})`);
                } else {
                    fieldConditions.push(`("id_field" = ${fieldId} AND "field_value" = ${userAdditionalFieldsFilter[fieldId]})`);
                }
            }
        }
        withTbale += ` HAVING SUM(IFF(${fieldConditions.join('')}, 1,0)) = ${fieldConditions.length}`;

        if (fieldConditions.length > 0) {
            withTbale += `
            )`;
            cte.push(withTbale);

            let joinField = `${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."iduser"`;
            switch (this.info.type) {
                case ReportsTypes.QUERY_BUILDER_DETAIL:
                case ReportsTypes.USERS:
                case ReportsTypes.USER_CONTRIBUTIONS:
                case ReportsTypes.VIEWER_ASSET_DETAILS:
                    joinField = `${TablesListAliases.CORE_USER}."idst"`;
                    break;
                case ReportsTypes.CERTIFICATIONS_USERS:
                case ReportsTypes.USERS_CERTIFICATIONS:
                    joinField = `${TablesListAliases.CERTIFICATION_USER}."id_user"`;
                    break;
                case ReportsTypes.USERS_BADGES:
                    joinField = `${TablesListAliases.GAMIFICATION_ASSIGNED_BADGES}."id_user"`;
                    break;
                case ReportsTypes.USERS_EXTERNAL_TRAINING:
                    joinField = `${TablesListAliases.TRANSCRIPTS_RECORD}."id_user"`;
                    break;
                case ReportsTypes.LP_USERS_STATISTICS:
                case ReportsTypes.USERS_LP:
                    joinField = `${TablesListAliases.LEARNING_COURSEPATH_USER}."iduser"`;
                    break;
                case ReportsTypes.ECOMMERCE_TRANSACTION:
                    joinField = `${TablesListAliases.ECOMMERCE_TRANSACTION}."id_user"`;
                    break;
            }

            from.push(`JOIN ${TablesList.CORE_USER_FIELD_FILTER_WITH} AS ${TablesListAliases.CORE_USER_FIELD_FILTER_WITH} ON ${TablesListAliases.CORE_USER_FIELD_FILTER_WITH}."id_user" = ${joinField}`);
        }
    }

    protected getAdditionalUsersFieldsFiltersSnowflakeQueryBuilder(queryModel: any, userExtraFields: UserExtraFieldsResponse, jsonUserFilter?: JsonUserFilter): string[] {
        const res: string[] = [];
        const {from, join} = queryModel;
        let userAdditionalFieldsFilter = this.info.userAdditionalFieldsFilter;
        if (typeof jsonUserFilter !== 'undefined') {
            userAdditionalFieldsFilter = jsonUserFilter.userAdditionalFieldsFilter;
        }
        for (const userField of userExtraFields.data.items) {
            const fieldId = parseInt(userField.id, 10);
            if (fieldId in userAdditionalFieldsFilter && userField.type === 'dropdown') {
                res.push(`AND (${TablesListAliases.CORE_USER_FIELD_VALUE}."id_field" = ${fieldId} AND ${TablesListAliases.CORE_USER_FIELD_VALUE}."field_value" = ${userAdditionalFieldsFilter[fieldId]})`);
                if (!join.includes(joinedTables.CORE_USER_FIELD_VALUE)) {
                    join.push(joinedTables.CORE_USER_FIELD_VALUE);
                    let joinField = `${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."iduser"`;
                    switch (this.info.type) {
                        case ReportsTypes.QUERY_BUILDER_DETAIL:
                        case ReportsTypes.USERS:
                        case ReportsTypes.USER_CONTRIBUTIONS:
                        case ReportsTypes.VIEWER_ASSET_DETAILS:
                            joinField = `${TablesListAliases.CORE_USER}."idst"`;
                            break;
                        case ReportsTypes.CERTIFICATIONS_USERS:
                        case ReportsTypes.USERS_CERTIFICATIONS:
                            joinField = `${TablesListAliases.CERTIFICATION_USER}."id_user"`;
                            break;
                        case ReportsTypes.USERS_BADGES:
                            joinField = `${TablesListAliases.GAMIFICATION_ASSIGNED_BADGES}."id_user"`;
                            break;
                        case ReportsTypes.USERS_EXTERNAL_TRAINING:
                            joinField = `${TablesListAliases.TRANSCRIPTS_RECORD}."id_user"`;
                            break;
                    }
                    from.push(`LEFT JOIN ${TablesList.CORE_USER_FIELD_VALUE} AS ${TablesListAliases.CORE_USER_FIELD_VALUE} ON ${TablesListAliases.CORE_USER_FIELD_VALUE}."id_user" = ${joinField}`);
                }
            }
        }

        return res;
    }

    /**
     * Get the default enrollment date for the new report
     */
    public getDefaultDateOptions(): DateOptionsValueDescriptor {
        return {
            any: true,
            days: 1,
            type: '',
            operator: '',
            to: '',
            from: ''
        };
    }

    /**
     * Get the default value for the enrollment in new report
     */
    public getDefaultEnrollment(): Enrollment {
        const defaultEnrollment: Enrollment = {
            completed: true,
            inProgress: true,
            notStarted: true,
            waitingList: true,
            suspended: true,
            enrollmentsToConfirm: true,
            subscribed: true,
            overbooking: true,
        };
        // when removing the toggle, remember to update the type of enrollmentTypes field which will no longer be optional
        if (this.session.platform.isToggleMultipleEnrollmentCompletions()) {
            defaultEnrollment.enrollmentTypes = EnrollmentTypes.active;
        }
        return defaultEnrollment;
    }

    /**
     * Get default value for the publish status filter
     * @protected
     */
    protected getDefaultPublishStatus(): PublishStatusFilter {
        return {
            published: true,
            unpublished: true,
        };
    }

    /**
     * Get Default value for the session type filter
     * @protected
     */
    protected getDefaultSessionAttendanceType(): SessionAttendanceType {
        return {
            blended: true,
            flexible: true,
            fullOnline: true,
            fullOnsite : true
        };
    }

    /**
     * Get the default value for the externalTraining in new report
     */
    protected getDefaultExternalTrainingStatusFilter(): ExternalTrainingStatusFilter {
        return {
            approved: true,
            waiting: true,
            rejected: true,
        };
    }

    protected isUserExtraField(field: string): boolean {
        return field.indexOf('user_extrafield_') === 0;
    }

    protected isCourseExtraField(field: string): boolean {
        return field.indexOf('course_extrafield_') === 0;
    }

    protected isCourseUserExtraField(field: string): boolean {
        return field.indexOf('courseuser_extrafield_') === 0;
    }


    protected isLearningPlanExtraField(field: string): boolean {
        return field.indexOf('lp_extrafield_') === 0;
    }

    protected isWebinarExtraField(field: string): boolean {
        return field.indexOf('webinar_extrafield_') === 0;
    }

    protected isClassroomExtraField(field: string): boolean {
        return field.indexOf('classroom_extrafield_') === 0;
    }

    protected isExternalActivityExtraField(field: string): boolean {
        return field.indexOf('external_activity_extrafield_') === 0;
    }

    /**
     * Update the values of custom field title and return an array with all translations used
     * @param items
     * @param translations
     * @param type
     * @param translationValues
     * @protected
     */
    protected updateExtraFieldsDuplicated(items: any, translations: { [key: string]: string }, type: string, translationValues = []): string[] {
        for (const field of items) {
            let fieldCode = '';
            let nameObject: any;
            let oldValue = '';
            switch (type) {
                case 'user':
                    fieldCode = `user_extrafield_${field.id}`;
                    oldValue = field.title || fieldCode;
                    break;
                case 'course':
                    fieldCode = `course_extrafield_${field.id}`;
                    nameObject = field.name;
                    oldValue = this.setAdditionalFieldTranslation(field);
                    break;
                case 'lp':
                    fieldCode = `lp_extrafield_${field.id}`;
                    nameObject = field.name;
                    oldValue = this.setAdditionalFieldTranslation(field);
                    break;
                case 'classroom':
                    fieldCode = `classroom_extrafield_${field.id}`;
                    nameObject = field.name;
                    oldValue = this.setAdditionalFieldTranslation(field) || fieldCode;
                    break;
                case 'external-training':
                    fieldCode = `external_activity_extrafield_${field.id}`;
                    oldValue = field.title || fieldCode;
                    break;
                case 'webinar':
                    fieldCode = `webinar_extrafield_${field.id}`;
                    nameObject = field.name;
                    oldValue = nameObject.value;
                    break;
                case 'course-user':
                    fieldCode = `courseuser_extrafield_${field.id}`;
                    oldValue = field.name;
                    break;
                default:
                    throw new Error(`Type ${type} not recognized as extrafield type`);
            }
            // if it is not a selected field .. skip
            if (!this.info.fields.includes(fieldCode)) {
                continue;
            }

            // Exclude all the translations that are not View Options.
            // Ref. https://docebo.atlassian.net/browse/DD-30422
            const translationValuesFiltered = [];
            Object.keys(translations).forEach(translationKey => {
                if (!Object.values(FieldTranslation).includes(translationKey as any)) {
                    translationValuesFiltered.push(translations[translationKey]);
                }
            });

            const newValue = this.getUniqueValue(oldValue, translationValuesFiltered, translationValues);

            translationValues.push(newValue);

            switch (type) {
                case 'user':
                case 'external-training':
                    field.title = newValue;
                    break;
                case 'course':
                case 'classroom':
                case 'webinar':
                case 'lp':
                    nameObject = field.name;
                    nameObject.value = newValue;
                    break;
                case 'course-user':
                    field.name = newValue;
                    break;
                default:
                    throw new Error(`Type ${type} not recognized as extrafield type`);
            }
        }

        return translationValues;
    }

    /**
     * Check if the string for the column is already used. If it is already used, create new values
     * like as: name, name 1, name 2
     * See https://docebo.atlassian.net/browse/DOC-22325
     * @param value
     * @param translationValues
     * @private
     */
    private getUniqueValue(value: string, translationValues: string[], translationValuesAlreadyStored: string[]): string {
        let newValue = value;
        let count = 1;

        // Need to check all type of additional fields column name to avoid duplicates
        // https://docebo.atlassian.net/browse/DD-31037
        const allTranslationValues = [...translationValues, ...translationValuesAlreadyStored];

        translationValues = allTranslationValues.map(value => value.toLowerCase());

        while (translationValues.includes(newValue.toLowerCase())) {
            newValue = `${value} ${count}`;
            count = count + 1;
        }

        return newValue;
    }

    /**
     * Workaround to fix this situation https://docebo.atlassian.net/browse/DD-30786.
     * The DL 2.5 ingestion converts as wrong Date (ex. 0002-11-30 00:49:56) the default value of 0000-00-00 00:00:00.000.
     * We need to map this value with NULL in order to avoid bad data
     */
    public mapTimestampDefaultValueWithDLV2(column: string, timezone?: string): string {

        // Just use a comparison date
        const comparisonDate = '1700-01-01 00:00:00.000';

        let columnQuery = `CASE WHEN ${column} < TIMESTAMP '${comparisonDate}' THEN NULL ELSE ${column} END `;

        if (timezone) {
            columnQuery = `CASE WHEN ${column} < TIMESTAMP '${comparisonDate}' THEN NULL
            ELSE DATE_FORMAT(${column} AT TIME ZONE '${timezone}','%Y-%m-%d %H:%i:%s')
            END `;
        }

        return columnQuery;
    }

    /**
     * Workaround to fix this situation https://docebo.atlassian.net/browse/DD-31191.
     * The DL 2.5 ingestion converts as wrong Date (ex. 0101-01-01) the default value of 0000-00-00.
     * We need to map this value with NULL in order to avoid bad data
     */
    public mapDateDefaultValueWithDLV2(column: string): string {
        // Just use a comparison date
        if (!this.session.platform.isDatalakeV2Active()) {
            return column;
        }

        const comparisonDate = '0101-01-01';

        return `CASE WHEN ${column} = DATE '${comparisonDate}' THEN NULL ELSE ${column} END `;
    }

    // For reference https://trino.io/docs/current/functions/array.html
    public sortArrayValuesInSelectStatementAsc(arrayValues: string): string {
        return `ARRAY_SORT(${arrayValues}, (x,y) -> IF( x>y, 1, IF(x=y, 0 , -1)) )`;
    }

    /**
     * In the datalake 2.5 this field valid is an integer and not a boolean
     */
    public getCheckIsValidFieldClause() {
        const isDatalakeV2Active = this.session.platform.isDatalakeV2Active();

        if (isDatalakeV2Active) {
            return '= 1 ';
        }
        return '= true ';
    }
    /**
     * In the datalake 2.5 this field valid is an integer and not a boolean
     */
    public getCheckIsInvalidFieldClause() {
        const isDatalakeV2Active = this.session.platform.isDatalakeV2Active();

        if (isDatalakeV2Active) {
            return '= 0 ';
        }
        return '= false ';
    }

    /**
     * Archived Enrollment
     */
    protected showActive(): boolean {
        if (this.info.enrollment && !this.info.enrollment.enrollmentTypes) {
            return true;
        }
        return [EnrollmentTypes.active, EnrollmentTypes.activeAndArchived].includes(this.info.enrollment.enrollmentTypes);
    }

    protected showArchived(): boolean {
        return [EnrollmentTypes.archived, EnrollmentTypes.activeAndArchived].includes(this.info.enrollment.enrollmentTypes);
    }

    /**
     * Utils for Query
     */
    protected renderStringInQuerySelect(text: string): string {
        if (typeof text !== 'string') {
            throw(new Error('Invalid translation returned'));
        }
        return `"${text.replace(/"/g, '""')}"`;
    }

    protected renderStringInQueryCase(text: string): string {
        if (typeof text !== 'string') {
            throw(new Error('Invalid translation returned'));
        }
        return `'${text.replace(/'/g, "''")}'`;
    }

    protected querySelectLPFields(field: string, queryHelper: any): boolean {
        const {select, from, join, groupBy, translations} = queryHelper;
        const toggleActive = this.info.type === ReportsTypes.USERS_LP ? this.session.platform.isToggleNewLearningPlanManagementAndReportEnhancement() : true;
        switch (field) {
            case FieldsList.LP_NAME:
                select.push(`${TablesListAliases.LEARNING_COURSEPATH}."path_name" AS ${this.renderStringInQuerySelect(translations[FieldsList.LP_NAME])}`);
                groupBy.push(`${TablesListAliases.LEARNING_COURSEPATH}."path_name"`);
                break;
            case FieldsList.LP_CODE:
                select.push(`${TablesListAliases.LEARNING_COURSEPATH}."path_code" AS ${this.renderStringInQuerySelect(translations[FieldsList.LP_CODE])}`);
                groupBy.push(`${TablesListAliases.LEARNING_COURSEPATH}."path_code"`);
                break;
            case FieldsList.LP_CREDITS:
                select.push(`${TablesListAliases.LEARNING_COURSEPATH}."credits" AS ${this.renderStringInQuerySelect(translations[FieldsList.LP_CREDITS])}`);
                groupBy.push(`${TablesListAliases.LEARNING_COURSEPATH}."credits"`);
                break;
            case FieldsList.LP_UUID:
                if (!toggleActive) break;
                select.push(`${TablesListAliases.LEARNING_COURSEPATH}."uuid" AS ${this.renderStringInQuerySelect(translations[FieldsList.LP_UUID])}`);
                groupBy.push(`${TablesListAliases.LEARNING_COURSEPATH}."uuid"`);
                break;
            case FieldsList.LP_LAST_EDIT:
                if (!toggleActive) break;
                select.push(`${this.queryConvertTimezone(`${TablesListAliases.LEARNING_COURSEPATH}."last_update"`)} AS ${this.renderStringInQuerySelect(translations[FieldsList.LP_LAST_EDIT])}`);
                groupBy.push(`${TablesListAliases.LEARNING_COURSEPATH}."last_update"`);
                break;
            case FieldsList.LP_CREATION_DATE:
                if (!toggleActive) break;
                select.push(`${this.queryConvertTimezone(`${TablesListAliases.LEARNING_COURSEPATH}."create_date"`)} AS ${this.renderStringInQuerySelect(translations[FieldsList.LP_CREATION_DATE])}`);
                groupBy.push(`${TablesListAliases.LEARNING_COURSEPATH}."create_date"`);
                break;
            case FieldsList.LP_DESCRIPTION:
                if (!toggleActive) break;
                select.push(`${TablesListAliases.LEARNING_COURSEPATH}."path_descr" AS ${this.renderStringInQuerySelect(translations[FieldsList.LP_DESCRIPTION])}`);
                groupBy.push(`${TablesListAliases.LEARNING_COURSEPATH}."path_descr"`);
                break;
            case FieldsList.LP_STATUS:
                if (!toggleActive) break;
                select.push(`
                        CASE
                            WHEN ${TablesListAliases.LEARNING_COURSEPATH}."status" = 0 THEN ${this.renderStringInQueryCase(translations[FieldTranslation.LP_UNDER_MAINTENANCE])}
                            ELSE ${this.renderStringInQueryCase(translations[FieldTranslation.LP_PUBLISHED])}
                        END AS ${this.renderStringInQuerySelect(translations[FieldsList.LP_STATUS])}`);
                groupBy.push(`${TablesListAliases.LEARNING_COURSEPATH}."status"`);
                break;
            case FieldsList.LP_LANGUAGE:
                if (!toggleActive) break;
                if (!join.includes(joinedTables.CORE_LANG_LANGUAGE)) {
                    join.push(joinedTables.CORE_LANG_LANGUAGE);
                    from.push(`LEFT JOIN ${TablesList.CORE_LANG_LANGUAGE} AS ${TablesListAliases.CORE_LANG_LANGUAGE}
                            ON ${TablesListAliases.LEARNING_COURSEPATH}."lang_code" = ${TablesListAliases.CORE_LANG_LANGUAGE}."lang_code"`);
                }
                select.push(`${TablesListAliases.CORE_LANG_LANGUAGE}."lang_description" AS ${this.renderStringInQuerySelect(translations[FieldsList.LP_LANGUAGE])}`);
                groupBy.push(`${TablesListAliases.CORE_LANG_LANGUAGE}."lang_description"`);
                break;
            case FieldsList.LP_ASSOCIATED_COURSES:
                if (!toggleActive) break;
                if (!join.includes(joinedTables.LEARNING_COURSEPATH_COURSES_COUNT)) {
                    join.push(joinedTables.LEARNING_COURSEPATH_COURSES_COUNT);
                    from.push(`LEFT JOIN ${TablesList.LEARNING_COURSEPATH_COURSES_COUNT} AS ${TablesListAliases.LEARNING_COURSEPATH_COURSES_COUNT}
                            ON ${TablesListAliases.LEARNING_COURSEPATH_COURSES_COUNT}."id_path" = ${TablesListAliases.LEARNING_COURSEPATH}."id_path"`);
                }
                select.push(`${TablesListAliases.LEARNING_COURSEPATH_COURSES_COUNT}."courses" AS ${this.renderStringInQuerySelect(translations[FieldsList.LP_ASSOCIATED_COURSES])}`);
                groupBy.push(`${TablesListAliases.LEARNING_COURSEPATH_COURSES_COUNT}."courses"`);
                break;
            case FieldsList.LP_MANDATORY_ASSOCIATED_COURSES:
                if (!toggleActive) break;
                if (!join.includes(joinedTables.LEARNING_COURSEPATH_COURSES_COUNT)) {
                    join.push(joinedTables.LEARNING_COURSEPATH_COURSES_COUNT);
                    from.push(`LEFT JOIN ${TablesList.LEARNING_COURSEPATH_COURSES_COUNT} AS ${TablesListAliases.LEARNING_COURSEPATH_COURSES_COUNT}
                            ON ${TablesListAliases.LEARNING_COURSEPATH_COURSES_COUNT}."id_path" = ${TablesListAliases.LEARNING_COURSEPATH}."id_path"`);
                }
                select.push(`${TablesListAliases.LEARNING_COURSEPATH_COURSES_COUNT}."coursesMandatory" AS ${this.renderStringInQuerySelect(translations[FieldsList.LP_MANDATORY_ASSOCIATED_COURSES])}`);
                groupBy.push(`${TablesListAliases.LEARNING_COURSEPATH_COURSES_COUNT}."coursesMandatory"`);
                break;
            default:
                return false;
        }
        return true;
    }

    // Methods for the Select switch statement divided by fields
    protected querySelectUserFields(field: string, queryHelper: any): boolean {

        const {select, from, join, translations, groupBy, archivedGroupBy, archivedSelect, checkPuVisibility, cte} = queryHelper;

        switch (field) {
            // User fields
            case FieldsList.USER_ID:
                select.push(`${TablesListAliases.CORE_USER}."idst" AS ${this.renderStringInQuerySelect(translations[FieldsList.USER_ID])}`);
                archivedSelect.push(`${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}."user_id" AS ${this.renderStringInQuerySelect(translations[FieldsList.USER_ID])}`);
                groupBy.push(`${TablesListAliases.CORE_USER}."idst"`);
                archivedGroupBy.push(`${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}."user_id"`);
                break;
            case FieldsList.USER_USERID:
                select.push(`SUBSTR(${TablesListAliases.CORE_USER}."userid", 2) AS ${this.renderStringInQuerySelect(translations[FieldsList.USER_USERID])}`);
                archivedSelect.push(`SUBSTR(json_extract_path_text(${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}."user_info", 'username'), 2) AS ${this.renderStringInQuerySelect(translations[FieldsList.USER_USERID])}`);
                groupBy.push(`${TablesListAliases.CORE_USER}."userid"`);
                archivedGroupBy.push(`${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}."user_info"`);
                break;
            case FieldsList.USER_FIRSTNAME:
                select.push(`${TablesListAliases.CORE_USER}."firstname" AS ${this.renderStringInQuerySelect(translations[FieldsList.USER_FIRSTNAME])}`);
                archivedSelect.push(`json_extract_path_text(${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}."user_info", 'firstname') AS ${this.renderStringInQuerySelect(translations[FieldsList.USER_FIRSTNAME])}`);
                groupBy.push(`${TablesListAliases.CORE_USER}."firstname"`);
                archivedGroupBy.push(`${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}."user_info"`);
                break;
            case FieldsList.USER_LASTNAME:
                select.push(`${TablesListAliases.CORE_USER}."lastname" AS ${this.renderStringInQuerySelect(translations[FieldsList.USER_LASTNAME])}`);
                archivedSelect.push(`json_extract_path_text(${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}."user_info", 'lastname') AS ${this.renderStringInQuerySelect(translations[FieldsList.USER_LASTNAME])}`);
                groupBy.push(`${TablesListAliases.CORE_USER}."lastname"`);
                archivedGroupBy.push(`${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}."user_info"`);
                break;
            case FieldsList.USER_FULLNAME:
                if (this.session.platform.getShowFirstNameFirst()) {
                    select.push(`CONCAT(${TablesListAliases.CORE_USER}."firstname", ' ', ${TablesListAliases.CORE_USER}."lastname") AS ${this.renderStringInQuerySelect(translations[FieldsList.USER_FULLNAME])}`);
                    archivedSelect.push(`CONCAT(json_extract_path_text(${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}."user_info", 'firstname'), ' ', json_extract_path_text("user_info", 'lastname')) AS ${this.renderStringInQuerySelect(translations[FieldsList.USER_FULLNAME])}`);
                } else {
                    select.push(`CONCAT(${TablesListAliases.CORE_USER}."lastname", ' ', ${TablesListAliases.CORE_USER}."firstname") AS ${this.renderStringInQuerySelect(translations[FieldsList.USER_FULLNAME])}`);
                    archivedSelect.push(`CONCAT(json_extract_path_text(${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}."user_info", 'lastname'), ' ', json_extract_path_text("user_info", 'firstname')) AS ${this.renderStringInQuerySelect(translations[FieldsList.USER_FULLNAME])}`);
                }
                groupBy.push(`${TablesListAliases.CORE_USER}."firstname"`);
                groupBy.push(`${TablesListAliases.CORE_USER}."lastname"`);
                archivedGroupBy.push(`${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}."user_info"`);
                break;
            case FieldsList.USER_EMAIL:
                select.push(`${TablesListAliases.CORE_USER}."email" AS ${this.renderStringInQuerySelect(translations[FieldsList.USER_EMAIL])}`);
                archivedSelect.push(`json_extract_path_text(${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}."user_info", 'email') AS ${this.renderStringInQuerySelect(translations[FieldsList.USER_EMAIL])}`);
                groupBy.push(`${TablesListAliases.CORE_USER}."email"`);
                archivedGroupBy.push(`${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}."user_info"`);
                break;
            case FieldsList.USER_EMAIL_VALIDATION_STATUS:
                select.push(`
                            CASE
                                WHEN ${TablesListAliases.CORE_USER}."email_status" = 0 THEN ${this.renderStringInQueryCase(translations[FieldTranslation.NO])}
                                ELSE ${this.renderStringInQueryCase(translations[FieldTranslation.YES])}
                            END AS ${this.renderStringInQuerySelect(translations[FieldsList.USER_EMAIL_VALIDATION_STATUS])}`);
                archivedSelect.push(`NULL AS ${this.renderStringInQuerySelect(translations[FieldsList.USER_EMAIL_VALIDATION_STATUS])}`);
                groupBy.push(`${TablesListAliases.CORE_USER}."email_status"`);
                break;
            case FieldsList.USER_LEVEL:
                this.selectUserLevel(join, from, select, archivedSelect, groupBy, translations);
                break;
            case FieldsList.USER_DEACTIVATED:
                select.push(`
                            CASE
                                WHEN ${TablesListAliases.CORE_USER}."valid" = 1 THEN ${this.renderStringInQueryCase(translations[FieldTranslation.NO])}
                                ELSE ${this.renderStringInQueryCase(translations[FieldTranslation.YES])}
                            END AS ${this.renderStringInQuerySelect(translations[FieldsList.USER_DEACTIVATED])}`);
                archivedSelect.push(`'' AS ${this.renderStringInQuerySelect(translations[FieldsList.USER_DEACTIVATED])}`);
                groupBy.push(`${TablesListAliases.CORE_USER}."valid"`);
                break;
            case FieldsList.USER_EXPIRATION:
                select.push(`${TablesListAliases.CORE_USER}."expiration" AS ${this.renderStringInQuerySelect(translations[FieldsList.USER_EXPIRATION])}`);
                archivedSelect.push(`NULL AS ${this.renderStringInQuerySelect(translations[FieldsList.USER_EXPIRATION])}`);
                groupBy.push(`${TablesListAliases.CORE_USER}."expiration"`);
                break;
            case FieldsList.USER_SUSPEND_DATE:
                select.push(`${this.queryConvertTimezone(`${TablesListAliases.CORE_USER}."suspend_date"`)} AS ${this.renderStringInQuerySelect(translations[FieldsList.USER_SUSPEND_DATE])}`);
                archivedSelect.push(`NULL AS ${this.renderStringInQuerySelect(translations[FieldsList.USER_SUSPEND_DATE])}`);
                groupBy.push(`${TablesListAliases.CORE_USER}."suspend_date"`);
                break;
            case FieldsList.USER_REGISTER_DATE:
                select.push(`${this.queryConvertTimezone(`${TablesListAliases.CORE_USER}."register_date"`)} AS ${this.renderStringInQuerySelect(translations[FieldsList.USER_REGISTER_DATE])}`);
                archivedSelect.push(`NULL AS ${this.renderStringInQuerySelect(translations[FieldsList.USER_REGISTER_DATE])}`);
                groupBy.push(`${TablesListAliases.CORE_USER}."register_date"`);
                break;
            case FieldsList.USER_LAST_ACCESS_DATE:
                select.push(`${this.queryConvertTimezone(`${TablesListAliases.CORE_USER}."lastenter"`)} AS ${this.renderStringInQuerySelect(translations[FieldsList.USER_LAST_ACCESS_DATE])}`);
                archivedSelect.push(`NULL AS ${this.renderStringInQuerySelect(translations[FieldsList.USER_LAST_ACCESS_DATE])}`);
                groupBy.push(`${TablesListAliases.CORE_USER}."lastenter"`);
                break;
            case FieldsList.USER_BRANCH_NAME:
                if (!join.includes(joinedTables.CORE_USER_BRANCHES_REFACTORED)) {
                    join.push(joinedTables.CORE_USER_BRANCHES_REFACTORED);
                    cte.push.apply(cte, this.getCoreUserBranchesWiths(checkPuVisibility));
                    from.push(`LEFT JOIN ${joinedTables.CORE_USER_BRANCHES_REFACTORED} AS ${TablesListAliases.CORE_USER_BRANCHES} ON ${TablesListAliases.CORE_USER_BRANCHES}."idst" = ${TablesListAliases.CORE_USER}."idst"`);
                }
                select.push(`${TablesListAliases.CORE_USER_BRANCHES}."branches_names" AS ${this.renderStringInQuerySelect(translations[FieldsList.USER_BRANCH_NAME])}`);
                archivedSelect.push(`NULL AS ${this.renderStringInQuerySelect(translations[FieldsList.USER_BRANCH_NAME])}`);
                groupBy.push(`${TablesListAliases.CORE_USER_BRANCHES}."branches_names"`);
                break;
            case FieldsList.USER_BRANCH_PATH:
                if (!join.includes(joinedTables.CORE_USER_BRANCHES_REFACTORED)) {
                    join.push(joinedTables.CORE_USER_BRANCHES_REFACTORED);
                    cte.push.apply(cte, this.getCoreUserBranchesWiths(checkPuVisibility));
                    from.push(`LEFT JOIN ${joinedTables.CORE_USER_BRANCHES_REFACTORED} AS ${TablesListAliases.CORE_USER_BRANCHES} ON ${TablesListAliases.CORE_USER_BRANCHES}."idst" = ${TablesListAliases.CORE_USER}."idst"`);
                }
                select.push(`${TablesListAliases.CORE_USER_BRANCHES}."branches" AS ${this.renderStringInQuerySelect(translations[FieldsList.USER_BRANCH_PATH])}`);
                archivedSelect.push(`NULL AS ${this.renderStringInQuerySelect(translations[FieldsList.USER_BRANCH_PATH])}`);
                groupBy.push(`${TablesListAliases.CORE_USER_BRANCHES}."branches"`);
                break;
            case FieldsList.USER_BRANCHES_CODES:
                if (!join.includes(joinedTables.CORE_USER_BRANCHES_REFACTORED)) {
                    join.push(joinedTables.CORE_USER_BRANCHES_REFACTORED);
                    cte.push.apply(cte, this.getCoreUserBranchesWiths(checkPuVisibility));
                    from.push(`LEFT JOIN ${joinedTables.CORE_USER_BRANCHES_REFACTORED} AS ${TablesListAliases.CORE_USER_BRANCHES} ON ${TablesListAliases.CORE_USER_BRANCHES}."idst" = ${TablesListAliases.CORE_USER}."idst"`);
                }
                select.push(`${TablesListAliases.CORE_USER_BRANCHES}."codes" AS ${this.renderStringInQuerySelect(translations[FieldsList.USER_BRANCHES_CODES])}`);
                archivedSelect.push(`'' AS ${this.renderStringInQuerySelect(translations[FieldsList.USER_BRANCHES_CODES])}`);
                groupBy.push(`${TablesListAliases.CORE_USER_BRANCHES}."codes"`);
                break;
            case FieldsList.USER_DIRECT_MANAGER:
                if (!join.includes(joinedTables.SKILL_MANAGERS)) {
                    join.push(joinedTables.SKILL_MANAGERS);
                    from.push(`LEFT JOIN ${TablesList.SKILL_MANAGERS} AS ${TablesListAliases.SKILL_MANAGERS} ON ${TablesListAliases.SKILL_MANAGERS}."idemployee" = ${TablesListAliases.CORE_USER}."idst" AND ${TablesListAliases.SKILL_MANAGERS}."type" = 1`);
                    from.push(`LEFT JOIN ${TablesList.CORE_USER} AS ${TablesListAliases.CORE_USER}s ON ${TablesListAliases.CORE_USER}s."idst" = ${TablesListAliases.SKILL_MANAGERS}."idmanager"`);
                }
                let directManagerFullName = '';
                if (this.session.platform.getShowFirstNameFirst()) {
                    directManagerFullName = `CONCAT(${TablesListAliases.CORE_USER}s."firstname", ' ', ${TablesListAliases.CORE_USER}s."lastname")`;
                } else {
                    directManagerFullName = `CONCAT(${TablesListAliases.CORE_USER}s."lastname", ' ', ${TablesListAliases.CORE_USER}s."firstname")`;
                }
                select.push(`IFF(${directManagerFullName} = ' ', SUBSTR(${TablesListAliases.CORE_USER}s."userid", 2), ${directManagerFullName}) AS ${this.renderStringInQuerySelect(translations[FieldsList.USER_DIRECT_MANAGER])}`);
                archivedSelect.push(`NULL AS ${this.renderStringInQuerySelect(translations[FieldsList.USER_DIRECT_MANAGER])}`);
                groupBy.push(`${TablesListAliases.CORE_USER}s."userid"`);
                groupBy.push(`${TablesListAliases.CORE_USER}s."firstname"`);
                groupBy.push(`${TablesListAliases.CORE_USER}s."lastname"`);
                break;
            case FieldsList.USER_TIMEZONE:
                if (!join.includes(joinedTables.CORE_SETTING_USER_TIMEZONE)) {
                    join.push(joinedTables.CORE_SETTING_USER_TIMEZONE);
                    from.push(`LEFT JOIN ${TablesList.CORE_SETTING_USER} AS ${TablesListAliases.CORE_SETTING_USER} ON ${TablesListAliases.CORE_SETTING_USER}."id_user" = ${TablesListAliases.CORE_USER}."idst" AND ${TablesListAliases.CORE_SETTING_USER}."path_name" = 'timezone'`);
                }
                from.push(`LEFT JOIN ${TablesList.CORE_SETTING} AS ${TablesListAliases.CORE_SETTING} ON ${TablesListAliases.CORE_SETTING}."param_name" = 'timezone_default'`);
                from.push(`LEFT JOIN ${TablesList.CORE_SETTING} AS ${TablesListAliases.CORE_SETTING}_allow_override ON ${TablesListAliases.CORE_SETTING}_allow_override."param_name" = 'timezone_allow_user_override'`);
                select.push(`IFF(${TablesListAliases.CORE_SETTING}_allow_override."param_value" = 'on', IFF(${TablesListAliases.CORE_SETTING_USER}."value" IS NOT NULL, ${TablesListAliases.CORE_SETTING_USER}."value", ${TablesListAliases.CORE_SETTING}."param_value"), ${TablesListAliases.CORE_SETTING}."param_value") AS ${this.renderStringInQuerySelect(translations[FieldsList.USER_TIMEZONE])}`);
                break;
            case FieldsList.USER_LANGUAGE:
                if (!join.includes(joinedTables.CORE_SETTING_USER_LANGUAGE)) {
                    join.push(joinedTables.CORE_SETTING_USER_LANGUAGE);
                    from.push(`LEFT JOIN ${TablesList.CORE_SETTING_USER} AS ${TablesListAliases.CORE_SETTING_USER}e ON ${TablesListAliases.CORE_SETTING_USER}e."id_user" = ${TablesListAliases.CORE_USER}."idst" AND ${TablesListAliases.CORE_SETTING_USER}e."path_name" = 'ui.language'`);
                    from.push(`LEFT JOIN ${TablesList.CORE_LANG_LANGUAGE} AS ${TablesListAliases.CORE_LANG_LANGUAGE} ON ${TablesListAliases.CORE_SETTING_USER}e."value" = ${TablesListAliases.CORE_LANG_LANGUAGE}."lang_code"`);

                }
                select.push(`${TablesListAliases.CORE_LANG_LANGUAGE}."lang_description" AS ${this.renderStringInQuerySelect(translations[FieldsList.USER_LANGUAGE])}`);
                break;
            case FieldsList.USER_AUTH_APP_PAIRED:
                if (!join.includes(joinedTables.CORE_USER_2FA_SECRETS)) {
                    join.push(joinedTables.CORE_USER_2FA_SECRETS);
                    from.push(`LEFT JOIN ${TablesList.CORE_USER_2FA_SECRETS} AS ${TablesListAliases.CORE_USER_2FA_SECRETS} ON ${TablesListAliases.CORE_USER_2FA_SECRETS}."user_id" = ${TablesListAliases.CORE_USER}."idst"`);
                }
                select.push(`
                            CASE
                                WHEN ${TablesListAliases.CORE_USER_2FA_SECRETS}."user_id" IS NOT NULL THEN ${this.renderStringInQueryCase(translations[FieldTranslation.YES])} ELSE NULL
                            END AS ${this.renderStringInQuerySelect(translations[FieldsList.USER_AUTH_APP_PAIRED])}`);
                break;
            case FieldsList.USER_MANAGER_PERMISSIONS:
                select.push(`
                            CASE
                                WHEN ${TablesListAliases.CORE_USER}."can_manage_subordinates" = 1 THEN ${this.renderStringInQueryCase(translations[FieldTranslation.YES])}
                                ELSE ${this.renderStringInQueryCase(translations[FieldTranslation.NO])}
                            END AS ${this.renderStringInQuerySelect(translations[FieldsList.USER_MANAGER_PERMISSIONS])}`);
                break;
            default:
                return false;
        }
        return true;
    }

    private selectUserLevel(join: string[], from: string[], select: string[], archivedSelect: string[], groupBy: string[], translations: any): void {
        if (this.info.type === ReportsTypes.USERS_CLASSROOM_SESSIONS || this.info.type === ReportsTypes.MANAGER_USERS_CLASSROOM_SESSIONS) {

            if (!join.includes(joinedTables.CORE_GROUP_MEMBERS)) {
                join.push(joinedTables.CORE_GROUP_MEMBERS);
                from.push(`JOIN ${TablesList.CORE_GROUP_MEMBERS} AS ${TablesListAliases.CORE_GROUP_MEMBERS} ON ${TablesListAliases.CORE_GROUP_MEMBERS}."idstmember" = ${TablesListAliases.CORE_USER}."idst"`);
            }
            if (!join.includes(joinedTables.CORE_GROUP_LEVEL)) {
                join.push(joinedTables.CORE_GROUP_LEVEL);
                from.push(`JOIN ${TablesList.CORE_GROUP} AS ${TablesListAliases.CORE_GROUP_MEMBERS}l
                    ON ${TablesListAliases.CORE_GROUP_MEMBERS}l."idst" = ${TablesListAliases.CORE_GROUP_MEMBERS}."idst" AND ${TablesListAliases.CORE_GROUP_MEMBERS}l."groupid" LIKE '/framework/level/%'`);
            }
            select.push(`
                CASE
                    WHEN ${TablesListAliases.CORE_GROUP_MEMBERS}l."groupid" = ${this.renderStringInQueryCase(UserLevelsGroups.GodAdmin)}
                        THEN ${this.renderStringInQueryCase(translations[FieldTranslation.USER_LEVEL_GODADMIN])}
                    WHEN ${TablesListAliases.CORE_GROUP_MEMBERS}l."groupid" = ${this.renderStringInQueryCase(UserLevelsGroups.PowerUser)}
                        THEN ${this.renderStringInQueryCase(translations[FieldTranslation.USER_LEVEL_POWERUSER])}
                    ELSE ${this.renderStringInQueryCase(translations[FieldTranslation.USER_LEVEL_USER])}
                END AS ${this.renderStringInQuerySelect(translations[FieldsList.USER_LEVEL])}`);
            archivedSelect.push(`'' AS ${this.renderStringInQuerySelect(translations[FieldsList.USER_LEVEL])}`);
            groupBy.push(`${TablesListAliases.CORE_GROUP_MEMBERS}l."groupid"`);
        } else {
            if (!join.includes(joinedTables.CORE_USER_LEVELS)) {
                join.push(joinedTables.CORE_USER_LEVELS);
                from.push(`LEFT JOIN ${TablesList.CORE_USER_LEVELS} AS ${TablesListAliases.CORE_USER_LEVELS} ON ${TablesListAliases.CORE_USER_LEVELS}."iduser" = ${TablesListAliases.CORE_USER}."idst"`);
            }
            select.push(`
                    CASE
                        WHEN ${TablesListAliases.CORE_USER_LEVELS}."level" = ${this.renderStringInQueryCase(UserLevelsGroups.GodAdmin)} THEN ${this.renderStringInQueryCase(translations[FieldTranslation.USER_LEVEL_GODADMIN])}
                        WHEN ${TablesListAliases.CORE_USER_LEVELS}."level" = ${this.renderStringInQueryCase(UserLevelsGroups.PowerUser)} THEN ${this.renderStringInQueryCase(translations[FieldTranslation.USER_LEVEL_POWERUSER])}
                        ELSE ${this.renderStringInQueryCase(translations[FieldTranslation.USER_LEVEL_USER])}
                    END AS ${this.renderStringInQuerySelect(translations[FieldsList.USER_LEVEL])}`);
            archivedSelect.push(`NULL AS ${this.renderStringInQuerySelect(translations[FieldsList.USER_LEVEL])}`);
            groupBy.push(`${TablesListAliases.CORE_USER_LEVELS}."level"`);
        }
    }

    protected querySelectAssetFields(field: string, queryHelper: any): boolean {
        const {select, from, join, translations, groupBy} = queryHelper;

        switch (field) {
            case FieldsList.UPLOADED_ASSETS:
                select.push(`SUM(CASE WHEN ${TablesListAliases.APP7020_CONTENT}."conversion_status" >= 3 THEN 1 ELSE 0 END) AS ${this.renderStringInQuerySelect(translations[FieldsList.UPLOADED_ASSETS])}`);
                break;
            case FieldsList.PUBLISHED_ASSETS:
                select.push(`SUM(CASE WHEN ${TablesListAliases.APP7020_CONTENT}."conversion_status" = 20 THEN 1 ELSE 0 END) AS ${this.renderStringInQuerySelect(translations[FieldsList.PUBLISHED_ASSETS])}`);
                break;
            case FieldsList.UNPUBLISHED_ASSETS:
                select.push(`SUM(CASE WHEN ${TablesListAliases.APP7020_CONTENT}."conversion_status" = 18 THEN 1 ELSE 0 END) AS ${this.renderStringInQuerySelect(translations[FieldsList.UNPUBLISHED_ASSETS])}`);
                break;
            case FieldsList.PRIVATE_ASSETS:
                select.push(`SUM(CASE WHEN ${TablesListAliases.APP7020_CONTENT}."is_private" > 0 THEN 1 ELSE 0 END) AS ${this.renderStringInQuerySelect(translations[FieldsList.PRIVATE_ASSETS])}`);
                break;
            case FieldsList.INVOLVED_CHANNELS:
                if (!join.includes(joinedTables.APP7020_INVOLVED_CHANNELS_AGGREGATE)) {
                    join.push(joinedTables.APP7020_INVOLVED_CHANNELS_AGGREGATE);
                    from.push(`LEFT JOIN ${TablesList.APP7020_INVOLVED_CHANNELS_AGGREGATE} AS ${TablesListAliases.APP7020_INVOLVED_CHANNELS_AGGREGATE} ON ${TablesListAliases.APP7020_INVOLVED_CHANNELS_AGGREGATE}."idst" = ${TablesListAliases.CORE_USER}."idst" AND ${TablesListAliases.APP7020_INVOLVED_CHANNELS_AGGREGATE}."lang" = '${this.session.user.getLangCode()}'`);
                }
                select.push(`${TablesListAliases.APP7020_INVOLVED_CHANNELS_AGGREGATE}."involvedchannels" AS ${this.renderStringInQuerySelect(translations[FieldsList.INVOLVED_CHANNELS])}`);
                groupBy.push(`${TablesListAliases.APP7020_INVOLVED_CHANNELS_AGGREGATE}."involvedchannels"`);
                break;
            case FieldsList.ASSET_NAME:
                select.push(`${TablesListAliases.APP7020_CONTENT}."title" AS ${this.renderStringInQuerySelect(translations[FieldsList.ASSET_NAME])}`);
                groupBy.push(`${TablesListAliases.APP7020_CONTENT}."title"`);
                break;
            case FieldsList.PUBLISHED_ON:
                this.selectPublishedOn(join, from, select, groupBy, translations);
                break;
            case FieldsList.CHANNELS:
                if (!join.includes(joinedTables.APP7020_CHANNEL_ASSETS) && this.info.type === ReportsTypes.VIEWER_ASSET_DETAILS) {
                    join.push(joinedTables.APP7020_CHANNEL_ASSETS);
                    from.push(`LEFT JOIN ${TablesList.APP7020_CHANNEL_ASSETS} AS ${TablesListAliases.APP7020_CHANNEL_ASSETS} ON ${TablesListAliases.APP7020_CHANNEL_ASSETS}."idasset" = ${TablesListAliases.APP7020_CONTENT}."id" AND ${TablesListAliases.APP7020_CHANNEL_ASSETS}."asset_type" = 1`);
                }
                if (!join.includes(joinedTables.APP7020_CHANNEL_TRANSLATION)) {
                    join.push(joinedTables.APP7020_CHANNEL_TRANSLATION);
                    const subQueryForLanguage = `
                                SELECT
                                    cha."idchannel" as "idchannel",
                                    MAX(
                                        CASE
                                            WHEN ${TablesListAliases.APP7020_CHANNEL_TRANSLATION}."name" IS NOT NULL THEN ${TablesListAliases.APP7020_CHANNEL_TRANSLATION}."name"
                                            WHEN ${TablesListAliases.APP7020_CHANNEL_TRANSLATION}DefaultLang."name" IS NOT NULL THEN ${TablesListAliases.APP7020_CHANNEL_TRANSLATION}DefaultLang."name"
                                            ELSE ${TablesListAliases.APP7020_CHANNEL_TRANSLATION}Fallback."name"
                                        END
                                        ) as "name"
                                    FROM ${TablesList.APP7020_CHANNEL_ASSETS} AS ${TablesListAliases.APP7020_CHANNEL_ASSETS}
                                    LEFT JOIN ${TablesList.APP7020_CHANNEL_TRANSLATION} AS ${TablesListAliases.APP7020_CHANNEL_TRANSLATION}
                                        ON ${TablesListAliases.APP7020_CHANNEL_TRANSLATION}."idchannel" = ${TablesListAliases.APP7020_CHANNEL_ASSETS}."idchannel"
                                        AND ${TablesListAliases.APP7020_CHANNEL_TRANSLATION}."lang" = '${this.session.user.getLangCode()}'

                                    LEFT JOIN ${TablesList.APP7020_CHANNEL_TRANSLATION} AS ${TablesListAliases.APP7020_CHANNEL_TRANSLATION}DefaultLang
                                        ON ${TablesListAliases.APP7020_CHANNEL_TRANSLATION}DefaultLang."idchannel" = ${TablesListAliases.APP7020_CHANNEL_ASSETS}."idchannel"
                                        AND ${TablesListAliases.APP7020_CHANNEL_TRANSLATION}DefaultLang."lang" = '${this.session.platform.getDefaultLanguageCode()}'

                                    LEFT JOIN ${TablesList.APP7020_CHANNEL_TRANSLATION} AS ${TablesListAliases.APP7020_CHANNEL_TRANSLATION}Fallback
                                        ON ${TablesListAliases.APP7020_CHANNEL_TRANSLATION}Fallback."idchannel" = ${TablesListAliases.APP7020_CHANNEL_ASSETS}."idchannel"
                                        AND ${TablesListAliases.APP7020_CHANNEL_TRANSLATION}Fallback."lang" IS NOT NULL
                                GROUP BY ${TablesListAliases.APP7020_CHANNEL_ASSETS}."idchannel"
                            `;

                    from.push(`LEFT JOIN (${subQueryForLanguage}) AS ${TablesListAliases.APP7020_CHANNEL_TRANSLATION}
                                ON ${TablesListAliases.APP7020_CHANNEL_TRANSLATION}."idchannel" = ${TablesListAliases.APP7020_CHANNEL_ASSETS}."idchannel"`);
                }
                select.push(`ARRAY_TO_STRING(ARRAY_AGG(DISTINCT(${TablesListAliases.APP7020_CHANNEL_TRANSLATION}."name")) WITHIN GROUP (ORDER BY ${TablesListAliases.APP7020_CHANNEL_TRANSLATION}."name" ASC), ', ') AS ${this.renderStringInQuerySelect(translations[FieldsList.CHANNELS])}`);
                break;
            case FieldsList.PUBLISHED_BY:
                this.selectPublishedBy(join, from, select, groupBy, translations);
                break;
            case FieldsList.LAST_EDIT_BY:
                if (!join.includes(joinedTables.APP7020_CONTENT_PUBLISHED_AGGREGATE_EDIT)) {
                    join.push(joinedTables.APP7020_CONTENT_PUBLISHED_AGGREGATE_EDIT);
                    from.push(`LEFT JOIN ${TablesList.APP7020_CONTENT_PUBLISHED_AGGREGATE} AS ${TablesListAliases.APP7020_CONTENT_PUBLISHED_AGGREGATE_EDIT} ON ${TablesListAliases.APP7020_CONTENT_PUBLISHED_AGGREGATE_EDIT}."idcontent" = ${TablesListAliases.APP7020_CONTENT_PUBLISHED_AGGREGATE_MAX}."idcontent" AND ${TablesListAliases.APP7020_CONTENT_PUBLISHED_AGGREGATE_EDIT}."lasteditdate" = ${TablesListAliases.APP7020_CONTENT_PUBLISHED_AGGREGATE_MAX}."lasteditdate"`);
                }
                if (!join.includes(joinedTables.CORE_USER_MODIFY)) {
                    join.push(joinedTables.CORE_USER_MODIFY);
                    from.push(`LEFT JOIN ${TablesList.CORE_USER} AS ${TablesListAliases.CORE_USER}_modify ON ${TablesListAliases.CORE_USER}_modify."idst" = ${TablesListAliases.APP7020_CONTENT_PUBLISHED_AGGREGATE_EDIT}."lasteditby"`);
                }

                select.push(`MAX(SUBSTR(${TablesListAliases.CORE_USER}_modify."userid", 2)) AS ${this.renderStringInQuerySelect(translations[FieldsList.LAST_EDIT_BY])}`);
                break;
            case FieldsList.ASSET_TYPE:
                select.push(`
                            CASE
                                WHEN ${TablesListAliases.APP7020_CONTENT}."contenttype" = 1 THEN ${this.renderStringInQueryCase(translations[FieldTranslation.VIDEO])}
                                WHEN ${TablesListAliases.APP7020_CONTENT}."contenttype" = 2 THEN ${this.renderStringInQueryCase(translations[FieldTranslation.DOC])}
                                WHEN ${TablesListAliases.APP7020_CONTENT}."contenttype" = 3 THEN ${this.renderStringInQueryCase(translations[FieldTranslation.EXCEL])}
                                WHEN ${TablesListAliases.APP7020_CONTENT}."contenttype" = 4 THEN ${this.renderStringInQueryCase(translations[FieldTranslation.PPT])}
                                WHEN ${TablesListAliases.APP7020_CONTENT}."contenttype" = 5 THEN ${this.renderStringInQueryCase(translations[FieldTranslation.PDF])}
                                WHEN ${TablesListAliases.APP7020_CONTENT}."contenttype" = 6 THEN ${this.renderStringInQueryCase(translations[FieldTranslation.TEXT])}
                                WHEN ${TablesListAliases.APP7020_CONTENT}."contenttype" = 7 THEN ${this.renderStringInQueryCase(translations[FieldTranslation.IMAGE])}
                                WHEN ${TablesListAliases.APP7020_CONTENT}."contenttype" = 8 THEN ${this.renderStringInQueryCase(translations[FieldTranslation.QUESTION])}
                                WHEN ${TablesListAliases.APP7020_CONTENT}."contenttype" = 9 THEN ${this.renderStringInQueryCase(translations[FieldTranslation.RESPONSE])}
                                WHEN ${TablesListAliases.APP7020_CONTENT}."contenttype" = 10 THEN ${this.renderStringInQueryCase(translations[FieldTranslation.OTHER])}
                                WHEN ${TablesListAliases.APP7020_CONTENT}."contenttype" = 11 THEN ${this.renderStringInQueryCase(translations[FieldTranslation.DEFAULT_OTHER])}
                                WHEN ${TablesListAliases.APP7020_CONTENT}."contenttype" = 12 THEN ${this.renderStringInQueryCase(translations[FieldTranslation.DEFAULT_MUSIC])}
                                WHEN ${TablesListAliases.APP7020_CONTENT}."contenttype" = 13 THEN ${this.renderStringInQueryCase(translations[FieldTranslation.DEFAULT_ARCHIVE])}
                                WHEN ${TablesListAliases.APP7020_CONTENT}."contenttype" = 15 THEN ${this.renderStringInQueryCase(translations[FieldTranslation.LINKS])}
                                WHEN ${TablesListAliases.APP7020_CONTENT}."contenttype" = 16 THEN ${this.renderStringInQueryCase(translations[FieldTranslation.GOOGLE_DRIVE_DOCS])}
                                WHEN ${TablesListAliases.APP7020_CONTENT}."contenttype" = 17 THEN ${this.renderStringInQueryCase(translations[FieldTranslation.GOOGLE_DRIVE_SHEETS])}
                                WHEN ${TablesListAliases.APP7020_CONTENT}."contenttype" = 18 THEN ${this.renderStringInQueryCase(translations[FieldTranslation.GOOGLE_DRIVE_SLIDES])}
                                WHEN ${TablesListAliases.APP7020_CONTENT}."contenttype" = 19 THEN ${this.renderStringInQueryCase(translations[FieldTranslation.PLAYLIST])}
                                WHEN ${TablesListAliases.APP7020_CONTENT}."contenttype" = 20 THEN ${this.renderStringInQueryCase(translations[FieldTranslation.YOUTUBE])}
                                WHEN ${TablesListAliases.APP7020_CONTENT}."contenttype" = 21 THEN ${this.renderStringInQueryCase(translations[FieldTranslation.VIMEO])}
                                WHEN ${TablesListAliases.APP7020_CONTENT}."contenttype" = 22 THEN ${this.renderStringInQueryCase(translations[FieldTranslation.WISTIA])}
                                ELSE CAST (${TablesListAliases.APP7020_CONTENT}."contenttype" as varchar)
                            END AS ${this.renderStringInQuerySelect(translations[FieldsList.ASSET_TYPE])}`);
                groupBy.push(`${TablesListAliases.APP7020_CONTENT}."contenttype"`);
                break;
            case FieldsList.ASSET_AVERAGE_REVIEW:
                if (!join.includes(joinedTables.APP7020_CONTENT_RATING)) {
                    join.push(joinedTables.APP7020_CONTENT_RATING);
                    from.push(`LEFT JOIN ${TablesList.APP7020_CONTENT_RATING} AS ${TablesListAliases.APP7020_CONTENT_RATING} ON ${TablesListAliases.APP7020_CONTENT_RATING}."idcontent" = ${TablesListAliases.APP7020_CONTENT}."id"`);
                }
                select.push(`AVG(${TablesListAliases.APP7020_CONTENT_RATING}."rating") AS ${this.renderStringInQuerySelect(translations[FieldsList.ASSET_AVERAGE_REVIEW])}`);
                break;
            case FieldsList.ASSET_DESCRIPTION:
                select.push(`${TablesListAliases.APP7020_CONTENT}."description" AS ${this.renderStringInQuerySelect(translations[FieldsList.ASSET_DESCRIPTION])}`);
                groupBy.push(`${TablesListAliases.APP7020_CONTENT}."description"`);
                break;
            case FieldsList.ASSET_TAG:
                if (!join.includes(joinedTables.APP7020_TAG_LINK)) {
                    join.push(joinedTables.APP7020_TAG_LINK);
                    from.push(`LEFT JOIN ${TablesList.APP7020_TAG_LINK} AS ${TablesListAliases.APP7020_TAG_LINK} ON ${TablesListAliases.APP7020_TAG_LINK}."idcontent" = ${TablesListAliases.APP7020_CONTENT}."id"`);
                }
                if (!join.includes(joinedTables.APP7020_TAG)) {
                    join.push(joinedTables.APP7020_TAG);
                    from.push(`LEFT JOIN ${TablesList.APP7020_TAG} AS ${TablesListAliases.APP7020_TAG} ON ${TablesListAliases.APP7020_TAG}."id" = ${TablesListAliases.APP7020_TAG_LINK}."idtag"`);
                }
                select.push(`ARRAY_TO_STRING(ARRAY_AGG(DISTINCT(${TablesListAliases.APP7020_TAG}."tagtext")) WITHIN GROUP (ORDER BY ${TablesListAliases.APP7020_TAG}."tagtext" ASC), ', ') AS ${this.renderStringInQuerySelect(translations[FieldsList.ASSET_TAG])}`);
                break;
            case FieldsList.ASSET_SKILL:
                if (!join.includes(joinedTables.SKILL_SKILLS_OBJECTS)) {
                    join.push(joinedTables.SKILL_SKILLS_OBJECTS);
                    from.push(`LEFT JOIN ${TablesList.SKILL_SKILLS_OBJECTS} AS ${TablesListAliases.SKILL_SKILLS_OBJECTS} ON ${TablesListAliases.SKILL_SKILLS_OBJECTS}."idobject" = ${TablesListAliases.APP7020_CONTENT}."id" AND ${TablesListAliases.SKILL_SKILLS_OBJECTS}."objecttype" = 3`);
                }
                if (!join.includes(joinedTables.SKILL_SKILLS)) {
                    join.push(joinedTables.SKILL_SKILLS);
                    from.push(`LEFT JOIN ${TablesList.SKILL_SKILLS} AS ${TablesListAliases.SKILL_SKILLS} ON ${TablesListAliases.SKILL_SKILLS}."id" = ${TablesListAliases.SKILL_SKILLS_OBJECTS}."idskill"`);
                }
                select.push(`ARRAY_TO_STRING(ARRAY_AGG(DISTINCT(${TablesListAliases.SKILL_SKILLS}."title")) WITHIN GROUP (ORDER BY ${TablesListAliases.SKILL_SKILLS}."title" ASC), ', ') AS ${this.renderStringInQuerySelect(translations[FieldsList.ASSET_SKILL])}`);
                break;
            case FieldsList.ASSET_LAST_ACCESS:
                if (!join.includes(joinedTables.APP7020_CONTENT_HISTORY)) {
                    join.push(joinedTables.APP7020_CONTENT_HISTORY);
                    from.push(`LEFT JOIN ${TablesList.APP7020_CONTENT_HISTORY} AS ${TablesListAliases.APP7020_CONTENT_HISTORY} ON ${TablesListAliases.APP7020_CONTENT_HISTORY}."idcontent" = ${TablesListAliases.APP7020_CONTENT}."id" AND ${TablesListAliases.APP7020_CONTENT_HISTORY}."iduser" = ${TablesListAliases.APP7020_CONTENT}."useridview"`);
                }
                select.push(`${this.queryConvertTimezone(`MAX(${TablesListAliases.APP7020_CONTENT_HISTORY}."viewed")`)} AS ${this.renderStringInQuerySelect(translations[FieldsList.ASSET_LAST_ACCESS])}`);
                break;
            case FieldsList.ASSET_FIRST_ACCESS:
                if (!join.includes(joinedTables.APP7020_CONTENT_HISTORY)) {
                    join.push(joinedTables.APP7020_CONTENT_HISTORY);
                    from.push(`LEFT JOIN ${TablesList.APP7020_CONTENT_HISTORY} AS ${TablesListAliases.APP7020_CONTENT_HISTORY} ON ${TablesListAliases.APP7020_CONTENT_HISTORY}."idcontent" = ${TablesListAliases.APP7020_CONTENT}."id" AND ${TablesListAliases.APP7020_CONTENT_HISTORY}."iduser" = ${TablesListAliases.APP7020_CONTENT}."useridview"`);
                }
                select.push(`${this.queryConvertTimezone(`MIN(${TablesListAliases.APP7020_CONTENT_HISTORY}."viewed")`)} AS ${this.renderStringInQuerySelect(translations[FieldsList.ASSET_FIRST_ACCESS])}`);
                break;
            case FieldsList.ASSET_NUMBER_ACCESS:
                if (!join.includes(joinedTables.APP7020_CONTENT_HISTORY)) {
                    join.push(joinedTables.APP7020_CONTENT_HISTORY);
                    from.push(`LEFT JOIN ${TablesList.APP7020_CONTENT_HISTORY} AS ${TablesListAliases.APP7020_CONTENT_HISTORY} ON ${TablesListAliases.APP7020_CONTENT_HISTORY}."idcontent" = ${TablesListAliases.APP7020_CONTENT}."id" AND ${TablesListAliases.APP7020_CONTENT_HISTORY}."iduser" = ${TablesListAliases.APP7020_CONTENT}."useridview"`);
                }
                select.push(`COUNT(DISTINCT(${TablesListAliases.APP7020_CONTENT_HISTORY}."id")) AS ${this.renderStringInQuerySelect(translations[FieldsList.ASSET_NUMBER_ACCESS])}`);
                break;
            default:
                return false;
        }
        return true;
    }


    private selectPublishedBy(join: string[], from: string[], select: string[], groupBy: string[], translations: any): void {
        if (this.info.type === ReportsTypes.VIEWER_ASSET_DETAILS) {
            if (!join.includes(joinedTables.APP7020_CONTENT_PUBLISHED_AGGREGATE_PUBLISH)) {
                join.push(joinedTables.APP7020_CONTENT_PUBLISHED_AGGREGATE_PUBLISH);
                from.push(`LEFT JOIN ${TablesList.APP7020_CONTENT_PUBLISHED_AGGREGATE} AS ${TablesListAliases.APP7020_CONTENT_PUBLISHED_AGGREGATE_PUBLISH} ON ${TablesListAliases.APP7020_CONTENT_PUBLISHED_AGGREGATE_PUBLISH}."idcontent" = ${TablesListAliases.APP7020_CONTENT_PUBLISHED_AGGREGATE_MAX}."idcontent" AND ${TablesListAliases.APP7020_CONTENT_PUBLISHED_AGGREGATE_PUBLISH}."lastpublishdate" = ${TablesListAliases.APP7020_CONTENT_PUBLISHED_AGGREGATE_MAX}."lastpublishdate"`);
            }
            if (!join.includes(joinedTables.CORE_USER_PUBLISH)) {
                join.push(joinedTables.CORE_USER_PUBLISH);
                from.push(`LEFT JOIN ${TablesList.CORE_USER} AS ${TablesListAliases.CORE_USER}_publish ON ${TablesListAliases.CORE_USER}_publish."idst" = ${TablesListAliases.APP7020_CONTENT_PUBLISHED_AGGREGATE_PUBLISH}."lastpublishedby"`);
            }

            select.push(`CASE
                        WHEN ${TablesListAliases.APP7020_CONTENT}."conversion_status" = 20 THEN
                            MAX(SUBSTR(${TablesListAliases.CORE_USER}_publish."userid", 2))
                        ELSE '' END AS ${this.renderStringInQuerySelect(translations[FieldsList.PUBLISHED_BY])}`);
            groupBy.push(`${TablesListAliases.APP7020_CONTENT}."conversion_status"`);
        } else {
            // Report Assets - Statistics
            if (!join.includes(joinedTables.CORE_USER)) {
                join.push(joinedTables.CORE_USER);
                from.push(`LEFT JOIN ${TablesList.CORE_USER} AS ${TablesListAliases.CORE_USER} ON ${TablesListAliases.CORE_USER}."idst" = ${TablesListAliases.APP7020_CONTENT}."userid"`);
            }
            if (this.session.platform.getShowFirstNameFirst()) {
                select.push(`CONCAT(${TablesListAliases.CORE_USER}."firstname", ' ', ${TablesListAliases.CORE_USER}."lastname") AS ${this.renderStringInQuerySelect(translations[FieldsList.PUBLISHED_BY])}`);
            } else {
                select.push(`CONCAT(${TablesListAliases.CORE_USER}."lastname", ' ', ${TablesListAliases.CORE_USER}."firstname") AS ${this.renderStringInQuerySelect(translations[FieldsList.PUBLISHED_BY])}`);
            }
            groupBy.push(`${TablesListAliases.CORE_USER}."firstname"`, `${TablesListAliases.CORE_USER}."lastname"`);
        }
    }

    private selectPublishedOn(join: string[], from: string[], select: string[], groupBy: string[], translations: any): void {
        if (this.info.type === ReportsTypes.VIEWER_ASSET_DETAILS) {
            select.push(`
                CASE
                    WHEN ${TablesListAliases.APP7020_CONTENT}."conversion_status" = 20 THEN
                        MAX(${this.queryConvertTimezone(`${TablesListAliases.APP7020_CONTENT_PUBLISHED_AGGREGATE_MAX}."lastpublishdate"`)})
                    ELSE '' END AS ${this.renderStringInQuerySelect(translations[FieldsList.PUBLISHED_ON])}`);
            groupBy.push(`${TablesListAliases.APP7020_CONTENT}."conversion_status"`);
        } else {
            // Report Assets - Statistics
            select.push(`${this.queryConvertTimezone(`${TablesListAliases.APP7020_CONTENT_PUBLISHED}."datepublished"`)} AS ${this.renderStringInQuerySelect(translations[FieldsList.PUBLISHED_ON])}`);
            groupBy.push(`${TablesListAliases.APP7020_CONTENT_PUBLISHED}."datepublished"`);
        }
    }

    protected querySelectGroupFields(field: string, queryModel: any): boolean {

        const {select, groupBy, translations} = queryModel;

        switch (field) {
            case FieldsList.GROUP_GROUP_OR_BRANCH_NAME:
                groupBy.push(`${TablesListAliases.CORE_ORG_CHART_TREE}."idorg"`);
                groupBy.push(`${TablesListAliases.CORE_ORG_CHART_TREE}."code"`);
                groupBy.push(`${TablesListAliases.CORE_ORG_CHART}."translation"`);
                groupBy.push(`${TablesListAliases.CORE_GROUP}."groupid"`);
                select.push(`
                            CASE
                                WHEN ${TablesListAliases.CORE_ORG_CHART_TREE}."idorg" IS NOT NULL THEN
                                    CONCAT(
                                        CASE
                                            WHEN ${TablesListAliases.CORE_ORG_CHART_TREE}."code" <> '' THEN
                                                CONCAT('(',${TablesListAliases.CORE_ORG_CHART_TREE}."code", ') ')
                                            ELSE ''
                                        END,
                                            ${TablesListAliases.CORE_ORG_CHART}."translation"
                                        )
                                ELSE
                                SUBSTR(${TablesListAliases.CORE_GROUP}."groupid", 2)
                            END AS ${this.renderStringInQuerySelect(translations[FieldsList.GROUP_GROUP_OR_BRANCH_NAME])}`);
                break;
            case FieldsList.GROUP_MEMBERS_COUNT:
                groupBy.push(`${TablesListAliases.CORE_GROUP_MEMBERS}Count."idstmembercount"`);
                select.push(`${TablesListAliases.CORE_GROUP_MEMBERS}Count."idstmembercount" AS ${this.renderStringInQuerySelect(translations[FieldsList.GROUP_MEMBERS_COUNT])}`);
                break;
            default:
                return false;
        }
        return true;
    }

    protected querySelectBadgeFields(field: string, queryHelper: any): boolean {

        const {select, translations} = queryHelper;

        switch (field) {
            case FieldsList.BADGE_DESCRIPTION:
                select.push(`
                            CASE
                                WHEN ${TablesListAliases.GAMIFICATION_BADGE_TRANSLATION}."description" IS NOT NULL THEN ${TablesListAliases.GAMIFICATION_BADGE_TRANSLATION}."description"
                                ELSE ${TablesListAliases.GAMIFICATION_BADGE_TRANSLATION}d."description"
                            END AS ${this.renderStringInQuerySelect(translations[FieldsList.BADGE_DESCRIPTION])}`);
                break;
            case FieldsList.BADGE_NAME:
                select.push(`
                            CASE
                                WHEN ${TablesListAliases.GAMIFICATION_BADGE_TRANSLATION}."name" IS NOT NULL THEN ${TablesListAliases.GAMIFICATION_BADGE_TRANSLATION}."name"
                                ELSE ${TablesListAliases.GAMIFICATION_BADGE_TRANSLATION}d."name"
                            END AS ${this.renderStringInQuerySelect(translations[FieldsList.BADGE_NAME])}`);
                break;
            case FieldsList.BADGE_SCORE:
                select.push(`${TablesListAliases.GAMIFICATION_ASSIGNED_BADGES}."score" AS ${this.renderStringInQuerySelect(translations[FieldsList.BADGE_SCORE])}`);
                break;
            case FieldsList.BADGE_ISSUED_ON:
                select.push(`${this.queryConvertTimezone(`${TablesListAliases.GAMIFICATION_ASSIGNED_BADGES}."issued_on"`)} AS ${this.renderStringInQuerySelect(translations[FieldsList.BADGE_ISSUED_ON])}`);
                break;
            default:
                return false;
        }
        return true;
    }

    protected querySelectCourseFields(field: string, queryHelper: any): boolean {

        const { select, from, archivedFrom, join, groupBy, archivedGroupBy, translations, archivedSelect } = queryHelper;
        const isToggleUsersLearningPlansReportEnhancement = this.info.type === ReportsTypes.USERS_LP && this.session.platform.isToggleUsersLearningPlansReportEnhancement();
        if(isToggleUsersLearningPlansReportEnhancement){
            this.joinLpCoursesTables(join, from);
        }

        switch (field) {
            case FieldsList.COURSE_ID:
                select.push(`${TablesListAliases.LEARNING_COURSE}."idcourse" AS ${this.renderStringInQuerySelect(translations[FieldsList.COURSE_ID])}`);
                archivedSelect.push(`CAST(json_extract_path_text(${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}."course_info", 'id') AS INTEGER) AS ${this.renderStringInQuerySelect(translations[FieldsList.COURSE_ID])}`);
                groupBy.push(`${TablesListAliases.LEARNING_COURSE}."idcourse"`);
                archivedGroupBy.push(`${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}."course_info"`);
                break;
            case FieldsList.COURSE_UNIQUE_ID:
                select.push(`${TablesListAliases.LEARNING_COURSE}."uidcourse" AS ${this.renderStringInQuerySelect(translations[FieldsList.COURSE_UNIQUE_ID])}`);
                archivedSelect.push(`json_extract_path_text(${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}."course_info", 'uid') AS ${this.renderStringInQuerySelect(translations[FieldsList.COURSE_UNIQUE_ID])}`);
                groupBy.push(`${TablesListAliases.LEARNING_COURSE}."uidcourse"`);
                archivedGroupBy.push(`${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}."course_info"`);
                break;
            case FieldsList.COURSE_CODE:
                select.push(`${TablesListAliases.LEARNING_COURSE}."code" AS ${this.renderStringInQuerySelect(translations[FieldsList.COURSE_CODE])}`);
                archivedSelect.push(`json_extract_path_text(${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}."course_info", 'code') AS ${this.renderStringInQuerySelect(translations[FieldsList.COURSE_CODE])}`);
                groupBy.push(`${TablesListAliases.LEARNING_COURSE}."code"`);
                archivedGroupBy.push(`${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}."course_info"`);
                break;
            case FieldsList.COURSE_NAME:
                select.push(`${TablesListAliases.LEARNING_COURSE}."name" AS ${this.renderStringInQuerySelect(translations[FieldsList.COURSE_NAME])}`);
                archivedSelect.push(`json_extract_path_text(${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}."course_info", 'name') AS ${this.renderStringInQuerySelect(translations[FieldsList.COURSE_NAME])}`);
                groupBy.push(`${TablesListAliases.LEARNING_COURSE}."name"`);
                archivedGroupBy.push(`${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}."course_info"`);
                break;
            case FieldsList.COURSE_CATEGORY_CODE:
                if (!join.includes(joinedTables.COURSE_CATEGORIES)) {
                    join.push(joinedTables.COURSE_CATEGORIES);
                    from.push(`LEFT JOIN ${TablesList.LEARNING_CATEGORY} AS ${TablesListAliases.LEARNING_CATEGORY} ON ${TablesListAliases.LEARNING_CATEGORY}."idcategory" = ${TablesListAliases.LEARNING_COURSE}."idcategory" AND ${TablesListAliases.LEARNING_CATEGORY}."lang_code" = '${this.session.user.getLang()}'`);
                }
                select.push(`${TablesListAliases.LEARNING_CATEGORY}."code" AS ${this.renderStringInQuerySelect(translations[FieldsList.COURSE_CATEGORY_CODE])}`);
                archivedSelect.push(`'' AS ${this.renderStringInQuerySelect(translations[FieldsList.COURSE_CATEGORY_CODE])}`);
                groupBy.push(`${TablesListAliases.LEARNING_CATEGORY}."code"`);
                break;
            case FieldsList.COURSE_CATEGORY_NAME:
                if (!join.includes(joinedTables.COURSE_CATEGORIES)) {
                    join.push(joinedTables.COURSE_CATEGORIES);
                    from.push(`LEFT JOIN ${TablesList.LEARNING_CATEGORY} AS ${TablesListAliases.LEARNING_CATEGORY} ON ${TablesListAliases.LEARNING_CATEGORY}."idcategory" = ${TablesListAliases.LEARNING_COURSE}."idcategory" AND ${TablesListAliases.LEARNING_CATEGORY}."lang_code" = '${this.session.user.getLang()}'`);
                }
                select.push(`${TablesListAliases.LEARNING_CATEGORY}."translation" AS ${this.renderStringInQuerySelect(translations[FieldsList.COURSE_CATEGORY_NAME])}`);
                archivedSelect.push(`'' AS ${this.renderStringInQuerySelect(translations[FieldsList.COURSE_CATEGORY_NAME])}`);
                groupBy.push(`${TablesListAliases.LEARNING_CATEGORY}."translation"`);
                break;
            case FieldsList.COURSE_STATUS:
                const courseStatus = (field: string) => `CASE
                                        WHEN ${field} = 0 THEN ${this.renderStringInQueryCase(translations[FieldTranslation.COURSE_STATUS_PREPARATION])}
                                        ELSE ${this.renderStringInQueryCase(translations[FieldTranslation.COURSE_STATUS_EFFECTIVE])}
                                    END AS ${this.renderStringInQuerySelect(translations[FieldsList.COURSE_STATUS])}`;
                select.push(courseStatus(TablesListAliases.LEARNING_COURSE + `."status"`));
                archivedSelect.push(courseStatus(`CAST(json_extract_path_text(${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}."course_info", 'status') AS int)`));
                groupBy.push(`${TablesListAliases.LEARNING_COURSE}."status"`);
                archivedGroupBy.push(`${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}."course_info"`);
                break;
            case FieldsList.COURSE_CREDITS:
                select.push(`${TablesListAliases.LEARNING_COURSE}."credits" AS ${this.renderStringInQuerySelect(translations[FieldsList.COURSE_CREDITS])}`);
                archivedSelect.push(`CAST(json_extract_path_text(${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}."course_info", 'credits') AS INTEGER) AS ${this.renderStringInQuerySelect(translations[FieldsList.COURSE_CREDITS])}`);
                groupBy.push(`${TablesListAliases.LEARNING_COURSE}."credits"`);
                archivedGroupBy.push(`${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}."course_info"`);
                break;
            case FieldsList.COURSE_DURATION:
                select.push(`${TablesListAliases.LEARNING_COURSE}."mediumtime" AS ${this.renderStringInQuerySelect(translations[FieldsList.COURSE_DURATION])}`);
                archivedSelect.push(`CAST(json_extract_path_text(${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}."course_info", '$.duration') as INTEGER) AS ${this.renderStringInQuerySelect(translations[FieldsList.COURSE_DURATION])}`);
                groupBy.push(`${TablesListAliases.LEARNING_COURSE}."mediumtime"`);
                archivedGroupBy.push(`${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}."course_info"`);
                break;
            case FieldsList.COURSE_TYPE:
                const courseType = (field: string) => `
                                    CASE
                                        WHEN ${field} = ${this.renderStringInQueryCase(CourseTypes.Elearning)} THEN ${this.renderStringInQueryCase(translations[FieldTranslation.COURSE_TYPE_ELEARNING])}
                                        WHEN ${field} = ${this.renderStringInQueryCase(CourseTypes.Classroom)} THEN ${this.renderStringInQueryCase(translations[FieldTranslation.COURSE_TYPE_CLASSROOM])}
                                        ELSE ${this.renderStringInQueryCase(translations[FieldTranslation.COURSE_TYPE_WEBINAR])}
                                    END AS ${this.renderStringInQuerySelect(translations[FieldsList.COURSE_TYPE])}`;
                select.push(courseType(TablesListAliases.LEARNING_COURSE + `."course_type"`));
                archivedSelect.push(courseType(`json_extract_path_text(${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}."course_info", 'type')`));
                groupBy.push(`${TablesListAliases.LEARNING_COURSE}."course_type"`);
                archivedGroupBy.push(`${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}."course_info"`);
                break;
            case FieldsList.COURSE_DATE_BEGIN:
                select.push(`${TablesListAliases.LEARNING_COURSE}."date_begin" AS ${this.renderStringInQuerySelect(translations[FieldsList.COURSE_DATE_BEGIN])}`);
                archivedSelect.push(`to_timestamp(json_extract_path_text(${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}."course_info", 'start_at'), 'YYYY-MM-DD') AS ${this.renderStringInQuerySelect(translations[FieldsList.COURSE_DATE_BEGIN])}`);
                groupBy.push(`${TablesListAliases.LEARNING_COURSE}."date_begin"`);
                archivedGroupBy.push(`${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}."course_info"`);
                break;
            case FieldsList.COURSE_DATE_END:
                select.push(`${TablesListAliases.LEARNING_COURSE}."date_end" AS ${this.renderStringInQuerySelect(translations[FieldsList.COURSE_DATE_END])}`);
                archivedSelect.push(`to_timestamp(json_extract_path_text(${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}."course_info", 'end_at'), 'YYYY-MM-DD') AS ${this.renderStringInQuerySelect(translations[FieldsList.COURSE_DATE_END])}`);
                groupBy.push(`${TablesListAliases.LEARNING_COURSE}."date_end"`);
                archivedGroupBy.push(`${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}."course_info"`);
                break;
            case FieldsList.COURSE_EXPIRED:
                const courseExpired = (field: string) => `
                                    CASE
                                        WHEN ${field} < CURRENT_DATE THEN ${this.renderStringInQueryCase(translations[FieldTranslation.YES])}
                                        ELSE ${this.renderStringInQueryCase(translations[FieldTranslation.NO])}
                                    END AS ${this.renderStringInQuerySelect(translations[FieldsList.COURSE_EXPIRED])}`;
                select.push(courseExpired(`${TablesListAliases.LEARNING_COURSE}."date_end"`));
                archivedSelect.push(courseExpired(`to_timestamp(json_extract_path_text(${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}."course_info", 'end_at'), 'YYYY-MM-DD')`));
                groupBy.push(`${TablesListAliases.LEARNING_COURSE}."date_end"`);
                archivedGroupBy.push(`${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}."course_info"`);
                break;
            case FieldsList.COURSE_CREATION_DATE:
                select.push(`${this.queryConvertTimezone(`${TablesListAliases.LEARNING_COURSE}."create_date"`)} AS ${this.renderStringInQuerySelect(translations[FieldsList.COURSE_CREATION_DATE])}`);
                archivedSelect.push(`${this.queryConvertTimezone(`to_timestamp_ntz(json_extract_path_text(${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}."course_info", 'created_at'))`)} AS ${this.renderStringInQuerySelect(translations[FieldsList.COURSE_CREATION_DATE])}`);
                groupBy.push(`${TablesListAliases.LEARNING_COURSE}."create_date"`);
                archivedGroupBy.push(`${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}."course_info"`);
                break;
            case FieldsList.COURSE_E_SIGNATURE:
                if (this.session.platform.checkPluginESignatureEnabled()) {
                    select.push(`CASE
                                    WHEN ${TablesListAliases.LEARNING_COURSE}."has_esignature_enabled" = 1 THEN ${this.renderStringInQueryCase(translations[FieldTranslation.YES])}
                                    ELSE ${this.renderStringInQueryCase(translations[FieldTranslation.NO])}
                                END AS ${this.renderStringInQuerySelect(translations[FieldsList.COURSE_E_SIGNATURE])}`);
                    archivedSelect.push(`'' AS ${this.renderStringInQuerySelect(translations[FieldsList.COURSE_E_SIGNATURE])}`);
                    groupBy.push(`${TablesListAliases.LEARNING_COURSE}."has_esignature_enabled"`);
                }
                break;
            case FieldsList.COURSE_LANGUAGE:
            case FieldsList.LP_COURSE_LANGUAGE:
                let courseLanguageKey = isToggleUsersLearningPlansReportEnhancement ? FieldsList.LP_COURSE_LANGUAGE : FieldsList.COURSE_LANGUAGE;
                if (!join.includes(joinedTables.CORE_LANG_LANGUAGE)) {
                    join.push(joinedTables.CORE_LANG_LANGUAGE);
                    from.push(`LEFT JOIN ${TablesList.CORE_LANG_LANGUAGE} AS ${TablesListAliases.CORE_LANG_LANGUAGE} ON ${TablesListAliases.LEARNING_COURSE}."lang_code" = ${TablesListAliases.CORE_LANG_LANGUAGE}."lang_code"`);
                }
                select.push(`${TablesListAliases.CORE_LANG_LANGUAGE}."lang_description" AS ${this.renderStringInQuerySelect(translations[courseLanguageKey])}`);
                archivedSelect.push(`json_extract_path_text(${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}."course_info", 'language') AS ${this.renderStringInQuerySelect(translations[courseLanguageKey])}`);
                groupBy.push(`${TablesListAliases.CORE_LANG_LANGUAGE}."lang_description"`);
                archivedGroupBy.push(`${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}."course_info"`);
                break;
            case FieldsList.COURSE_SKILLS:
                if (!join.includes(joinedTables.SKILL_SKILLS_OBJECTS)) {
                    join.push(joinedTables.SKILL_SKILLS_OBJECTS);
                    from.push(`LEFT JOIN ${TablesList.SKILL_SKILLS_OBJECTS} AS ${TablesListAliases.SKILL_SKILLS_OBJECTS} ON ${TablesListAliases.SKILL_SKILLS_OBJECTS}."idobject" = ${TablesListAliases.LEARNING_COURSE}."idcourse" AND ${TablesListAliases.SKILL_SKILLS_OBJECTS}."objecttype" = 1`);
                    archivedFrom.push(`LEFT JOIN ${TablesList.SKILL_SKILLS_OBJECTS} AS ${TablesListAliases.SKILL_SKILLS_OBJECTS} ON ${TablesListAliases.SKILL_SKILLS_OBJECTS}."idobject" = ${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}."course_id" AND ${TablesListAliases.SKILL_SKILLS_OBJECTS}."objecttype" = 1`);
                }
                if (!join.includes(joinedTables.SKILL_SKILLS)) {
                    join.push(joinedTables.SKILL_SKILLS);
                    from.push(`LEFT JOIN ${TablesList.SKILL_SKILLS} AS ${TablesListAliases.SKILL_SKILLS} ON ${TablesListAliases.SKILL_SKILLS}."id" = ${TablesListAliases.SKILL_SKILLS_OBJECTS}."idskill"`);
                    archivedFrom.push(`LEFT JOIN ${TablesList.SKILL_SKILLS} AS ${TablesListAliases.SKILL_SKILLS} ON ${TablesListAliases.SKILL_SKILLS}."id" = ${TablesListAliases.SKILL_SKILLS_OBJECTS}."idskill"`);
                }
                select.push(`ARRAY_TO_STRING(ARRAY_AGG(DISTINCT(${TablesListAliases.SKILL_SKILLS}."title")) WITHIN GROUP (ORDER BY ${TablesListAliases.SKILL_SKILLS}."title" ASC), ', ') AS ${this.renderStringInQuerySelect(translations[FieldsList.COURSE_SKILLS])}`);
                archivedSelect.push(`ARRAY_TO_STRING(ARRAY_AGG(DISTINCT(${TablesListAliases.SKILL_SKILLS}."title")) WITHIN GROUP (ORDER BY ${TablesListAliases.SKILL_SKILLS}."title" ASC), ', ') AS ${this.renderStringInQuerySelect(translations[FieldsList.COURSE_SKILLS])}`);
                break;
            default:
                return false;
        }
        return true;
    }

    protected querySelectSessionFields(field: string, queryHelper: any): boolean {

        const { select, from, join, groupBy, archivedGroupBy, translations, archivedSelect } = queryHelper;
        const sessionTable = this.info.type === ReportsTypes.USERS_CLASSROOM_SESSIONS || this.info.type === ReportsTypes.MANAGER_USERS_CLASSROOM_SESSIONS ?
            TablesListAliases.LT_COURSEUSER_SESSION_DETAILS : TablesListAliases.LT_COURSE_SESSION;
        switch (field) {
            case FieldsList.SESSION_NAME:
                select.push(`${sessionTable}."name" AS ${this.renderStringInQuerySelect(translations[FieldsList.SESSION_NAME])}`);
                archivedSelect.push(`json_extract_path_text(${TablesListAliases.ARCHIVED_ENROLLMENT_SESSION}."session_info", '$.name') AS ${this.renderStringInQuerySelect(translations[FieldsList.SESSION_NAME])}`);
                groupBy.push(`${sessionTable}."name"`);
                archivedGroupBy.push(`${TablesListAliases.ARCHIVED_ENROLLMENT_SESSION}."session_info"`);
                break;
            case FieldsList.SESSION_CODE:
                select.push(`${sessionTable}."session_code" AS ${this.renderStringInQuerySelect(translations[FieldsList.SESSION_CODE])}`);
                archivedSelect.push(`json_extract_path_text(${TablesListAliases.ARCHIVED_ENROLLMENT_SESSION}."session_info", '$.code') AS ${this.renderStringInQuerySelect(translations[FieldsList.SESSION_CODE])}`);
                groupBy.push(`${sessionTable}."session_code"`);
                archivedGroupBy.push(`${TablesListAliases.ARCHIVED_ENROLLMENT_SESSION}."session_info"`);
                break;
            case FieldsList.SESSION_START_DATE:
                select.push(`${this.queryConvertTimezone(`${sessionTable}."date_begin"`)}
                        AS ${this.renderStringInQuerySelect(translations[FieldsList.SESSION_START_DATE])}`);
                archivedSelect.push(`${this.queryConvertTimezone(`to_timestamp_ntz(json_extract_path_text(${TablesListAliases.ARCHIVED_ENROLLMENT_SESSION}."session_info", '$.start_at'))`)}
                    AS ${this.renderStringInQuerySelect(translations[FieldsList.SESSION_START_DATE])}`);
                groupBy.push(`${sessionTable}."date_begin"`);
                archivedGroupBy.push(`${TablesListAliases.ARCHIVED_ENROLLMENT_SESSION}."session_info"`);
                break;
            case FieldsList.SESSION_END_DATE:
                select.push(`${this.queryConvertTimezone(`${sessionTable}."date_end"`)}
                        AS ${this.renderStringInQuerySelect(translations[FieldsList.SESSION_END_DATE])}`);
                archivedSelect.push(`${this.queryConvertTimezone(`to_timestamp_ntz(json_extract_path_text(${TablesListAliases.ARCHIVED_ENROLLMENT_SESSION}."session_info", '$.end_at'))`)}
                    AS ${this.renderStringInQuerySelect(translations[FieldsList.SESSION_END_DATE])}`);
                groupBy.push(`${sessionTable}."date_end"`);
                archivedGroupBy.push(`${TablesListAliases.ARCHIVED_ENROLLMENT_SESSION}."session_info"`);
                break;
            case FieldsList.SESSION_EVALUATION_SCORE_BASE:
                select.push(`CONCAT(CAST(${sessionTable}."evaluation_score" AS VARCHAR), '/', CAST(${sessionTable}."score_base" AS VARCHAR))
                        AS ${this.renderStringInQuerySelect(translations[FieldsList.SESSION_EVALUATION_SCORE_BASE])}`);
                archivedSelect.push(`'' AS ${this.renderStringInQuerySelect(translations[FieldsList.SESSION_EVALUATION_SCORE_BASE])}`);
                groupBy.push(`${sessionTable}."evaluation_score"`);
                groupBy.push(`${sessionTable}."score_base"`);
                break;
            case FieldsList.SESSION_TIME_SESSION:
                select.push(`
                    CASE
                     WHEN CAST(((${sessionTable}."total_hours" / 60) * 3600) % 60 AS INTEGER) = 0
                      THEN
                       CONCAT(CAST(CAST(${sessionTable}."total_hours" AS INTEGER) AS VARCHAR), 'h')
                      ELSE
                         CONCAT(
                           CAST(CAST(FLOOR(${sessionTable}."total_hours") AS INTEGER) AS VARCHAR),
                           'h ',
                           CAST(CAST(((${sessionTable}."total_hours" / 60) * 3600) % 60 AS INTEGER) AS VARCHAR),
                           'm'
                         )
                     END
                    AS ${this.renderStringInQuerySelect(translations[FieldsList.SESSION_TIME_SESSION])}`);
                archivedSelect.push(`'' AS ${this.renderStringInQuerySelect(translations[FieldsList.SESSION_TIME_SESSION])}`);
                groupBy.push(`${sessionTable}."total_hours"`);
                break;
            case FieldsList.SESSION_INTERNAL_ID:
                select.push(`${TablesListAliases.LT_COURSE_SESSION}."id_session" AS ${this.renderStringInQuerySelect(translations[FieldsList.SESSION_INTERNAL_ID])}`);
                break;
            case FieldsList.SESSION_UNIQUE_ID:
                select.push(`${sessionTable}."uid_session" AS ${this.renderStringInQuerySelect(translations[FieldsList.SESSION_UNIQUE_ID])}`);
                archivedSelect.push(`json_extract_path_text(${TablesListAliases.ARCHIVED_ENROLLMENT_SESSION}."session_info", '$.uid') AS ${this.renderStringInQuerySelect(translations[FieldsList.SESSION_UNIQUE_ID])}`);
                groupBy.push(`${sessionTable}."uid_session"`);
                archivedGroupBy.push(`${TablesListAliases.ARCHIVED_ENROLLMENT_SESSION}."session_info"`);
                break;
            case FieldsList.WEBINAR_SESSION_WEBINAR_TOOL:
                if (!join.includes(joinedTables.LT_COURSE_SESSION_DATE)) {
                    join.push(joinedTables.LT_COURSE_SESSION_DATE);
                    from.push(`LEFT JOIN ${TablesList.LT_COURSE_SESSION_DATE} AS ${TablesListAliases.LT_COURSE_SESSION_DATE}
                            ON ${TablesListAliases.LT_COURSE_SESSION_DATE}."id_session" = ${sessionTable}."id_session"`);
                }
                if (!join.includes(joinedTables.LT_COURSE_SESSION_DATE_WEBINAR_SETTING)) {
                    join.push(joinedTables.LT_COURSE_SESSION_DATE_WEBINAR_SETTING);
                    from.push(`LEFT JOIN ${TablesList.LT_COURSE_SESSION_DATE_WEBINAR_SETTING} AS ${TablesListAliases.LT_COURSE_SESSION_DATE_WEBINAR_SETTING}
                            ON ${TablesListAliases.LT_COURSE_SESSION_DATE_WEBINAR_SETTING}."id_date" = ${TablesListAliases.LT_COURSE_SESSION_DATE}."id_date"`);
                }
                select.push(`${TablesListAliases.LT_COURSE_SESSION_DATE_WEBINAR_SETTING}."webinar_tool" AS ${this.renderStringInQuerySelect(translations[FieldsList.WEBINAR_SESSION_WEBINAR_TOOL])}`);
                archivedSelect.push(`'' AS ${this.renderStringInQuerySelect(translations[FieldsList.WEBINAR_SESSION_WEBINAR_TOOL])}`);
                groupBy.push(`${TablesListAliases.LT_COURSE_SESSION_DATE_WEBINAR_SETTING}."webinar_tool"`);
                break;
            case FieldsList.WEBINAR_SESSION_TOOL_TIME_IN_SESSION:
                if (!join.includes(joinedTables.LT_COURSE_SESSION_DATE_ATTENDANCE_AGGREGATE)) {
                    join.push(joinedTables.LT_COURSE_SESSION_DATE_ATTENDANCE_AGGREGATE);
                    from.push(`LEFT JOIN ${TablesList.LT_COURSE_SESSION_DATE_ATTENDANCE_AGGREGATE} AS ${TablesListAliases.LT_COURSE_SESSION_DATE_ATTENDANCE_AGGREGATE}
                            ON ${TablesListAliases.LT_COURSE_SESSION_DATE_ATTENDANCE_AGGREGATE}."id_session" = ${sessionTable}."id_session"
                                AND ${TablesListAliases.LT_COURSE_SESSION_DATE_ATTENDANCE_AGGREGATE}."id_user" = ${sessionTable}."id_user"
                                AND ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."level" <> 6`);
                }
                select.push(`${TablesListAliases.LT_COURSE_SESSION_DATE_ATTENDANCE_AGGREGATE}."webinar_tool_session_time" AS ${this.renderStringInQuerySelect(translations[FieldsList.WEBINAR_SESSION_TOOL_TIME_IN_SESSION])}`);
                archivedSelect.push(`NULL AS ${this.renderStringInQuerySelect(translations[FieldsList.WEBINAR_SESSION_TOOL_TIME_IN_SESSION])}`);
                groupBy.push(`${TablesListAliases.LT_COURSE_SESSION_DATE_ATTENDANCE_AGGREGATE}."webinar_tool_session_time"`);
                break;
            case FieldsList.SESSION_INSTRUCTOR_USERIDS:
                if (!join.includes(joinedTables.LT_COURSE_SESSION_INSTRUCTOR_AGGREGATE)) {
                    join.push(joinedTables.LT_COURSE_SESSION_INSTRUCTOR_AGGREGATE);
                    from.push(`LEFT JOIN ${TablesList.LT_COURSE_SESSION_INSTRUCTOR_AGGREGATE} AS ${TablesListAliases.LT_COURSE_SESSION_INSTRUCTOR_AGGREGATE}
                            ON ${TablesListAliases.LT_COURSE_SESSION_INSTRUCTOR_AGGREGATE}."id_session" = ${sessionTable}."id_session"`);
                }
                select.push(`
                        ARRAY_TO_STRING(ARRAY_SORT(ARRAY_AGG(
                            DISTINCT(
                                CASE WHEN ${TablesListAliases.LT_COURSE_SESSION_INSTRUCTOR_AGGREGATE}."id_date" IS NULL THEN
                                ${TablesListAliases.LT_COURSE_SESSION_INSTRUCTOR_AGGREGATE}."userid" END
                            )
                        )), ', ') AS ${this.renderStringInQuerySelect(translations[FieldsList.SESSION_INSTRUCTOR_USERIDS])}`);

                archivedSelect.push(`CASE
                        WHEN JSON_EXTRACT_PATH_TEXT(${TablesListAliases.ARCHIVED_ENROLLMENT_SESSION}."session_info", '$.instructors') IN ('', NULL) THEN ''
                        ELSE JSON_EXTRACT_PATH_TEXT(${TablesListAliases.ARCHIVED_ENROLLMENT_SESSION}."session_info", '$.instructors') END
                        AS ${this.renderStringInQuerySelect(translations[FieldsList.SESSION_INSTRUCTOR_USERIDS])}`);
                archivedGroupBy.push(`${TablesListAliases.ARCHIVED_ENROLLMENT_SESSION}."session_info"`);
                break;
            case FieldsList.SESSION_INSTRUCTOR_LIST:
                let sessionInstructorListField = '';
                if (!join.includes(joinedTables.LT_COURSE_SESSION_INSTRUCTOR_AGGREGATE)) {
                    join.push(joinedTables.LT_COURSE_SESSION_INSTRUCTOR_AGGREGATE);
                    from.push(`LEFT JOIN ${TablesList.LT_COURSE_SESSION_INSTRUCTOR_AGGREGATE} AS ${TablesListAliases.LT_COURSE_SESSION_INSTRUCTOR_AGGREGATE}
                            ON ${TablesListAliases.LT_COURSE_SESSION_INSTRUCTOR_AGGREGATE}."id_session" = ${TablesListAliases.LT_COURSE_SESSION_DATE}."id_session"`);
                }
                if (this.session.platform.getShowFirstNameFirst()) {
                    sessionInstructorListField = `CONCAT(${TablesListAliases.LT_COURSE_SESSION_INSTRUCTOR_AGGREGATE}."firstname", ' ', ${TablesListAliases.LT_COURSE_SESSION_INSTRUCTOR_AGGREGATE}."lastname")`;
                } else {
                    sessionInstructorListField = `CONCAT(${TablesListAliases.LT_COURSE_SESSION_INSTRUCTOR_AGGREGATE}."lastname", ' ', ${TablesListAliases.LT_COURSE_SESSION_INSTRUCTOR_AGGREGATE}."firstname")`;
                }
                const sessionInstructorListFieldFallback = `IFF(${TablesListAliases.LT_COURSE_SESSION_INSTRUCTOR_AGGREGATE}."firstname" != '' OR ${TablesListAliases.LT_COURSE_SESSION_INSTRUCTOR_AGGREGATE}."lastname" != '', ${sessionInstructorListField}, ${TablesListAliases.LT_COURSE_SESSION_INSTRUCTOR_AGGREGATE}."userid")`;
                select.push(`REGEXP_REPLACE(
                                ARRAY_TO_STRING(ARRAY_SORT(ARRAY_AGG(
                                    DISTINCT(
                                        IFF(COALESCE(${TablesListAliases.LT_COURSE_SESSION_INSTRUCTOR_AGGREGATE}."id_date", 0) = 0, ${sessionInstructorListFieldFallback}, NULL)
                                    )
                                )), ', '),
                            '(,\\s+)+$') AS ${this.renderStringInQuerySelect(translations[FieldsList.SESSION_INSTRUCTOR_LIST])}`);
                break;
            case FieldsList.SESSION_COMPLETION_RATE:
                if (!join.includes(joinedTables.LT_COURSEUSER_SESSION)) {
                    join.push(joinedTables.LT_COURSEUSER_SESSION);
                    from.push(`LEFT JOIN ${TablesList.LT_COURSEUSER_SESSION} AS ${TablesListAliases.LT_COURSEUSER_SESSION} ON ${TablesListAliases.LT_COURSEUSER_SESSION}."id_session" = ${TablesListAliases.LT_COURSE_SESSION}."id_session" AND ${TablesListAliases.LT_COURSEUSER_SESSION}."id_user" = ${TablesListAliases.CORE_USER}."idst"`);
                }
                select.push(`IFF(COUNT(DISTINCT(${TablesListAliases.LT_COURSEUSER_SESSION}."id_user")) > 0, COUNT(DISTINCT(IFF(${TablesListAliases.LT_COURSEUSER_SESSION}."status" = 2, ${TablesListAliases.LT_COURSEUSER_SESSION}."id_user", null))) * 100 / COUNT(DISTINCT(${TablesListAliases.LT_COURSEUSER_SESSION}."id_user")) ,0) AS ${this.renderStringInQuerySelect(translations[FieldsList.SESSION_COMPLETION_RATE])}`);
                break;
            case FieldsList.SESSION_HOURS:
                select.push(`${TablesListAliases.LT_COURSE_SESSION}."total_hours" AS ${this.renderStringInQuerySelect(translations[FieldsList.SESSION_HOURS])}`);
                groupBy.push(`${sessionTable}."total_hours"`);
                break;
            case FieldsList.SESSION_INSTRUCTOR_FULLNAMES:
                if (!join.includes(joinedTables.LT_COURSE_SESSION_INSTRUCTOR_AGGREGATE)) {
                    join.push(joinedTables.LT_COURSE_SESSION_INSTRUCTOR_AGGREGATE);
                    from.push(`LEFT JOIN ${TablesList.LT_COURSE_SESSION_INSTRUCTOR_AGGREGATE} AS ${TablesListAliases.LT_COURSE_SESSION_INSTRUCTOR_AGGREGATE}
                            ON ${TablesListAliases.LT_COURSE_SESSION_INSTRUCTOR_AGGREGATE}."id_session" = ${sessionTable}."id_session"`);
                }
                const field = `CASE WHEN ${TablesListAliases.LT_COURSE_SESSION_INSTRUCTOR_AGGREGATE}."id_date" IS NULL THEN ` +
                    (this.session.platform.getShowFirstNameFirst()
                        ? `COLLATE(CONCAT(${TablesListAliases.LT_COURSE_SESSION_INSTRUCTOR_AGGREGATE}."firstname", ' ', ${TablesListAliases.LT_COURSE_SESSION_INSTRUCTOR_AGGREGATE}."lastname"), 'en-ci')`
                        : `COLLATE(CONCAT(${TablesListAliases.LT_COURSE_SESSION_INSTRUCTOR_AGGREGATE}."lastname", ' ', ${TablesListAliases.LT_COURSE_SESSION_INSTRUCTOR_AGGREGATE}."firstname"), 'en-ci')`
                    ) + ' END';
                select.push(`ARRAY_TO_STRING(ARRAY_AGG(DISTINCT(${field})) WITHIN GROUP (ORDER BY ${field}), ', ') AS ${this.renderStringInQuerySelect(translations[FieldsList.SESSION_INSTRUCTOR_FULLNAMES])}`);
                archivedSelect.push(`'' AS ${this.renderStringInQuerySelect(translations[FieldsList.SESSION_INSTRUCTOR_FULLNAMES])}`);
                break;
            case FieldsList.SESSION_ATTENDANCE_TYPE:
                const attendanceTypeTranslation = (field: string) => `
                        CASE
                            WHEN ${field} = '${AttendancesTypes.BLENDED}' THEN ${this.renderStringInQueryCase(translations[FieldTranslation.SESSION_ATTENDANCE_TYPE_BLEENDED])}
                            WHEN ${field} = '${AttendancesTypes.FLEXIBLE}' THEN ${this.renderStringInQueryCase(translations[FieldTranslation.SESSION_ATTENDANCE_TYPE_FLEXIBLE])}
                            WHEN ${field} = '${AttendancesTypes.FULLONLINE}' THEN ${this.renderStringInQueryCase(translations[FieldTranslation.SESSION_ATTENDANCE_TYPE_FULLONLINE])}
                            WHEN ${field} = '${AttendancesTypes.FULLONSITE}' THEN ${this.renderStringInQueryCase(translations[FieldTranslation.SESSION_ATTENDANCE_TYPE_FULLONSITE])}
                            ELSE ''
                        END AS ${this.renderStringInQuerySelect(translations[FieldsList.SESSION_ATTENDANCE_TYPE])}`;
                select.push(attendanceTypeTranslation(`${sessionTable}."attendance_type"`));
                archivedSelect.push(attendanceTypeTranslation(`json_extract_path_text(${TablesListAliases.ARCHIVED_ENROLLMENT_SESSION}."attendance_info", '$.type')`));
                groupBy.push(`${sessionTable}."attendance_type"`);
                archivedGroupBy.push(`${TablesListAliases.ARCHIVED_ENROLLMENT_SESSION}."attendance_info"`);
                break;
            case FieldsList.SESSION_MAXIMUM_ENROLLMENTS:
                select.push(`${sessionTable}."max_enroll" AS ${this.renderStringInQuerySelect(translations[FieldsList.SESSION_MAXIMUM_ENROLLMENTS])}`);
                archivedSelect.push(`NULL AS ${this.renderStringInQuerySelect(translations[FieldsList.SESSION_MAXIMUM_ENROLLMENTS])}`);
                groupBy.push(`${sessionTable}."max_enroll"`);
                break;
            case FieldsList.SESSION_MINIMUM_ENROLLMENTS:
                select.push(`${sessionTable}."min_enroll" AS ${this.renderStringInQuerySelect(translations[FieldsList.SESSION_MINIMUM_ENROLLMENTS])}`);
                archivedSelect.push(`NULL AS ${this.renderStringInQuerySelect(translations[FieldsList.SESSION_MINIMUM_ENROLLMENTS])}`);
                groupBy.push(`${sessionTable}."min_enroll"`);
                break;
            default:
                return false;
        }
        return true;
    }

    protected querySelectEventFields(field: string, queryHelper: any): boolean {
        const { select, from, join, groupBy, translations, archivedSelect } = queryHelper;

        switch (field) {
            case FieldsList.SESSION_EVENT_NAME:
                if (!join.includes(joinedTables.LT_COURSE_SESSION_DATE)) {
                    join.push(joinedTables.LT_COURSE_SESSION_DATE);
                    from.push(`LEFT JOIN ${TablesList.LT_COURSE_SESSION_DATE} AS ${TablesListAliases.LT_COURSE_SESSION_DATE}
                            ON ${TablesListAliases.LT_COURSE_SESSION_DATE}."id_session" = ${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}."id_session"`);
                }
                select.push(`${TablesListAliases.LT_COURSE_SESSION_DATE}."name" AS ${this.renderStringInQuerySelect(translations[FieldsList.SESSION_EVENT_NAME])}`);
                archivedSelect.push(`'' AS ${this.renderStringInQuerySelect(translations[FieldsList.SESSION_EVENT_NAME])}`);
                groupBy.push(`${TablesListAliases.LT_COURSE_SESSION_DATE}."name"`);
                groupBy.push(`${TablesListAliases.LT_COURSE_SESSION_DATE}."id_date"`);
                break;
            case FieldsList.SESSION_EVENT_DATE:
                if (!join.includes(joinedTables.LT_COURSE_SESSION_DATE)) {
                    join.push(joinedTables.LT_COURSE_SESSION_DATE);
                    from.push(`LEFT JOIN ${TablesList.LT_COURSE_SESSION_DATE} AS ${TablesListAliases.LT_COURSE_SESSION_DATE}
                            ON ${TablesListAliases.LT_COURSE_SESSION_DATE}."id_session" = ${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}."id_session"`);
                }
                select.push(`${TablesListAliases.LT_COURSE_SESSION_DATE}."day" AS ${this.renderStringInQuerySelect(translations[FieldsList.SESSION_EVENT_DATE])}`);
                archivedSelect.push(`NULL AS ${this.renderStringInQuerySelect(translations[FieldsList.SESSION_EVENT_DATE])}`);
                groupBy.push(`${TablesListAliases.LT_COURSE_SESSION_DATE}."day"`);
                groupBy.push(`${TablesListAliases.LT_COURSE_SESSION_DATE}."id_date"`);
                break;
            case FieldsList.SESSION_EVENT_DURATION:
                if (!join.includes(joinedTables.LT_COURSE_SESSION_DATE)) {
                    join.push(joinedTables.LT_COURSE_SESSION_DATE);
                    from.push(`LEFT JOIN ${TablesList.LT_COURSE_SESSION_DATE} AS ${TablesListAliases.LT_COURSE_SESSION_DATE}
                            ON ${TablesListAliases.LT_COURSE_SESSION_DATE}."id_session" = ${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}."id_session"`);
                }
                select.push(`ROUND(${TablesListAliases.LT_COURSE_SESSION_DATE}."effective_duration" / 60, 2) AS ${this.renderStringInQuerySelect(translations[FieldsList.SESSION_EVENT_DURATION])}`);
                archivedSelect.push(`NULL AS ${this.renderStringInQuerySelect(translations[FieldsList.SESSION_EVENT_DURATION])}`);
                groupBy.push(`${TablesListAliases.LT_COURSE_SESSION_DATE}."effective_duration"`);
                groupBy.push(`${TablesListAliases.LT_COURSE_SESSION_DATE}."id_date"`);
                break;
            case FieldsList.SESSION_EVENT_ID:
                if (!join.includes(joinedTables.LT_COURSE_SESSION_DATE)) {
                    join.push(joinedTables.LT_COURSE_SESSION_DATE);
                    from.push(`LEFT JOIN ${TablesList.LT_COURSE_SESSION_DATE} AS ${TablesListAliases.LT_COURSE_SESSION_DATE}
                            ON ${TablesListAliases.LT_COURSE_SESSION_DATE}."id_session" = ${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}."id_session"`);
                }
                select.push(`${TablesListAliases.LT_COURSE_SESSION_DATE}."id_date" AS ${this.renderStringInQuerySelect(translations[FieldsList.SESSION_EVENT_ID])}`);
                archivedSelect.push(`NULL AS ${this.renderStringInQuerySelect(translations[FieldsList.SESSION_EVENT_ID])}`);
                groupBy.push(`${TablesListAliases.LT_COURSE_SESSION_DATE}."id_date"`);
                break;
            case FieldsList.SESSION_EVENT_INSTRUCTOR_FULLNAME:
                if (!join.includes(joinedTables.LT_COURSE_SESSION_DATE)) {
                    join.push(joinedTables.LT_COURSE_SESSION_DATE);
                    from.push(`LEFT JOIN ${TablesList.LT_COURSE_SESSION_DATE} AS ${TablesListAliases.LT_COURSE_SESSION_DATE}
                            ON ${TablesListAliases.LT_COURSE_SESSION_DATE}."id_session" = ${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}."id_session"`);
                }
                if (!join.includes(joinedTables.LT_COURSE_SESSION_INSTRUCTOR_AGGREGATE)) {
                    join.push(joinedTables.LT_COURSE_SESSION_INSTRUCTOR_AGGREGATE);
                    from.push(`LEFT JOIN ${TablesList.LT_COURSE_SESSION_INSTRUCTOR_AGGREGATE} AS ${TablesListAliases.LT_COURSE_SESSION_INSTRUCTOR_AGGREGATE}
                            ON ${TablesListAliases.LT_COURSE_SESSION_INSTRUCTOR_AGGREGATE}."id_session" = ${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}."id_session"`);
                }
                if (this.session.platform.getShowFirstNameFirst()) {
                    select.push(`
                            ARRAY_TO_STRING(ARRAY_SORT(ARRAY_AGG(
                                DISTINCT(
                                    CASE WHEN ${TablesListAliases.LT_COURSE_SESSION_INSTRUCTOR_AGGREGATE}."id_date" = ${TablesListAliases.LT_COURSE_SESSION_DATE}."id_date" THEN
                                    CONCAT(${TablesListAliases.LT_COURSE_SESSION_INSTRUCTOR_AGGREGATE}."firstname", ' ', ${TablesListAliases.LT_COURSE_SESSION_INSTRUCTOR_AGGREGATE}."lastname") END
                                )
                            )), ', ') AS ${this.renderStringInQuerySelect(translations[FieldsList.SESSION_EVENT_INSTRUCTOR_FULLNAME])}`);
                } else {
                    select.push(`
                            ARRAY_TO_STRING(ARRAY_SORT(ARRAY_AGG(
                                DISTINCT(
                                    CASE WHEN ${TablesListAliases.LT_COURSE_SESSION_INSTRUCTOR_AGGREGATE}."id_date" = ${TablesListAliases.LT_COURSE_SESSION_DATE}."id_date" THEN
                                    CONCAT(${TablesListAliases.LT_COURSE_SESSION_INSTRUCTOR_AGGREGATE}."lastname", ' ', ${TablesListAliases.LT_COURSE_SESSION_INSTRUCTOR_AGGREGATE}."firstname") END
                                )
                            )), ', ') AS ${this.renderStringInQuerySelect(translations[FieldsList.SESSION_EVENT_INSTRUCTOR_FULLNAME])}`);
                }
                archivedSelect.push(`'' AS ${this.renderStringInQuerySelect(translations[FieldsList.SESSION_EVENT_INSTRUCTOR_FULLNAME])}`);
                groupBy.push(`${TablesListAliases.LT_COURSE_SESSION_DATE}."id_date"`);
                break;
            case FieldsList.SESSION_EVENT_INSTRUCTOR_USER_NAME:
                if (!join.includes(joinedTables.LT_COURSE_SESSION_DATE)) {
                    join.push(joinedTables.LT_COURSE_SESSION_DATE);
                    from.push(`LEFT JOIN ${TablesList.LT_COURSE_SESSION_DATE} AS ${TablesListAliases.LT_COURSE_SESSION_DATE}
                            ON ${TablesListAliases.LT_COURSE_SESSION_DATE}."id_session" = ${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}."id_session"`);
                }
                if (!join.includes(joinedTables.LT_COURSE_SESSION_INSTRUCTOR_AGGREGATE)) {
                    join.push(joinedTables.LT_COURSE_SESSION_INSTRUCTOR_AGGREGATE);
                    from.push(`LEFT JOIN ${TablesList.LT_COURSE_SESSION_INSTRUCTOR_AGGREGATE} AS ${TablesListAliases.LT_COURSE_SESSION_INSTRUCTOR_AGGREGATE}
                            ON ${TablesListAliases.LT_COURSE_SESSION_INSTRUCTOR_AGGREGATE}."id_session" = ${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}."id_session"`);
                }
                select.push(`
                            ARRAY_TO_STRING(ARRAY_SORT(ARRAY_AGG(
                                DISTINCT(
                                    CASE WHEN ${TablesListAliases.LT_COURSE_SESSION_INSTRUCTOR_AGGREGATE}."id_date" = ${TablesListAliases.LT_COURSE_SESSION_DATE}."id_date" THEN
                                    ${TablesListAliases.LT_COURSE_SESSION_INSTRUCTOR_AGGREGATE}."userid" END
                                )
                            )), ', ') AS ${this.renderStringInQuerySelect(translations[FieldsList.SESSION_EVENT_INSTRUCTOR_USER_NAME])}`);
                archivedSelect.push(`'' AS ${this.renderStringInQuerySelect(translations[FieldsList.SESSION_EVENT_INSTRUCTOR_USER_NAME])}`);
                groupBy.push(`${TablesListAliases.LT_COURSE_SESSION_DATE}."id_date"`);
                break;
            case FieldsList.SESSION_EVENT_TIMEZONE:
                if (!join.includes(joinedTables.LT_COURSE_SESSION_DATE)) {
                    join.push(joinedTables.LT_COURSE_SESSION_DATE);
                    from.push(`LEFT JOIN ${TablesList.LT_COURSE_SESSION_DATE} AS ${TablesListAliases.LT_COURSE_SESSION_DATE}
                            ON ${TablesListAliases.LT_COURSE_SESSION_DATE}."id_session" = ${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}."id_session"`);
                }
                select.push(`${TablesListAliases.LT_COURSE_SESSION_DATE}."timezone" AS ${this.renderStringInQuerySelect(translations[FieldsList.SESSION_EVENT_TIMEZONE])}`);
                archivedSelect.push(`'' AS ${this.renderStringInQuerySelect(translations[FieldsList.SESSION_EVENT_TIMEZONE])}`);
                groupBy.push(`${TablesListAliases.LT_COURSE_SESSION_DATE}."timezone"`);
                groupBy.push(`${TablesListAliases.LT_COURSE_SESSION_DATE}."id_date"`);
                break;
            case FieldsList.SESSION_EVENT_TYPE:
                if (!join.includes(joinedTables.LT_COURSE_SESSION_DATE)) {
                    join.push(joinedTables.LT_COURSE_SESSION_DATE);
                    from.push(`LEFT JOIN ${TablesList.LT_COURSE_SESSION_DATE} AS ${TablesListAliases.LT_COURSE_SESSION_DATE}
                            ON ${TablesListAliases.LT_COURSE_SESSION_DATE}."id_session" = ${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}."id_session"`);
                }
                if (!join.includes(joinedTables.LT_COURSE_SESSION_DATE_WEBINAR_SETTING)) {
                    join.push(joinedTables.LT_COURSE_SESSION_DATE_WEBINAR_SETTING);
                    from.push(`LEFT JOIN ${TablesList.LT_COURSE_SESSION_DATE_WEBINAR_SETTING} AS ${TablesListAliases.LT_COURSE_SESSION_DATE_WEBINAR_SETTING}
                            ON ${TablesListAliases.LT_COURSE_SESSION_DATE_WEBINAR_SETTING}."id_date" = ${TablesListAliases.LT_COURSE_SESSION_DATE}."id_date"`);
                }
                select.push(`
                        CASE
                            WHEN ${TablesListAliases.LT_COURSE_SESSION_DATE}."id_location" IS NOT NULL AND ${TablesListAliases.LT_COURSE_SESSION_DATE_WEBINAR_SETTING}."webinar_tool" IS NOT NULL
                                THEN ${this.renderStringInQueryCase(translations[FieldTranslation.SESSION_ATTENDANCE_TYPE_BLEENDED])}
                            WHEN ${TablesListAliases.LT_COURSE_SESSION_DATE}."id_location" IS NULL AND ${TablesListAliases.LT_COURSE_SESSION_DATE_WEBINAR_SETTING}."webinar_tool" IS NOT NULL
                                THEN ${this.renderStringInQueryCase(translations[FieldTranslation.SESSION_ATTENDANCE_TYPE_FULLONLINE])}
                            WHEN ${TablesListAliases.LT_COURSE_SESSION_DATE}."id_location" IS NOT NULL AND ${TablesListAliases.LT_COURSE_SESSION_DATE_WEBINAR_SETTING}."webinar_tool" IS NULL
                                THEN ${this.renderStringInQueryCase(translations[FieldTranslation.SESSION_ATTENDANCE_TYPE_FULLONSITE])}
                            ELSE ''
                        END AS ${this.renderStringInQuerySelect(translations[FieldsList.SESSION_EVENT_TYPE])}`);
                archivedSelect.push(`'' AS ${this.renderStringInQuerySelect(translations[FieldsList.SESSION_EVENT_TYPE])}`);
                groupBy.push(`${TablesListAliases.LT_COURSE_SESSION_DATE}."id_location"`);
                groupBy.push(`${TablesListAliases.LT_COURSE_SESSION_DATE_WEBINAR_SETTING}."webinar_tool"`);
                groupBy.push(`${TablesListAliases.LT_COURSE_SESSION_DATE}."id_date"`);
                break;
            case FieldsList.SESSION_EVENT_START_DATE:
                if (!join.includes(joinedTables.LT_COURSE_SESSION_DATE)) {
                    join.push(joinedTables.LT_COURSE_SESSION_DATE);
                    from.push(`LEFT JOIN ${TablesList.LT_COURSE_SESSION_DATE} AS ${TablesListAliases.LT_COURSE_SESSION_DATE}
                            ON ${TablesListAliases.LT_COURSE_SESSION_DATE}."id_session" = ${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}."id_session"`);
                }
                select.push(`CONCAT(${TablesListAliases.LT_COURSE_SESSION_DATE}."day", ' ', ${TablesListAliases.LT_COURSE_SESSION_DATE}."time_begin")
                        AS ${this.renderStringInQuerySelect(translations[FieldsList.SESSION_EVENT_START_DATE])}`);
                archivedSelect.push(`NULL AS ${this.renderStringInQuerySelect(translations[FieldsList.SESSION_EVENT_START_DATE])}`);
                groupBy.push(`${TablesListAliases.LT_COURSE_SESSION_DATE}."day"`);
                groupBy.push(`${TablesListAliases.LT_COURSE_SESSION_DATE}."time_begin"`);
                groupBy.push(`${TablesListAliases.LT_COURSE_SESSION_DATE}."id_date"`);
                break;
            case FieldsList.EVENT_INSTRUCTORS_LIST:
                let eventInstructorListField = '';
                if (!join.includes(joinedTables.LT_COURSE_SESSION_INSTRUCTOR_AGGREGATE)) {
                    join.push(joinedTables.LT_COURSE_SESSION_INSTRUCTOR_AGGREGATE);
                    from.push(`LEFT JOIN ${TablesList.LT_COURSE_SESSION_INSTRUCTOR_AGGREGATE} AS ${TablesListAliases.LT_COURSE_SESSION_INSTRUCTOR_AGGREGATE}
                            ON ${TablesListAliases.LT_COURSE_SESSION_INSTRUCTOR_AGGREGATE}."id_session" = ${TablesListAliases.LT_COURSE_SESSION_DATE}."id_session"`);
                }
                if (this.session.platform.getShowFirstNameFirst()) {
                    eventInstructorListField = `CONCAT(${TablesListAliases.LT_COURSE_SESSION_INSTRUCTOR_AGGREGATE}."firstname", ' ', ${TablesListAliases.LT_COURSE_SESSION_INSTRUCTOR_AGGREGATE}."lastname")`;
                } else {
                    eventInstructorListField = `CONCAT(${TablesListAliases.LT_COURSE_SESSION_INSTRUCTOR_AGGREGATE}."lastname", ' ', ${TablesListAliases.LT_COURSE_SESSION_INSTRUCTOR_AGGREGATE}."firstname")`;
                }
                const eventInstructorListFieldFallback = `IFF(${TablesListAliases.LT_COURSE_SESSION_INSTRUCTOR_AGGREGATE}."firstname" != '' OR ${TablesListAliases.LT_COURSE_SESSION_INSTRUCTOR_AGGREGATE}."lastname" != '', ${eventInstructorListField}, ${TablesListAliases.LT_COURSE_SESSION_INSTRUCTOR_AGGREGATE}."userid")`;
                select.push(`REGEXP_REPLACE(
                                ARRAY_TO_STRING(ARRAY_SORT(ARRAY_AGG(
                                    DISTINCT(
                                        IFF(${TablesListAliases.LT_COURSE_SESSION_INSTRUCTOR_AGGREGATE}."id_date" = ${TablesListAliases.LT_COURSE_SESSION_DATE}."id_date", ${eventInstructorListFieldFallback}, null)
                                    )
                                )), ', '),
                             '(,\\s+)+$') AS ${this.renderStringInQuerySelect(translations[FieldsList.EVENT_INSTRUCTORS_LIST])}`);
                break;
            case FieldsList.EVENT_ATTENDANCE_STATUS_NOT_SET:
                if (!join.includes(joinedTables.LT_COURSE_SESSION_DATE_ATTENDANCE)) {
                    join.push(joinedTables.LT_COURSE_SESSION_DATE_ATTENDANCE);
                    from.push(`LEFT JOIN ${TablesList.LT_COURSE_SESSION_DATE_ATTENDANCE} AS ${TablesListAliases.LT_COURSE_SESSION_DATE_ATTENDANCE} ON ${TablesListAliases.LT_COURSE_SESSION_DATE_ATTENDANCE}."id_session" = ${TablesListAliases.LT_COURSE_SESSION}."id_session" AND ${TablesListAliases.LT_COURSE_SESSION_DATE_ATTENDANCE}."id_date" = ${TablesListAliases.LT_COURSE_SESSION_DATE}."id_date" AND ${TablesListAliases.LT_COURSE_SESSION_DATE_ATTENDANCE}."id_user" = ${TablesListAliases.CORE_USER}."idst" AND ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."level" <> 6`);
                }
                if (!join.includes(joinedTables.LT_COURSEUSER_SESSION)) {
                    join.push(joinedTables.LT_COURSEUSER_SESSION);
                    from.push(`LEFT JOIN ${TablesList.LT_COURSEUSER_SESSION} AS ${TablesListAliases.LT_COURSEUSER_SESSION} ON ${TablesListAliases.LT_COURSEUSER_SESSION}."id_session" = ${TablesListAliases.LT_COURSE_SESSION}."id_session" AND ${TablesListAliases.LT_COURSEUSER_SESSION}."id_user" = ${TablesListAliases.CORE_USER}."idst"`);
                }
                select.push(`COUNT(DISTINCT(${TablesListAliases.LT_COURSEUSER_SESSION}."id_user")) - COUNT(DISTINCT( ${TablesListAliases.LT_COURSE_SESSION_DATE_ATTENDANCE}."id_user")) AS ${this.renderStringInQuerySelect(translations[FieldsList.EVENT_ATTENDANCE_STATUS_NOT_SET])}`);
                break;
            case FieldsList.EVENT_ATTENDANCE_STATUS_ABSENT_PERC:
                    if (!join.includes(joinedTables.LT_COURSE_SESSION_DATE_ATTENDANCE)) {
                        join.push(joinedTables.LT_COURSE_SESSION_DATE_ATTENDANCE);
                    from.push(`LEFT JOIN ${TablesList.LT_COURSE_SESSION_DATE_ATTENDANCE} AS ${TablesListAliases.LT_COURSE_SESSION_DATE_ATTENDANCE} ON ${TablesListAliases.LT_COURSE_SESSION_DATE_ATTENDANCE}."id_session" = ${TablesListAliases.LT_COURSE_SESSION}."id_session" AND ${TablesListAliases.LT_COURSE_SESSION_DATE_ATTENDANCE}."id_date" = ${TablesListAliases.LT_COURSE_SESSION_DATE}."id_date" AND ${TablesListAliases.LT_COURSE_SESSION_DATE_ATTENDANCE}."id_user" = ${TablesListAliases.CORE_USER}."idst" AND ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."level" <> 6`);
                }
                select.push(`IFF(
                            COUNT(DISTINCT(${TablesListAliases.LT_COURSE_SESSION_DATE_ATTENDANCE}."id_user")) > 0,
                            TRUNCATE(COUNT(DISTINCT(IFF(
                                    ${TablesListAliases.LT_COURSE_SESSION_DATE_ATTENDANCE}."attendance" = 0,
                                    ${TablesListAliases.LT_COURSE_SESSION_DATE_ATTENDANCE}."id_user",
                                    null
                            ))) * 100 / COUNT(DISTINCT(${TablesListAliases.LT_COURSE_SESSION_DATE_ATTENDANCE}."id_user")))
                            ,0) AS ${this.renderStringInQuerySelect(translations[FieldsList.EVENT_ATTENDANCE_STATUS_ABSENT_PERC])}`);
                break;
            case FieldsList.EVENT_ATTENDANCE_STATUS_PRESENT_PERC:
                    if (!join.includes(joinedTables.LT_COURSE_SESSION_DATE_ATTENDANCE)) {
                        join.push(joinedTables.LT_COURSE_SESSION_DATE_ATTENDANCE);
                    from.push(`LEFT JOIN ${TablesList.LT_COURSE_SESSION_DATE_ATTENDANCE} AS ${TablesListAliases.LT_COURSE_SESSION_DATE_ATTENDANCE} ON ${TablesListAliases.LT_COURSE_SESSION_DATE_ATTENDANCE}."id_session" = ${TablesListAliases.LT_COURSE_SESSION}."id_session" AND ${TablesListAliases.LT_COURSE_SESSION_DATE_ATTENDANCE}."id_date" = ${TablesListAliases.LT_COURSE_SESSION_DATE}."id_date" AND ${TablesListAliases.LT_COURSE_SESSION_DATE_ATTENDANCE}."id_user" = ${TablesListAliases.CORE_USER}."idst" AND ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."level" <> 6`);
                }
                select.push(`IFF(
                            COUNT(DISTINCT(${TablesListAliases.LT_COURSE_SESSION_DATE_ATTENDANCE}."id_user")) > 0,
                            TRUNCATE(COUNT(DISTINCT(IFF(
                                    ${TablesListAliases.LT_COURSE_SESSION_DATE_ATTENDANCE}."attendance" = 1,
                                    ${TablesListAliases.LT_COURSE_SESSION_DATE_ATTENDANCE}."id_user",
                                    null
                            ))) * 100 / COUNT(DISTINCT(${TablesListAliases.LT_COURSE_SESSION_DATE_ATTENDANCE}."id_user")))
                            ,0) AS ${this.renderStringInQuerySelect(translations[FieldsList.EVENT_ATTENDANCE_STATUS_PRESENT_PERC])}`);
                break;
            case FieldsList.EVENT_AVERAGE_SCORE:
                if (!join.includes(joinedTables.LT_COURSEUSER_SESSION)) {
                    join.push(joinedTables.LT_COURSEUSER_SESSION);
                    from.push(`LEFT JOIN ${TablesList.LT_COURSEUSER_SESSION} AS ${TablesListAliases.LT_COURSEUSER_SESSION} ON ${TablesListAliases.LT_COURSEUSER_SESSION}."id_session" = ${TablesListAliases.LT_COURSE_SESSION}."id_session" AND ${TablesListAliases.LT_COURSEUSER_SESSION}."id_user" = ${TablesListAliases.CORE_USER}."idst"`);
                }
                select.push(`AVG(${TablesListAliases.LT_COURSEUSER_SESSION}."evaluation_score") AS ${this.renderStringInQuerySelect(translations[FieldsList.EVENT_AVERAGE_SCORE])}`);
                break;
            default:
                return false;
        }
        return true;
    }

    protected querySelectTrainingMaterialsFields(field: string, queryHelper: any): boolean {
        const {select, from, join, translations} = queryHelper;

        switch (field) {
            case FieldsList.LO_BOOKMARK:
                select.push(`
                        CASE
                            WHEN ${TablesListAliases.LEARNING_COURSE}."initial_object" = ${TablesListAliases.LEARNING_ORGANIZATION}."idorg" AND ${TablesListAliases.LEARNING_COURSE}."final_object" = ${TablesListAliases.LEARNING_ORGANIZATION}."idorg" AND ${TablesListAliases.LEARNING_COURSE}."initial_score_mode" = '${ScoresTypes.INITIAL_SCORE_TYPE_KEY_OBJECT}' AND ${TablesListAliases.LEARNING_COURSE}."final_score_mode" = '${ScoresTypes.FINAL_SCORE_TYPE_KEY_OBJECT}' THEN ${this.renderStringInQueryCase(translations[FieldTranslation.LO_BOOKMARK_START] + ' - ' + translations[FieldTranslation.LO_BOOKMARK_FINAL])}
                            WHEN ${TablesListAliases.LEARNING_COURSE}."initial_object" = ${TablesListAliases.LEARNING_ORGANIZATION}."idorg" AND ${TablesListAliases.LEARNING_COURSE}."initial_score_mode" = '${ScoresTypes.INITIAL_SCORE_TYPE_KEY_OBJECT}' THEN ${this.renderStringInQueryCase(translations[FieldTranslation.LO_BOOKMARK_START])}
                            WHEN ${TablesListAliases.LEARNING_COURSE}."final_object" = ${TablesListAliases.LEARNING_ORGANIZATION}."idorg" AND ${TablesListAliases.LEARNING_COURSE}."final_score_mode" = '${ScoresTypes.FINAL_SCORE_TYPE_KEY_OBJECT}' THEN ${this.renderStringInQueryCase(translations[FieldTranslation.LO_BOOKMARK_FINAL])}
                            ELSE '-'
                        END AS ${this.renderStringInQuerySelect(translations[FieldsList.LO_BOOKMARK])}`);
                break;
            case FieldsList.LO_DATE_ATTEMPT:
                if (!join.includes(joinedTables.LEARNING_COMMONTRACK)) {
                    join.push(joinedTables.LEARNING_COMMONTRACK);
                    from.push(`LEFT JOIN ${TablesList.LEARNING_COMMONTRACK} AS ${TablesListAliases.LEARNING_COMMONTRACK} ON ${TablesListAliases.LEARNING_COMMONTRACK}."idreference" = ${TablesListAliases.LEARNING_ORGANIZATION}."idorg" AND ${TablesListAliases.LEARNING_COMMONTRACK}."iduser" = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."iduser"`);
                }
                select.push(`${this.queryConvertTimezone(`${TablesListAliases.LEARNING_COMMONTRACK}."dateattempt"`)} AS ${this.renderStringInQuerySelect(translations[FieldsList.LO_DATE_ATTEMPT])}`);
                break;
            case FieldsList.LO_FIRST_ATTEMPT:
                if (!join.includes(joinedTables.LEARNING_COMMONTRACK)) {
                    join.push(joinedTables.LEARNING_COMMONTRACK);
                    from.push(`LEFT JOIN ${TablesList.LEARNING_COMMONTRACK} AS ${TablesListAliases.LEARNING_COMMONTRACK} ON ${TablesListAliases.LEARNING_COMMONTRACK}."idreference" = ${TablesListAliases.LEARNING_ORGANIZATION}."idorg" AND ${TablesListAliases.LEARNING_COMMONTRACK}."iduser" = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."iduser"`);
                }
                select.push(`${this.queryConvertTimezone(`${TablesListAliases.LEARNING_COMMONTRACK}."firstattempt"`)} AS ${this.renderStringInQuerySelect(translations[FieldsList.LO_FIRST_ATTEMPT])}`);
                break;
            case FieldsList.LO_SCORE:
                if (!join.includes(joinedTables.LEARNING_COMMONTRACK)) {
                    join.push(joinedTables.LEARNING_COMMONTRACK);
                    from.push(`LEFT JOIN ${TablesList.LEARNING_COMMONTRACK} AS ${TablesListAliases.LEARNING_COMMONTRACK} ON ${TablesListAliases.LEARNING_COMMONTRACK}."idreference" = ${TablesListAliases.LEARNING_ORGANIZATION}."idorg" AND ${TablesListAliases.LEARNING_COMMONTRACK}."iduser" = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."iduser"`);
                }
                select.push(`${TablesListAliases.LEARNING_COMMONTRACK}."score" AS ${this.renderStringInQuerySelect(translations[FieldsList.LO_SCORE])}`);
                break;
            case FieldsList.LO_STATUS:
                if (!join.includes(joinedTables.LEARNING_COMMONTRACK)) {
                    join.push(joinedTables.LEARNING_COMMONTRACK);
                    from.push(`LEFT JOIN ${TablesList.LEARNING_COMMONTRACK} AS ${TablesListAliases.LEARNING_COMMONTRACK} ON ${TablesListAliases.LEARNING_COMMONTRACK}."idreference" = ${TablesListAliases.LEARNING_ORGANIZATION}."idorg" AND ${TablesListAliases.LEARNING_COMMONTRACK}."iduser" = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."iduser"`);
                }
                select.push(`
                        CASE
                            WHEN ${TablesListAliases.LEARNING_COMMONTRACK}."status" = ${this.renderStringInQueryCase(LOStatus.AB_INITIO)} THEN ${this.renderStringInQueryCase(translations[FieldTranslation.LO_STATUS_NOT_STARTED])}
                            WHEN ${TablesListAliases.LEARNING_COMMONTRACK}."status" = ${this.renderStringInQueryCase(LOStatus.ATTEMPTED)} THEN ${this.renderStringInQueryCase(translations[FieldTranslation.LO_STATUS_IN_ITINERE])}
                            WHEN ${TablesListAliases.LEARNING_COMMONTRACK}."status" = ${this.renderStringInQueryCase(LOStatus.COMPLETED)} OR ${TablesListAliases.LEARNING_COMMONTRACK}."status" = ${this.renderStringInQueryCase(LOStatus.PASSED)} THEN ${this.renderStringInQueryCase(translations[FieldTranslation.LO_STATUS_COMPLETED])}
                            WHEN ${TablesListAliases.LEARNING_COMMONTRACK}."status" = ${this.renderStringInQueryCase(LOStatus.FAILED)} THEN ${this.renderStringInQueryCase(translations[FieldTranslation.LO_STATUS_FAILED])}
                            ELSE ${this.renderStringInQueryCase(translations[FieldTranslation.LO_STATUS_NOT_STARTED])}
                        END AS ${this.renderStringInQuerySelect(translations[FieldsList.LO_STATUS])}`);
                break;
            case FieldsList.LO_TITLE:
                select.push(`${TablesListAliases.LEARNING_ORGANIZATION}."title" AS ${this.renderStringInQuerySelect(translations[FieldsList.LO_TITLE])}`);
                break;
            case FieldsList.LO_TYPE:
                select.push(`
                        CASE
                            WHEN ${TablesListAliases.LEARNING_ORGANIZATION}."objecttype" = ${this.renderStringInQueryCase(LOTypes.AUTHORING)} THEN ${this.renderStringInQueryCase(translations[FieldTranslation.LO_TYPE_AUTHORING])}
                            WHEN ${TablesListAliases.LEARNING_ORGANIZATION}."objecttype" = ${this.renderStringInQueryCase(LOTypes.DELIVERABLE)} THEN ${this.renderStringInQueryCase(translations[FieldTranslation.LO_TYPE_DELIVERABLE])}
                            WHEN ${TablesListAliases.LEARNING_ORGANIZATION}."objecttype" = ${this.renderStringInQueryCase(LOTypes.FILE)} THEN ${this.renderStringInQueryCase(translations[FieldTranslation.LO_TYPE_FILE])}
                            WHEN ${TablesListAliases.LEARNING_ORGANIZATION}."objecttype" = ${this.renderStringInQueryCase(LOTypes.HTMLPAGE)} THEN ${this.renderStringInQueryCase(translations[FieldTranslation.LO_TYPE_HTMLPAGE])}
                            WHEN ${TablesListAliases.LEARNING_ORGANIZATION}."objecttype" = ${this.renderStringInQueryCase(LOTypes.POLL)} THEN ${this.renderStringInQueryCase(translations[FieldTranslation.LO_TYPE_POLL])}
                            WHEN ${TablesListAliases.LEARNING_ORGANIZATION}."objecttype" = ${this.renderStringInQueryCase(LOTypes.SCORM)} THEN ${this.renderStringInQueryCase(translations[FieldTranslation.LO_TYPE_SCORM])}
                            WHEN ${TablesListAliases.LEARNING_ORGANIZATION}."objecttype" = ${this.renderStringInQueryCase(LOTypes.TEST)} THEN ${this.renderStringInQueryCase(translations[FieldTranslation.LO_TYPE_TEST])}
                            WHEN ${TablesListAliases.LEARNING_ORGANIZATION}."objecttype" = ${this.renderStringInQueryCase(LOTypes.TINCAN)} THEN ${this.renderStringInQueryCase(translations[FieldTranslation.LO_TYPE_TINCAN])}
                            WHEN ${TablesListAliases.LEARNING_ORGANIZATION}."objecttype" = ${this.renderStringInQueryCase(LOTypes.VIDEO)} THEN ${this.renderStringInQueryCase(translations[FieldTranslation.LO_TYPE_VIDEO])}
                            WHEN ${TablesListAliases.LEARNING_ORGANIZATION}."objecttype" = ${this.renderStringInQueryCase(LOTypes.AICC)} THEN ${this.renderStringInQueryCase(translations[FieldTranslation.LO_TYPE_AICC])}
                            WHEN ${TablesListAliases.LEARNING_ORGANIZATION}."objecttype" = ${this.renderStringInQueryCase(LOTypes.ELUCIDAT)} THEN ${this.renderStringInQueryCase(translations[FieldTranslation.LO_TYPE_ELUCIDAT])}
                            WHEN ${TablesListAliases.LEARNING_ORGANIZATION}."objecttype" = ${this.renderStringInQueryCase(LOTypes.GOOGLEDRIVE)} THEN ${this.renderStringInQueryCase(translations[FieldTranslation.LO_TYPE_GOOGLEDRIVE])}
                            WHEN ${TablesListAliases.LEARNING_ORGANIZATION}."objecttype" = ${this.renderStringInQueryCase(LOTypes.LTI)} THEN ${this.renderStringInQueryCase(translations[FieldTranslation.LO_TYPE_LTI])}
                            ELSE '-'
                        END AS ${this.renderStringInQuerySelect(translations[FieldsList.LO_TYPE])}`);
                break;
            case FieldsList.LO_VERSION:
                if (!join.includes(joinedTables.LEARNING_COMMONTRACK)) {
                    join.push(joinedTables.LEARNING_COMMONTRACK);
                    from.push(`LEFT JOIN ${TablesList.LEARNING_COMMONTRACK} AS ${TablesListAliases.LEARNING_COMMONTRACK} ON ${TablesListAliases.LEARNING_COMMONTRACK}."idreference" = ${TablesListAliases.LEARNING_ORGANIZATION}."idorg" AND ${TablesListAliases.LEARNING_COMMONTRACK}."iduser" = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."iduser"`);
                }
                if (!join.includes(joinedTables.LEARNING_REPOSITORY_OBJECT_VERSION)) {
                    join.push(joinedTables.LEARNING_REPOSITORY_OBJECT_VERSION);
                    from.push(`LEFT JOIN ${TablesList.LEARNING_REPOSITORY_OBJECT_VERSION} AS ${TablesListAliases.LEARNING_REPOSITORY_OBJECT_VERSION} ON (CASE WHEN ${TablesListAliases.LEARNING_COMMONTRACK}."idresource" is null THEN ${TablesListAliases.LEARNING_ORGANIZATION}."idresource" ELSE ${TablesListAliases.LEARNING_COMMONTRACK}."idresource" END) = ${TablesListAliases.LEARNING_REPOSITORY_OBJECT_VERSION}."id_resource" AND ${TablesListAliases.LEARNING_ORGANIZATION}."objecttype" = ${TablesListAliases.LEARNING_REPOSITORY_OBJECT_VERSION}."object_type"`);
                }
                select.push(`${TablesListAliases.LEARNING_REPOSITORY_OBJECT_VERSION}."version_name" AS ${this.renderStringInQuerySelect(translations[FieldsList.LO_VERSION])}`);
                break;
            case FieldsList.LO_DATE_COMPLETE:
                if (!join.includes(joinedTables.LEARNING_COMMONTRACK)) {
                    join.push(joinedTables.LEARNING_COMMONTRACK);
                    from.push(`LEFT JOIN ${TablesList.LEARNING_COMMONTRACK} AS ${TablesListAliases.LEARNING_COMMONTRACK} ON ${TablesListAliases.LEARNING_COMMONTRACK}."idreference" = ${TablesListAliases.LEARNING_ORGANIZATION}."idorg" AND ${TablesListAliases.LEARNING_COMMONTRACK}."iduser" = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."iduser"`);
                }
                select.push(`${this.queryConvertTimezone(`${TablesListAliases.LEARNING_COMMONTRACK}."last_complete"`)} AS ${this.renderStringInQuerySelect(translations[FieldsList.LO_DATE_COMPLETE])}`);
                break;
            default:
                return false;
        }
        return true;
    }

    protected querySelectExternalTrainingFields(field: string, queryHelper: any): boolean {
        const {select, from, join, translations} = queryHelper;

        switch (field) {
            case FieldsList.EXTERNAL_TRAINING_COURSE_NAME:
                if (!join.includes(joinedTables.TRANSCRIPTS_COURSE)) {
                    join.push(joinedTables.TRANSCRIPTS_COURSE);
                    from.push(`LEFT JOIN ${TablesList.TRANSCRIPTS_COURSE} AS ${TablesListAliases.TRANSCRIPTS_COURSE} ON ${TablesListAliases.TRANSCRIPTS_COURSE}."id" = ${TablesListAliases.TRANSCRIPTS_RECORD}."course_id"`);
                }

                select.push(`
                            CASE
                                WHEN ${TablesListAliases.TRANSCRIPTS_COURSE}."id" IS NOT NULL THEN ${TablesListAliases.TRANSCRIPTS_COURSE}."course_name"
                                ELSE ${TablesListAliases.TRANSCRIPTS_RECORD}."course_name"
                            END AS ${this.renderStringInQuerySelect(translations[FieldsList.EXTERNAL_TRAINING_COURSE_NAME])}`);
                break;
            case FieldsList.EXTERNAL_TRAINING_COURSE_TYPE:
                if (!join.includes(joinedTables.TRANSCRIPTS_COURSE)) {
                    join.push(joinedTables.TRANSCRIPTS_COURSE);
                    from.push(`LEFT JOIN ${TablesList.TRANSCRIPTS_COURSE} AS ${TablesListAliases.TRANSCRIPTS_COURSE} ON ${TablesListAliases.TRANSCRIPTS_COURSE}."id" = ${TablesListAliases.TRANSCRIPTS_RECORD}."course_id"`);
                }

                select.push(`
                            CASE
                                WHEN ${TablesListAliases.TRANSCRIPTS_COURSE}."id" IS NOT NULL THEN ${TablesListAliases.TRANSCRIPTS_COURSE}."type"
                                ELSE ${TablesListAliases.TRANSCRIPTS_RECORD}."course_type"
                            END AS ${this.renderStringInQuerySelect(translations[FieldsList.EXTERNAL_TRAINING_COURSE_TYPE])}`);
                break;
            case FieldsList.EXTERNAL_TRAINING_CERTIFICATE:
                select.push(`${TablesListAliases.TRANSCRIPTS_RECORD}."original_filename" AS ${this.renderStringInQuerySelect(translations[FieldsList.EXTERNAL_TRAINING_CERTIFICATE])}`);
                break;
            case FieldsList.EXTERNAL_TRAINING_SCORE:
                select.push(`${TablesListAliases.TRANSCRIPTS_RECORD}."score" AS ${this.renderStringInQuerySelect(translations[FieldsList.EXTERNAL_TRAINING_SCORE])}`);
                break;
            case FieldsList.EXTERNAL_TRAINING_DATE:
                select.push(`TO_CHAR(${TablesListAliases.TRANSCRIPTS_RECORD}."to_date", 'YYYY-MM-DD') AS ${this.renderStringInQuerySelect(translations[FieldsList.EXTERNAL_TRAINING_DATE])}`);
                break;
            case FieldsList.EXTERNAL_TRAINING_DATE_START:
                select.push(`TO_CHAR(${TablesListAliases.TRANSCRIPTS_RECORD}."from_date", 'YYYY-MM-DD') AS ${this.renderStringInQuerySelect(translations[FieldsList.EXTERNAL_TRAINING_DATE_START])}`);
                break;
            case FieldsList.EXTERNAL_TRAINING_CREDITS:
                select.push(`${TablesListAliases.TRANSCRIPTS_RECORD}."credits" AS ${this.renderStringInQuerySelect(translations[FieldsList.EXTERNAL_TRAINING_CREDITS])}`);
                break;
            case FieldsList.EXTERNAL_TRAINING_TRAINING_INSTITUTE:
                if (!join.includes(joinedTables.TRANSCRIPTS_COURSE)) {
                    join.push(joinedTables.TRANSCRIPTS_COURSE);
                    from.push(`LEFT JOIN ${TablesList.TRANSCRIPTS_COURSE} AS ${TablesListAliases.TRANSCRIPTS_COURSE} ON ${TablesListAliases.TRANSCRIPTS_COURSE}."id" = ${TablesListAliases.TRANSCRIPTS_RECORD}."course_id"`);
                }
                if (!join.includes(joinedTables.TRANSCRIPTS_INSTITUTE)) {
                    join.push(joinedTables.TRANSCRIPTS_INSTITUTE);
                    from.push(`LEFT JOIN ${TablesList.TRANSCRIPTS_INSTITUTE} AS ${TablesListAliases.TRANSCRIPTS_INSTITUTE} ON ${TablesListAliases.TRANSCRIPTS_INSTITUTE}."id" = ${TablesListAliases.TRANSCRIPTS_COURSE}."institute_id"`);
                }

                select.push(`
                            CASE
                                WHEN ${TablesListAliases.TRANSCRIPTS_INSTITUTE}."id" IS NOT NULL THEN ${TablesListAliases.TRANSCRIPTS_INSTITUTE}."institute_name"
                                ELSE ${TablesListAliases.TRANSCRIPTS_RECORD}."training_institute"
                            END AS ${this.renderStringInQuerySelect(translations[FieldsList.EXTERNAL_TRAINING_TRAINING_INSTITUTE])}`);
                break;
            case FieldsList.EXTERNAL_TRAINING_STATUS:
                select.push(`
                            CASE
                                WHEN ${TablesListAliases.TRANSCRIPTS_RECORD}."status" = ${this.renderStringInQueryCase(ExternalTrainingStatus.REJECTED)} THEN ${this.renderStringInQueryCase(translations[FieldTranslation.EXTERNAL_TRAINING_STATUS_REJECTED])}
                                WHEN ${TablesListAliases.TRANSCRIPTS_RECORD}."status" = ${this.renderStringInQueryCase(ExternalTrainingStatus.WAITING)} THEN ${this.renderStringInQueryCase(translations[FieldTranslation.EXTERNAL_TRAINING_STATUS_WAITING])}
                                ELSE ${this.renderStringInQueryCase(translations[FieldTranslation.EXTERNAL_TRAINING_STATUS_APPROVED])}
                            END AS ${this.renderStringInQuerySelect(translations[FieldsList.EXTERNAL_TRAINING_STATUS])}`);
                break;
            default:
                return false;
        }
        return true;
    }

    protected querySelectEnrollmentFields(field: string, queryHelper: any): boolean {

        const {select, from, groupBy, archivedGroupBy, join, translations, archivedSelect} = queryHelper;
        const isToggleUsersLearningPlansReportEnhancement = this.info.type === ReportsTypes.USERS_LP && this.session.platform.isToggleUsersLearningPlansReportEnhancement();

        if(isToggleUsersLearningPlansReportEnhancement){
            this.joinCourseUserAggregate(queryHelper);
        }
            switch (field) {
                case FieldsList.COURSEUSER_LEVEL:
                    groupBy.push(`${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."level"`);
                    const courseUserLevel = (field: string) => `
                            CASE
                                WHEN ${field} = ${CourseuserLevels.Teacher} THEN ${this.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_LEVEL_TEACHER])}
                                WHEN ${field} = ${CourseuserLevels.Tutor} THEN ${this.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_LEVEL_TUTOR])}
                                ELSE ${this.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_LEVEL_STUDENT])} END AS ${this.renderStringInQuerySelect(translations[FieldsList.COURSEUSER_LEVEL])}`;
                    select.push(courseUserLevel(TablesListAliases.LEARNING_COURSEUSER_AGGREGATE + '."level"'));
                    archivedSelect.push(courseUserLevel(`${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}."enrollment_level"`));
                    archivedGroupBy.push(`${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}."enrollment_level"`);
                    break;
                case FieldsList.COURSEUSER_DATE_INSCR:
                case FieldsList.COURSE_ENROLLMENT_DATE_INSCR:
                    const courseEnrollmentDateKey = isToggleUsersLearningPlansReportEnhancement ? FieldsList.COURSE_ENROLLMENT_DATE_INSCR : FieldsList.COURSEUSER_DATE_INSCR;

                    groupBy.push(`${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."date_inscr"`);
                    select.push(`${this.queryConvertTimezone(`${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."date_inscr"`)}  AS ${this.renderStringInQuerySelect(translations[courseEnrollmentDateKey])}`);
                    archivedSelect.push(`${this.queryConvertTimezone(`${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}."enrollment_enrolled_at"`)} AS ${this.renderStringInQuerySelect(translations[courseEnrollmentDateKey])}`);
                    archivedGroupBy.push(`${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}."enrollment_enrolled_at"`);
                    break;
                case FieldsList.COURSEUSER_DATE_FIRST_ACCESS:
                    groupBy.push(`${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."date_first_access"`);
                    select.push(`${this.queryConvertTimezone(`${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."date_first_access"`)} AS ${this.renderStringInQuerySelect(translations[FieldsList.COURSEUSER_DATE_FIRST_ACCESS])}`);
                    archivedSelect.push(`${this.queryConvertTimezone(`${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}."enrollment_access_first"`)} AS ${this.renderStringInQuerySelect(translations[FieldsList.COURSEUSER_DATE_FIRST_ACCESS])}`);
                    archivedGroupBy.push(`${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}."enrollment_access_first"`);
                    break;
                case FieldsList.COURSEUSER_DATE_LAST_ACCESS:
                    groupBy.push(`${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."date_last_access"`);
                    select.push(`${this.queryConvertTimezone(`${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."date_last_access"`)} AS ${this.renderStringInQuerySelect(translations[FieldsList.COURSEUSER_DATE_LAST_ACCESS])}`);
                    archivedSelect.push(`${this.queryConvertTimezone(`${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}."enrollment_access_last"`)} AS ${this.renderStringInQuerySelect(translations[FieldsList.COURSEUSER_DATE_LAST_ACCESS])}`);
                    archivedGroupBy.push(`${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}."enrollment_access_last"`);
                    break;
                case FieldsList.COURSEUSER_DATE_COMPLETE:
                case FieldsList.COURSE_ENROLLMENT_DATE_COMPLETE:
                    const courseDateCompleteKey = isToggleUsersLearningPlansReportEnhancement ? FieldsList.COURSE_ENROLLMENT_DATE_COMPLETE : FieldsList.COURSEUSER_DATE_COMPLETE;

                    groupBy.push(`${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."date_complete"`);
                    select.push(`${this.queryConvertTimezone(`${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."date_complete"`)} AS ${this.renderStringInQuerySelect(translations[courseDateCompleteKey])}`);
                    archivedSelect.push(`${this.queryConvertTimezone(`${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}."enrollment_completed_at"`)} AS ${this.renderStringInQuerySelect(translations[courseDateCompleteKey])}`);
                    archivedGroupBy.push(`${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}."enrollment_completed_at"`);
                    break;
                case FieldsList.COURSEUSER_STATUS:
                case FieldsList.COURSE_ENROLLMENT_STATUS:
                    let courseEnrollmentStatusKey = FieldsList.COURSEUSER_STATUS;

                    if (isToggleUsersLearningPlansReportEnhancement) {
                        courseEnrollmentStatusKey = FieldsList.COURSE_ENROLLMENT_STATUS;
                        this.joinLpCoursesTables(join, from);
                    }

                    groupBy.push(`${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."status"`);
                    groupBy.push(`${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."waiting"`);
                    groupBy.push(`${TablesListAliases.LEARNING_COURSE}."course_type"`);
                    const waitingEnroll = this.info.type === ReportsTypes.USERS_ENROLLMENT_TIME ?
                        ` WHEN ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."waiting" = 1 THEN ${this.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_WAITING])} ` : '';
                    const waitingListElearning =  this.info.type !== ReportsTypes.USERS_ENROLLMENT_TIME ?
                        ` OR (${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."waiting" = 1 AND ${TablesListAliases.LEARNING_COURSE}."course_type" = 'elearning') ` : '';
                    const defaultCase = this.info.type === ReportsTypes.USERS_ENROLLMENT_TIME ?
                        `${this.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_OVERBOOKING])}` :
                        `CAST (${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."status" as varchar)`;
                    select.push(`
                            CASE
                                ${waitingEnroll}
                                WHEN ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."status" = ${EnrollmentStatuses.Confirmed} THEN ${this.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_ENROLLMENTS_TO_CONFIRM])}
                                WHEN ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."status" = ${EnrollmentStatuses.WaitingList} ${waitingListElearning} THEN ${this.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_WAITING_LIST])}
                                WHEN ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."status" = ${EnrollmentStatuses.Subscribed} THEN ${this.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_SUBSCRIBED])}
                                WHEN ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."status" = ${EnrollmentStatuses.InProgress} THEN ${this.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_IN_PROGRESS])}
                                WHEN ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."status" = ${EnrollmentStatuses.Completed} THEN ${this.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_COMPLETED])}
                                WHEN ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."status" = ${EnrollmentStatuses.Suspend} THEN ${this.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_SUSPENDED])}
                                WHEN ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."status" = ${EnrollmentStatuses.Overbooking} THEN ${this.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_OVERBOOKING])}
                                ELSE ${defaultCase}
                            END AS ${this.renderStringInQuerySelect(translations[courseEnrollmentStatusKey])}`);
                    archivedSelect.push(`
                            CASE
                                WHEN ${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}."enrollment_status" = ${EnrollmentStatuses.Confirmed} THEN ${this.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_ENROLLMENTS_TO_CONFIRM])}
                                WHEN ${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}."enrollment_status" = ${EnrollmentStatuses.WaitingList} THEN ${this.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_WAITING_LIST])}
                                WHEN ${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}."enrollment_status" = ${EnrollmentStatuses.Subscribed} THEN ${this.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_SUBSCRIBED])}
                                WHEN ${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}."enrollment_status" = ${EnrollmentStatuses.InProgress} THEN ${this.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_IN_PROGRESS])}
                                WHEN ${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}."enrollment_status" = ${EnrollmentStatuses.Completed} THEN ${this.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_COMPLETED])}
                                WHEN ${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}."enrollment_status" = ${EnrollmentStatuses.Suspend} THEN ${this.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_SUSPENDED])}
                                WHEN ${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}."enrollment_status" = ${EnrollmentStatuses.Overbooking} THEN ${this.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_OVERBOOKING])}
                                ELSE CAST (${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}."enrollment_status" as varchar)
                            END AS ${this.renderStringInQuerySelect(translations[courseEnrollmentStatusKey])}`);
                    archivedGroupBy.push(`${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}."enrollment_status"`);
                    break;
                case FieldsList.COURSEUSER_DATE_BEGIN_VALIDITY:
                case FieldsList.COURSE_ENROLLMENT_DATE_BEGIN_VALIDITY:
                    const courseEnrollmentStardDateKey = isToggleUsersLearningPlansReportEnhancement ? FieldsList.COURSE_ENROLLMENT_DATE_BEGIN_VALIDITY : FieldsList.COURSEUSER_DATE_BEGIN_VALIDITY;

                    groupBy.push(`${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."date_begin_validity"`);
                    select.push(`${this.queryConvertTimezone(`${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."date_begin_validity"`)} AS ${this.renderStringInQuerySelect(translations[courseEnrollmentStardDateKey])}`);
                    archivedSelect.push(`${this.queryConvertTimezone(`${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}."enrollment_validity_start"`)} AS ${this.renderStringInQuerySelect(translations[courseEnrollmentStardDateKey])}`);
                    archivedGroupBy.push(`${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}."enrollment_validity_start"`);
                    break;
                case FieldsList.COURSEUSER_DATE_EXPIRE_VALIDITY:
                case FieldsList.COURSE_ENROLLMENT_DATE_EXPIRE_VALIDITY:
                    const courseEnrollmentEndDateKey = isToggleUsersLearningPlansReportEnhancement ? FieldsList.COURSE_ENROLLMENT_DATE_EXPIRE_VALIDITY : FieldsList.COURSEUSER_DATE_EXPIRE_VALIDITY;

                    groupBy.push(`${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."date_expire_validity"`);
                    select.push(`${this.queryConvertTimezone(`${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."date_expire_validity"`)} AS ${this.renderStringInQuerySelect(translations[courseEnrollmentEndDateKey])}`);
                    archivedSelect.push(`${this.queryConvertTimezone(`${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}."enrollment_validity_end"`)} AS ${this.renderStringInQuerySelect(translations[courseEnrollmentEndDateKey])}`);
                    archivedGroupBy.push(`${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}."enrollment_validity_end"`);
                    break;
                case FieldsList.COURSEUSER_SCORE_GIVEN:
                    groupBy.push(`${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."score_given"`);
                    select.push(`ROUND(${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."score_given", 2) AS ${this.renderStringInQuerySelect(translations[FieldsList.COURSEUSER_SCORE_GIVEN])}`);
                    archivedSelect.push(`ROUND(${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}."enrollment_score", 2) AS ${this.renderStringInQuerySelect(translations[FieldsList.COURSEUSER_SCORE_GIVEN])}`);
                    archivedGroupBy.push(`${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}."enrollment_score"`);
                    break;
                case FieldsList.COURSEUSER_INITIAL_SCORE_GIVEN:
                    groupBy.push(`${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."initial_score_given"`);
                    select.push(`${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."initial_score_given" AS ${this.renderStringInQuerySelect(translations[FieldsList.COURSEUSER_INITIAL_SCORE_GIVEN])}`);
                    archivedSelect.push(`${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}."enrollment_score_initial" AS ${this.renderStringInQuerySelect(translations[FieldsList.COURSEUSER_INITIAL_SCORE_GIVEN])}`);
                    archivedGroupBy.push(`${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}."enrollment_score_initial"`);
                    break;
                case FieldsList.COURSE_E_SIGNATURE_HASH:
                    if (this.session.platform.checkPluginESignatureEnabled()) {
                        groupBy.push(`${TablesListAliases.LEARNING_COURSEUSER_SIGN}."signature"`);
                        if (!join.includes(joinedTables.LEARNING_COURSEUSER_SIGN)) {
                            join.push(joinedTables.LEARNING_COURSEUSER_SIGN);
                            from.push(`LEFT JOIN ${TablesList.LEARNING_COURSEUSER_SIGN} AS ${TablesListAliases.LEARNING_COURSEUSER_SIGN} ON ${TablesListAliases.LEARNING_COURSEUSER_SIGN}."course_id" = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."idcourse" AND ${TablesListAliases.LEARNING_COURSEUSER_SIGN}."user_id" = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."iduser"`);
                        }
                        select.push(`${TablesListAliases.LEARNING_COURSEUSER_SIGN}."signature" AS ${this.renderStringInQuerySelect(translations[FieldsList.COURSE_E_SIGNATURE_HASH])}`);
                        archivedSelect.push(`NULL AS ${this.renderStringInQuerySelect(translations[FieldsList.COURSE_E_SIGNATURE_HASH])}`);
                    }
                    break;
                case FieldsList.ENROLLMENT_ARCHIVING_DATE:
                    if (this.session.platform.isToggleMultipleEnrollmentCompletions()) {
                        select.push(`NULL AS ${this.renderStringInQuerySelect(translations[FieldsList.ENROLLMENT_ARCHIVING_DATE])}`);
                        archivedSelect.push(`${this.queryConvertTimezone(`${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}."created_at"`)} AS ${this.renderStringInQuerySelect(translations[FieldsList.ENROLLMENT_ARCHIVING_DATE])}`);
                        archivedGroupBy.push(`${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}."created_at"`);
                    }
                    break;
                case FieldsList.ENROLLMENT_ARCHIVED:
                    if (this.session.platform.isToggleMultipleEnrollmentCompletions()) {
                        select.push(`${this.renderStringInQueryCase(translations[FieldTranslation.NO])} AS ${this.renderStringInQuerySelect(translations[FieldsList.ENROLLMENT_ARCHIVED])}`);
                        archivedSelect.push(`${this.renderStringInQueryCase(translations[FieldTranslation.YES])} AS ${this.renderStringInQuerySelect(translations[FieldsList.ENROLLMENT_ARCHIVED])}`);
                    }
                    break;
                case FieldsList.COURSEUSER_EXPIRATION_DATE:
                    groupBy.push(`${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."date_expire_validity"`);
                    select.push(`${this.queryConvertTimezone(`${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."date_expire_validity"`)} AS ${this.renderStringInQuerySelect(translations[FieldsList.COURSEUSER_EXPIRATION_DATE])}`);
                    break;
                case FieldsList.COURSEUSER_DAYS_LEFT:
                    groupBy.push(`${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."date_expire_validity"`);
                    select.push(`
                    CASE
                        WHEN ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."date_expire_validity" IS NULL THEN NULL
                        ELSE DATEDIFF('day', DATE_TRUNC('day', CURRENT_DATE()), DATE_TRUNC('day', ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."date_expire_validity"))
                    END AS ${this.renderStringInQuerySelect(translations[FieldsList.COURSEUSER_DAYS_LEFT])}`);
                    break;
                case FieldsList.COURSEUSER_ASSIGNMENT_TYPE:
                    if (this.session.platform.isCoursesAssignmentTypeActive()) {
                        select.push(this.getCourseAssignmentTypeSelectField(false, translations));
                        groupBy.push(`${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."assignment_type"`);
                        archivedSelect.push(`NULL AS ${this.renderStringInQuerySelect(translations[FieldsList.COURSEUSER_ASSIGNMENT_TYPE])}`);
                    }
                    break;
                default:
                    return false;
            }
        return true;
    }

    private joinLpCoursesTables(join: string[], from: string[]) {
        if (!join.includes(joinedTables.LEARNING_COURSEPATH_COURSES)) {
            join.push(joinedTables.LEARNING_COURSEPATH_COURSES);
            from.push(`LEFT JOIN ${TablesList.LEARNING_COURSEPATH_COURSES} AS ${TablesListAliases.LEARNING_COURSEPATH_COURSES}
                        ON ${TablesListAliases.LEARNING_COURSEPATH_COURSES}."id_path" = ${TablesListAliases.LEARNING_COURSEPATH}."id_path"`);
        }
        if (!join.includes(joinedTables.LEARNING_COURSE)) {
            join.push(joinedTables.LEARNING_COURSE);
            from.push(`LEFT JOIN ${TablesList.LEARNING_COURSE} AS ${TablesListAliases.LEARNING_COURSE}
                            ON ${TablesListAliases.LEARNING_COURSE}."idcourse" = ${TablesListAliases.LEARNING_COURSEPATH_COURSES}."id_item"`);
        }
    }

    protected querySelectSessionEnrollmentFields(field: string, queryHelper: any, isEventFields = false): boolean {

        const { select, from, join, groupBy, archivedGroupBy, translations, archivedSelect } = queryHelper;

        switch (field) {
            case FieldsList.ENROLLMENT_DATE:
                select.push(`MAX(${this.queryConvertTimezone(`${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."date_inscr"`)}) AS ${this.renderStringInQuerySelect(translations[FieldsList.ENROLLMENT_DATE])}`);
                archivedSelect.push(`${this.queryConvertTimezone(`${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}."enrollment_enrolled_at"`)} AS ${this.renderStringInQuerySelect(translations[FieldsList.ENROLLMENT_DATE])}`);
                archivedGroupBy.push(`${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}."enrollment_enrolled_at"`);
                break;
            case FieldsList.ENROLLMENT_ENROLLMENT_STATUS:
                select.push(`
                        CASE
                            WHEN MAX(${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."status") = -1 THEN ${this.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_ENROLLMENTS_TO_CONFIRM])}
                            WHEN MAX(${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."status") = -2 THEN ${this.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_WAITING_LIST])}
                            WHEN MAX(${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."status") = 0 THEN ${this.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_SUBSCRIBED])}
                            WHEN MAX(${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."status") = 1 THEN ${this.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_IN_PROGRESS])}
                            WHEN MAX(${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."status") = 2 THEN ${this.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_COMPLETED])}
                            WHEN MAX(${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."status") = 3 THEN ${this.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_SUSPENDED])}
                            WHEN MAX(${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."status") = 4 THEN ${this.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_OVERBOOKING])}
                            ELSE CAST (MAX(${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."status") as varchar)
                        END AS ${this.renderStringInQuerySelect(translations[FieldsList.ENROLLMENT_ENROLLMENT_STATUS])}`);
                archivedSelect.push(`
                        CASE
                            WHEN ${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}."enrollment_status" = -1 THEN ${this.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_ENROLLMENTS_TO_CONFIRM])}
                            WHEN ${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}."enrollment_status" = -2 THEN ${this.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_WAITING_LIST])}
                            WHEN ${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}."enrollment_status" = 0 THEN ${this.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_SUBSCRIBED])}
                            WHEN ${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}."enrollment_status" = 1 THEN ${this.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_IN_PROGRESS])}
                            WHEN ${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}."enrollment_status" = 2 THEN ${this.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_COMPLETED])}
                            WHEN ${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}."enrollment_status" = 3 THEN ${this.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_SUSPENDED])}
                            WHEN ${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}."enrollment_status" = 4 THEN ${this.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_OVERBOOKING])}
                            ELSE CAST (${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}."enrollment_status" as varchar)
                        END AS ${this.renderStringInQuerySelect(translations[FieldsList.ENROLLMENT_ENROLLMENT_STATUS])}`);
                archivedGroupBy.push(`${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}."enrollment_status"`);
                break;
            case FieldsList.ENROLLMENT_USER_COURSE_LEVEL:
                select.push(`
                        CASE
                            WHEN ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."level" = ${CourseuserLevels.Teacher}
                                THEN ${this.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_LEVEL_TEACHER])}
                            WHEN ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."level" = ${CourseuserLevels.Tutor}
                                THEN ${this.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_LEVEL_TUTOR])}
                            ELSE ${this.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_LEVEL_STUDENT])}
                        END AS ${this.renderStringInQuerySelect(translations[FieldsList.ENROLLMENT_USER_COURSE_LEVEL])}`);

                const courseUserLevel = (field: string) => `
                        CASE
                            WHEN ${field} = ${CourseuserLevels.Teacher} THEN ${this.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_LEVEL_TEACHER])}
                            WHEN ${field} = ${CourseuserLevels.Tutor} THEN ${this.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_LEVEL_TUTOR])}
                            ELSE ${this.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_LEVEL_STUDENT])}
                        END AS ${this.renderStringInQuerySelect(translations[FieldsList.ENROLLMENT_USER_COURSE_LEVEL])}`;

                // Needed because of https://docebo.atlassian.net/browse/DD-38954.
                // This columns should handled this way, if there is a column selected from the event fields
                if (isEventFields) {
                    groupBy.push(`${TablesListAliases.LT_COURSE_SESSION_INSTRUCTOR}."id_date"`);
                    groupBy.push(`${TablesListAliases.LT_COURSE_SESSION_INSTRUCTOR}."id_session"`);
                    groupBy.push(`${TablesListAliases.LT_COURSE_SESSION_INSTRUCTOR}."id_user"`);
                }

                archivedSelect.push(courseUserLevel(`${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}."enrollment_level"`));
                archivedGroupBy.push(`${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}."enrollment_level"`);
                break;

            case FieldsList.ENROLLMENT_USER_SESSION_STATUS:
                select.push(`
                    CASE
                        WHEN MAX(${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}."status") = -2 THEN ${this.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_WAITING_LIST])}
                        WHEN MAX(${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}."status") = 0 THEN ${this.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_ENROLLED])}
                        WHEN MAX(${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}."status") = 1 THEN ${this.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_IN_PROGRESS])}
                        WHEN MAX(${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}."status") = 2 THEN ${this.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_COMPLETED])}
                        WHEN MAX(${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}."status") = 3 THEN ${this.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_SUSPENDED])}
                        ELSE ''
                    END AS ${this.renderStringInQuerySelect(translations[FieldsList.ENROLLMENT_USER_SESSION_STATUS])}`);
                        archivedSelect.push(`
                    CASE
                        WHEN CAST(JSON_EXTRACT_PATH_TEXT(${TablesListAliases.ARCHIVED_ENROLLMENT_SESSION}."enrollment_info", '$.status') AS int) = -2 THEN ${this.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_WAITING_LIST])}
                        WHEN CAST(JSON_EXTRACT_PATH_TEXT(${TablesListAliases.ARCHIVED_ENROLLMENT_SESSION}."enrollment_info", '$.status') AS int) = 0 THEN ${this.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_ENROLLED])}
                        WHEN CAST(JSON_EXTRACT_PATH_TEXT(${TablesListAliases.ARCHIVED_ENROLLMENT_SESSION}."enrollment_info", '$.status') AS int) = 1 THEN ${this.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_IN_PROGRESS])}
                        WHEN CAST(JSON_EXTRACT_PATH_TEXT(${TablesListAliases.ARCHIVED_ENROLLMENT_SESSION}."enrollment_info", '$.status') AS int) = 2 THEN ${this.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_COMPLETED])}
                        WHEN CAST(JSON_EXTRACT_PATH_TEXT(${TablesListAliases.ARCHIVED_ENROLLMENT_SESSION}."enrollment_info", '$.status') AS int) = 3 THEN ${this.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_SUSPENDED])}
                        ELSE ''
                    END AS ${this.renderStringInQuerySelect(translations[FieldsList.ENROLLMENT_USER_SESSION_STATUS])}`);
                archivedGroupBy.push(`${TablesListAliases.ARCHIVED_ENROLLMENT_SESSION}."enrollment_info"`);
                break;
            case FieldsList.ENROLLMENT_USER_SESSION_SUBSCRIBE_DATE:
                select.push(`MAX(${this.queryConvertTimezone(`${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}."date_subscribed"`)}) AS ${this.renderStringInQuerySelect(translations[FieldsList.ENROLLMENT_USER_SESSION_SUBSCRIBE_DATE])}`);
                archivedSelect.push(`${this.queryConvertTimezone(`TO_TIMESTAMP_NTZ(JSON_EXTRACT_PATH_TEXT(${TablesListAliases.ARCHIVED_ENROLLMENT_SESSION}."enrollment_info", '$.created_at'))`)} AS ${this.renderStringInQuerySelect(translations[FieldsList.ENROLLMENT_USER_SESSION_SUBSCRIBE_DATE])}`);
                archivedGroupBy.push(`${TablesListAliases.ARCHIVED_ENROLLMENT_SESSION}."enrollment_info"`);
                break;
            case FieldsList.ENROLLMENT_USER_SESSION_COMPLETE_DATE:
                select.push(`MAX(${this.queryConvertTimezone(`${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}."date_completed"`)}) AS ${this.renderStringInQuerySelect(translations[FieldsList.ENROLLMENT_USER_SESSION_COMPLETE_DATE])}`);
                archivedSelect.push(`${this.queryConvertTimezone(`TO_TIMESTAMP_NTZ(JSON_EXTRACT_PATH_TEXT(${TablesListAliases.ARCHIVED_ENROLLMENT_SESSION}."enrollment_info", '$.completed_at'))`)} AS ${this.renderStringInQuerySelect(translations[FieldsList.ENROLLMENT_USER_SESSION_COMPLETE_DATE])}`);
                archivedGroupBy.push(`${TablesListAliases.ARCHIVED_ENROLLMENT_SESSION}."enrollment_info"`);
                break;
            case FieldsList.COURSEUSER_DATE_COMPLETE:
                select.push(`MAX(${this.queryConvertTimezone(`${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."date_complete"`)}) AS ${this.renderStringInQuerySelect(translations[FieldsList.COURSEUSER_DATE_COMPLETE])}`);
                archivedSelect.push(`${this.queryConvertTimezone(`${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}."enrollment_completed_at"`)} AS ${this.renderStringInQuerySelect(translations[FieldsList.COURSEUSER_DATE_COMPLETE])}`);
                archivedGroupBy.push(`${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}."enrollment_completed_at"`);
                break;
            case FieldsList.ENROLLMENT_EVALUATION_STATUS:
                const evaluationStatus = (field: string) => `
                    CASE
                        WHEN ${field} = ${SessionEvaluationStatus.PASSED} THEN ${this.renderStringInQueryCase(translations[FieldTranslation.WEBINAR_SESSION_USER_EVAL_STATUS_PASSED])}
                        WHEN ${field} = ${SessionEvaluationStatus.FAILED} THEN ${this.renderStringInQueryCase(translations[FieldTranslation.WEBINAR_SESSION_USER_EVAL_STATUS_FAILED])}
                        ELSE ''
                    END AS ${this.renderStringInQuerySelect(translations[FieldsList.ENROLLMENT_EVALUATION_STATUS])}`;
                select.push(evaluationStatus(`${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}."evaluation_status"`));
                archivedSelect.push(evaluationStatus(`CAST(JSON_EXTRACT_PATH_TEXT(${TablesListAliases.ARCHIVED_ENROLLMENT_SESSION}."attendance_info", '$.attendance.status') AS int)`));
                groupBy.push(`${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}."evaluation_status"`);
                archivedGroupBy.push(`${TablesListAliases.ARCHIVED_ENROLLMENT_SESSION}."attendance_info"`);
                break;

            case FieldsList.ENROLLMENT_LEARNER_EVALUATION:
                select.push(`${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}."evaluation_score" AS ${this.renderStringInQuerySelect(translations[FieldsList.ENROLLMENT_LEARNER_EVALUATION])}`);
                archivedSelect.push(`NULL AS ${this.renderStringInQuerySelect(translations[FieldsList.ENROLLMENT_LEARNER_EVALUATION])}`);
                groupBy.push(`${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}."evaluation_score"`);
                break;

            case FieldsList.ENROLLMENT_INSTRUCTOR_FEEDBACK:
                select.push(`${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}."evaluation_text" AS ${this.renderStringInQuerySelect(translations[FieldsList.ENROLLMENT_INSTRUCTOR_FEEDBACK])}`);
                archivedSelect.push(`'' AS ${this.renderStringInQuerySelect(translations[FieldsList.ENROLLMENT_INSTRUCTOR_FEEDBACK])}`);
                groupBy.push(`${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}."evaluation_text"`);
                break;

            case FieldsList.ENROLLMENT_ATTENDANCE:
                if (!join.includes(joinedTables.LT_COURSE_SESSION_DATE_ATTENDANCE_AGGREGATE)) {
                    join.push(joinedTables.LT_COURSE_SESSION_DATE_ATTENDANCE_AGGREGATE);
                    from.push(`LEFT JOIN ${TablesList.LT_COURSE_SESSION_DATE_ATTENDANCE_AGGREGATE} AS ${TablesListAliases.LT_COURSE_SESSION_DATE_ATTENDANCE_AGGREGATE}
                            ON ${TablesListAliases.LT_COURSE_SESSION_DATE_ATTENDANCE_AGGREGATE}."id_session" = ${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}."id_session"
                            AND ${TablesListAliases.LT_COURSE_SESSION_DATE_ATTENDANCE_AGGREGATE}."id_user" = ${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}."id_user"
                            AND ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."level" <> 6`);
                }
                select.push(`
                    CASE
                        WHEN ${TablesListAliases.LT_COURSE_SESSION_DATE_ATTENDANCE_AGGREGATE}."attendance_time_spent" = '0h'
                            AND ${TablesListAliases.LT_COURSE_SESSION_DATE_ATTENDANCE_AGGREGATE}."session_total_time" = '0h'
                        THEN
                        (CASE
                            WHEN ${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}."total_hours" % 60 = 0
                            THEN CONCAT( CAST(${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}."total_hours" AS varchar), ' h' )
                            ELSE CONCAT('0h / ', '0h')
                            END
                            )
                        ELSE CONCAT(
                            CAST(${TablesListAliases.LT_COURSE_SESSION_DATE_ATTENDANCE_AGGREGATE}."attendance_time_spent" AS VARCHAR), ' / ' ,
                            CAST(${TablesListAliases.LT_COURSE_SESSION_DATE_ATTENDANCE_AGGREGATE}."session_total_time" AS VARCHAR) )
                    END AS ${this.renderStringInQuerySelect(translations[FieldsList.ENROLLMENT_ATTENDANCE])}`);
                archivedSelect.push(`'' AS ${this.renderStringInQuerySelect(translations[FieldsList.ENROLLMENT_ATTENDANCE])}`);
                groupBy.push(`${TablesListAliases.LT_COURSE_SESSION_DATE_ATTENDANCE_AGGREGATE}."attendance_time_spent"`);
                groupBy.push(`${TablesListAliases.LT_COURSE_SESSION_DATE_ATTENDANCE_AGGREGATE}."session_total_time"`);
                groupBy.push(`${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}."total_hours"`);
                break;
            case FieldsList.COURSE_E_SIGNATURE_HASH:
                if (this.session.platform.checkPluginESignatureEnabled()) {
                    if (!join.includes(joinedTables.LEARNING_COURSEUSER_SIGN)) {
                        join.push(joinedTables.LEARNING_COURSEUSER_SIGN);
                        from.push(`LEFT JOIN ${TablesList.LEARNING_COURSEUSER_SIGN} AS ${TablesListAliases.LEARNING_COURSEUSER_SIGN}
                                ON ${TablesListAliases.LEARNING_COURSEUSER_SIGN}."course_id" = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."idcourse"
                                AND ${TablesListAliases.LEARNING_COURSEUSER_SIGN}."user_id" = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."iduser"`);
                    }
                    select.push(`${TablesListAliases.LEARNING_COURSEUSER_SIGN}."signature" AS ${this.renderStringInQuerySelect(translations[FieldsList.COURSE_E_SIGNATURE_HASH])}`);
                }
                archivedSelect.push(`'' AS ${this.renderStringInQuerySelect(translations[FieldsList.COURSE_E_SIGNATURE_HASH])}`);
                groupBy.push(`${TablesListAliases.LEARNING_COURSEUSER_SIGN}."signature"`);
                break;
            case FieldsList.ENROLLMENT_USER_SESSION_EVENT_ATTENDANCE_HOURS:
                if (!join.includes(joinedTables.LT_COURSE_SESSION_DATE)) {
                    join.push(joinedTables.LT_COURSE_SESSION_DATE);
                    from.push(`LEFT JOIN ${TablesList.LT_COURSE_SESSION_DATE} AS ${TablesListAliases.LT_COURSE_SESSION_DATE}
                            ON ${TablesListAliases.LT_COURSE_SESSION_DATE}."id_session" = ${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}."id_session"`);
                }
                if (!join.includes(joinedTables.LT_COURSE_SESSION_DATE_ATTENDANCE)) {
                    join.push(joinedTables.LT_COURSE_SESSION_DATE_ATTENDANCE);
                    from.push(`LEFT JOIN ${TablesList.LT_COURSE_SESSION_DATE_ATTENDANCE} AS ${TablesListAliases.LT_COURSE_SESSION_DATE_ATTENDANCE}
                            ON ${TablesListAliases.LT_COURSE_SESSION_DATE_ATTENDANCE}."id_session" = ${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}."id_session"
                            AND ${TablesListAliases.LT_COURSE_SESSION_DATE_ATTENDANCE}."id_date" = ${TablesListAliases.LT_COURSE_SESSION_DATE}."id_date"
                            AND ${TablesListAliases.LT_COURSE_SESSION_DATE_ATTENDANCE}."id_user" = ${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}."id_user"
                            AND ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."level" <> 6`);
                }
                select.push(`ROUND(${TablesListAliases.LT_COURSE_SESSION_DATE_ATTENDANCE}."effective_duration" / 60, 2) AS ${this.renderStringInQuerySelect(translations[FieldsList.ENROLLMENT_USER_SESSION_EVENT_ATTENDANCE_HOURS])}`);
                archivedSelect.push(`NULL AS ${this.renderStringInQuerySelect(translations[FieldsList.ENROLLMENT_USER_SESSION_EVENT_ATTENDANCE_HOURS])}`);
                groupBy.push(`${TablesListAliases.LT_COURSE_SESSION_DATE_ATTENDANCE}."effective_duration"`);
                groupBy.push(`${TablesListAliases.LT_COURSE_SESSION_DATE}."id_date"`);
                break;
            case FieldsList.ENROLLMENT_USER_SESSION_EVENT_ATTENDANCE_STATUS:
                if (!join.includes(joinedTables.LT_COURSE_SESSION_DATE)) {
                    join.push(joinedTables.LT_COURSE_SESSION_DATE);
                    from.push(`LEFT JOIN ${TablesList.LT_COURSE_SESSION_DATE} AS ${TablesListAliases.LT_COURSE_SESSION_DATE}
                            ON ${TablesListAliases.LT_COURSE_SESSION_DATE}."id_session" = ${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}."id_session"`);
                }
                if (!join.includes(joinedTables.LT_COURSE_SESSION_DATE_ATTENDANCE)) {
                    join.push(joinedTables.LT_COURSE_SESSION_DATE_ATTENDANCE);
                    from.push(`LEFT JOIN ${TablesList.LT_COURSE_SESSION_DATE_ATTENDANCE} AS ${TablesListAliases.LT_COURSE_SESSION_DATE_ATTENDANCE}
                            ON ${TablesListAliases.LT_COURSE_SESSION_DATE_ATTENDANCE}."id_session" = ${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}."id_session"
                            AND ${TablesListAliases.LT_COURSE_SESSION_DATE_ATTENDANCE}."id_date" = ${TablesListAliases.LT_COURSE_SESSION_DATE}."id_date"
                            AND ${TablesListAliases.LT_COURSE_SESSION_DATE_ATTENDANCE}."id_user" = ${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}."id_user"
                            AND ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."level" <> 6`);
                }
                select.push(`
                        CASE
                            WHEN ${TablesListAliases.LT_COURSE_SESSION_DATE_ATTENDANCE}."attendance" = 1
                                THEN ${this.renderStringInQueryCase(translations[FieldTranslation.ENROLLMENT_USER_SESSION_EVENT_ATTENDANCE_STATUS_PRESENT])}
                            WHEN ${TablesListAliases.LT_COURSE_SESSION_DATE_ATTENDANCE}."attendance" IS NULL AND ${TablesListAliases.LT_COURSE_SESSION_DATE_ATTENDANCE}."effective_duration" > 0
                                THEN ${this.renderStringInQueryCase(translations[FieldTranslation.ENROLLMENT_USER_SESSION_EVENT_ATTENDANCE_STATUS_PRESENT])}
                            WHEN ${TablesListAliases.LT_COURSE_SESSION_DATE_ATTENDANCE}."attendance" = 0
                                THEN ${this.renderStringInQueryCase(translations[FieldTranslation.ENROLLMENT_USER_SESSION_EVENT_ATTENDANCE_STATUS_ABSENT])}
                            ELSE ${this.renderStringInQueryCase(translations[FieldTranslation.ENROLLMENT_USER_SESSION_EVENT_ATTENDANCE_STATUS_NOT_SET])}
                        END AS ${this.renderStringInQuerySelect(translations[FieldsList.ENROLLMENT_USER_SESSION_EVENT_ATTENDANCE_STATUS])}`);
                archivedSelect.push(`'' AS ${this.renderStringInQuerySelect(translations[FieldsList.ENROLLMENT_USER_SESSION_EVENT_ATTENDANCE_STATUS])}`);
                groupBy.push(`${TablesListAliases.LT_COURSE_SESSION_DATE_ATTENDANCE}."effective_duration"`);
                groupBy.push(`${TablesListAliases.LT_COURSE_SESSION_DATE_ATTENDANCE}."attendance"`);
                groupBy.push(`${TablesListAliases.LT_COURSE_SESSION_DATE}."id_date"`);
                break;
            case FieldsList.COURSEUSER_ASSIGNMENT_TYPE:
                if (this.session.platform.isCoursesAssignmentTypeActive()) {
                    select.push(this.getCourseAssignmentTypeSelectField(false, translations));
                    archivedSelect.push(`NULL AS ${this.renderStringInQuerySelect(translations[FieldsList.COURSEUSER_ASSIGNMENT_TYPE])}`);
                    groupBy.push(`${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."assignment_type"`);
                }
                break;

            case FieldsList.ENROLLMENT_ARCHIVING_DATE:
                if (this.session.platform.isToggleMultipleEnrollmentCompletions()) {
                    select.push(`NULL AS ${this.renderStringInQuerySelect(translations[FieldsList.ENROLLMENT_ARCHIVING_DATE])}`);
                    archivedSelect.push(`${this.queryConvertTimezone(`${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}."created_at"`)} AS ${this.renderStringInQuerySelect(translations[FieldsList.ENROLLMENT_ARCHIVING_DATE])}`);
                    archivedGroupBy.push(`${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}."created_at"`);
                }
                break;
            case FieldsList.ENROLLMENT_ARCHIVED:
                if (this.session.platform.isToggleMultipleEnrollmentCompletions()) {
                    select.push(`${this.renderStringInQueryCase(translations[FieldTranslation.NO])} AS ${this.renderStringInQuerySelect(translations[FieldsList.ENROLLMENT_ARCHIVED])}`);
                    archivedSelect.push(`${this.renderStringInQueryCase(translations[FieldTranslation.YES])} AS ${this.renderStringInQuerySelect(translations[FieldsList.ENROLLMENT_ARCHIVED])}`);
                }
                break;
            default:
                return false;
        }
        return true;
    }

    protected joinCourseUserAggregate(queryHelper: any): void {
        const {from, join} = queryHelper;
        if (!join.includes(joinedTables.LEARNING_COURSEPATH_COURSES)) {
            join.push(joinedTables.LEARNING_COURSEPATH_COURSES);
            from.push(`LEFT JOIN ${TablesList.LEARNING_COURSEPATH_COURSES} AS ${TablesListAliases.LEARNING_COURSEPATH_COURSES}
                            ON ${TablesListAliases.LEARNING_COURSEPATH_COURSES}."id_path" = ${TablesListAliases.LEARNING_COURSEPATH}."id_path"`);
        }
        if (!join.includes(joinedTables.LEARNING_COURSEUSER_AGGREGATE)) {
            join.push(joinedTables.LEARNING_COURSEUSER_AGGREGATE);
            from.push(`LEFT JOIN ${TablesList.LEARNING_COURSEUSER_AGGREGATE} AS ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}
                            ON ${TablesListAliases.LEARNING_COURSEPATH_COURSES}."id_item" = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."idcourse"
                            AND ${TablesListAliases.CORE_USER}."idst" = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."iduser"`);
        }
        if (!join.includes(joinedTables.LEARNING_COURSEPATH_USER_COMPLETED_COURSES)) {
            join.push(joinedTables.LEARNING_COURSEPATH_USER_COMPLETED_COURSES);
            from.push(`LEFT JOIN ${TablesList.LEARNING_COURSEPATH_USER_COMPLETED_COURSES} AS ${TablesListAliases.LEARNING_COURSEPATH_USER_COMPLETED_COURSES}
                            ON ${TablesListAliases.LEARNING_COURSEPATH_USER_COMPLETED_COURSES}."idpath" = ${TablesListAliases.LEARNING_COURSEPATH}."id_path"
                            AND ${TablesListAliases.LEARNING_COURSEPATH_USER_COMPLETED_COURSES}."iduser" = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."iduser"`);
        }
        if (!join.includes(joinedTables.LEARNING_COURSEPATH_COURSES_COUNT)) {
            join.push(joinedTables.LEARNING_COURSEPATH_COURSES_COUNT);
            from.push(`LEFT JOIN ${TablesList.LEARNING_COURSEPATH_COURSES_COUNT} AS ${TablesListAliases.LEARNING_COURSEPATH_COURSES_COUNT}
                            ON ${TablesListAliases.LEARNING_COURSEPATH_COURSES_COUNT}."id_path" = ${TablesListAliases.LEARNING_COURSEPATH}."id_path"`);
        }
    }

    protected querySelectLpUsageStatisticsFields(field: string, queryHelper: any): boolean {
        const {select, translations} = queryHelper;
        const enrolledUsers = `COUNT(DISTINCT(${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."iduser"))`;
        const inProgressOrCompletedUsers = `COUNT(DISTINCT(
                                    CASE WHEN
                                            ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."status" = ${EnrollmentStatuses.InProgress} OR
                                            ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."status" = ${EnrollmentStatuses.Completed}
                                    THEN ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."iduser" ELSE null END))`;
        const completedMandatoryCoursesUsers = `COUNT(DISTINCT(
                                    CASE WHEN ${TablesListAliases.LEARNING_COURSEPATH_COURSES_COUNT}."coursesMandatory" > 0 AND
                                            ${TablesListAliases.LEARNING_COURSEPATH_USER_COMPLETED_COURSES}."completedCoursesMandatory"
                                                >= ${TablesListAliases.LEARNING_COURSEPATH_COURSES_COUNT}."coursesMandatory"
                                    THEN ${TablesListAliases.LEARNING_COURSEPATH_USER_COMPLETED_COURSES}."iduser" ELSE NULL END)
                                    )`;
        const inProgressUsers = `(${inProgressOrCompletedUsers} - ${completedMandatoryCoursesUsers})`;
        switch (field) {
            case FieldsList.STATS_PATH_ENROLLED_USERS:
                select.push(`${enrolledUsers} AS ${this.renderStringInQuerySelect(translations[FieldsList.STATS_PATH_ENROLLED_USERS])}`);
                break;
            case FieldsList.STATS_PATH_NOT_STARTED_USERS:
                select.push(`${enrolledUsers} - ${inProgressOrCompletedUsers} AS ${this.renderStringInQuerySelect(translations[FieldsList.STATS_PATH_NOT_STARTED_USERS])}`);
                break;
            case FieldsList.STATS_PATH_NOT_STARTED_USERS_PERCENTAGE:
                select.push(`TRUNCATE(
                                        CASE
                                            WHEN ${enrolledUsers} = 0 THEN 0
                                            ELSE ROUND((${enrolledUsers} - ${inProgressOrCompletedUsers}) * 100) / ${enrolledUsers}
                                        END
                                    ) AS ${this.renderStringInQuerySelect(translations[FieldsList.STATS_PATH_NOT_STARTED_USERS_PERCENTAGE])}`);
                break;
            case FieldsList.STATS_PATH_IN_PROGRESS_USERS:
                select.push(`${inProgressUsers} AS ${this.renderStringInQuerySelect(translations[FieldsList.STATS_PATH_IN_PROGRESS_USERS])}`);
                break;
            case FieldsList.STATS_PATH_IN_PROGRESS_USERS_PERCENTAGE:
                select.push(`TRUNCATE(
                                    CASE
                                        WHEN ${enrolledUsers} = 0 THEN 0
                                        ELSE ROUND(${inProgressUsers} * 100 /${enrolledUsers})
                                    END
                                ) AS ${this.renderStringInQuerySelect(translations[FieldsList.STATS_PATH_IN_PROGRESS_USERS_PERCENTAGE])}`);
                break;
            case FieldsList.STATS_PATH_COMPLETED_USERS:
                select.push(`${completedMandatoryCoursesUsers} AS ${this.renderStringInQuerySelect(translations[FieldsList.STATS_PATH_COMPLETED_USERS])}`);
                break;
            case FieldsList.STATS_PATH_COMPLETED_USERS_PERCENTAGE:
                select.push(`TRUNCATE(
                                        CASE
                                            WHEN ${enrolledUsers} = 0 THEN 0
                                            ELSE ROUND(${completedMandatoryCoursesUsers} * 100) / ${enrolledUsers}
                                        END
                                    ) AS ${this.renderStringInQuerySelect(translations[FieldsList.STATS_PATH_COMPLETED_USERS_PERCENTAGE])}`);
                break;
            default:
                return false;
        }
        this.joinCourseUserAggregate(queryHelper);

        return true;
    }

    protected querySelectUsageStatisticsFields(field: string, queryHelper: any): boolean {

        const {select, from, join, groupBy, archivedGroupBy, translations, archivedSelect} = queryHelper;

            switch (field) {
                case FieldsList.STATS_USER_COURSE_COMPLETION_PERCENTAGE:
                    groupBy.push(`${TablesListAliases.LEARNING_COMMONTRACK_COMPLETED}."completed"`);
                    groupBy.push(`${TablesListAliases.LEARNING_ORGANIZATION_COUNT}."count"`);
                    if (!join.includes(joinedTables.LEARNING_ORGANIZATION_COUNT)) {
                        join.push(joinedTables.LEARNING_ORGANIZATION_COUNT);
                        from.push(`LEFT JOIN ${TablesList.LEARNING_ORGANIZATION_COUNT} AS ${TablesListAliases.LEARNING_ORGANIZATION_COUNT} ON ${TablesListAliases.LEARNING_ORGANIZATION_COUNT}."idcourse" = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."idcourse"`);
                    }
                    if (!join.includes(joinedTables.LEARNING_COMMONTRACK_COMPLETED)) {
                        join.push(joinedTables.LEARNING_COMMONTRACK_COMPLETED);
                        from.push(`LEFT JOIN ${TablesList.LEARNING_COMMONTRACK_COMPLETED} AS ${TablesListAliases.LEARNING_COMMONTRACK_COMPLETED} ON ${TablesListAliases.LEARNING_COMMONTRACK_COMPLETED}."iduser" = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."iduser" AND ${TablesListAliases.LEARNING_COMMONTRACK_COMPLETED}."idcourse" = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."idcourse"`);
                    }
                    select.push(`
                            TRUNCATE(
                                CASE
                                    WHEN ${TablesListAliases.LEARNING_COMMONTRACK_COMPLETED}."completed" IS NOT NULL AND ${TablesListAliases.LEARNING_ORGANIZATION_COUNT}."count" > 0 THEN (${TablesListAliases.LEARNING_COMMONTRACK_COMPLETED}."completed" * 100) / ${TablesListAliases.LEARNING_ORGANIZATION_COUNT}."count"
                                    ELSE 0
                                END
                            ) AS ${this.renderStringInQuerySelect(translations[FieldsList.STATS_USER_COURSE_COMPLETION_PERCENTAGE])}`);
                    archivedSelect.push(`NULL AS ${this.renderStringInQuerySelect(translations[FieldsList.STATS_USER_COURSE_COMPLETION_PERCENTAGE])}`);
                    break;
                case FieldsList.STATS_TOTAL_TIME_IN_COURSE:
                    if (this.info.type !== ReportsTypes.GROUPS_COURSES && this.info.type !== ReportsTypes.COURSES_USERS && this.info.type !== ReportsTypes.SESSIONS_USER_DETAIL) {
                        groupBy.push(`${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}."totaltime"`);
                    }
                    if (!join.includes(joinedTables.LEARNING_TRACKSESSION_AGGREGATE)) {
                        join.push(joinedTables.LEARNING_TRACKSESSION_AGGREGATE);
                        const fieldJoinTotalTime = this.info.type === ReportsTypes.COURSES_USERS ? `${TablesListAliases.CORE_USER}."idst"` : `${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."iduser"`;
                        from.push(`LEFT JOIN ${TablesList.LEARNING_TRACKSESSION_AGGREGATE} AS ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE} ON ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}."iduser" = ${fieldJoinTotalTime} AND ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}."idcourse" = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."idcourse"`);
                    }
                    let field = `${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}."totaltime"`;
                    if (this.info.type === ReportsTypes.GROUPS_COURSES || this.info.type === ReportsTypes.COURSES_USERS || this.info.type === ReportsTypes.SESSIONS_USER_DETAIL) {
                        field = `SUM(${field})`;
                    }
                    select.push(`${field} AS ${this.renderStringInQuerySelect(translations[FieldsList.STATS_TOTAL_TIME_IN_COURSE])}`);
                    archivedSelect.push(`NULL AS ${this.renderStringInQuerySelect(translations[FieldsList.STATS_TOTAL_TIME_IN_COURSE])}`);
                    break;
                case FieldsList.STATS_TOTAL_SESSIONS_IN_COURSE:
                    groupBy.push(`${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}."actions"`);
                    if (!join.includes(joinedTables.LEARNING_TRACKSESSION_AGGREGATE)) {
                        join.push(joinedTables.LEARNING_TRACKSESSION_AGGREGATE);
                        from.push(`LEFT JOIN ${TablesList.LEARNING_TRACKSESSION_AGGREGATE} AS ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE} ON ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}."iduser" = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."iduser" AND ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}."idcourse" = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."idcourse"`);
                    }
                    select.push(`${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}."actions" AS ${this.renderStringInQuerySelect(translations[FieldsList.STATS_TOTAL_SESSIONS_IN_COURSE])}`);
                    archivedSelect.push(`${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}."enrollment_sessions_count" AS ${this.renderStringInQuerySelect(translations[FieldsList.STATS_TOTAL_SESSIONS_IN_COURSE])}`);
                    archivedGroupBy.push(`${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}."enrollment_sessions_count"`);
                    break;
                case FieldsList.STATS_NUMBER_OF_ACTIONS:
                    groupBy.push(`${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}."numberofactions"`);
                    if (!join.includes(joinedTables.LEARNING_TRACKSESSION_AGGREGATE)) {
                        join.push(joinedTables.LEARNING_TRACKSESSION_AGGREGATE);
                        from.push(`LEFT JOIN ${TablesList.LEARNING_TRACKSESSION_AGGREGATE} AS ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE} ON ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}."iduser" = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."iduser" AND ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}."idcourse" = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."idcourse"`);
                    }
                    select.push(`${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}."numberofactions" AS ${this.renderStringInQuerySelect(translations[FieldsList.STATS_NUMBER_OF_ACTIONS])}`);
                    archivedSelect.push(`NULL AS ${this.renderStringInQuerySelect(translations[FieldsList.STATS_NUMBER_OF_ACTIONS])}`);
                    break;
                case FieldsList.STATS_SESSION_TIME:
                    if (this.info.type !== ReportsTypes.COURSES_USERS && this.info.type !== ReportsTypes.GROUPS_COURSES && this.info.type !== ReportsTypes.SESSIONS_USER_DETAIL) {
                        groupBy.push(`${TablesListAliases.COURSE_SESSION_TIME_AGGREGATE}."session_time"`);
                    }
                    if (!join.includes(joinedTables.COURSE_SESSION_TIME_AGGREGATE)) {
                        join.push(joinedTables.COURSE_SESSION_TIME_AGGREGATE);
                        const fieldJoinSessionTime = this.info.type === ReportsTypes.COURSES_USERS ? `${TablesListAliases.CORE_USER}."idst"` : `${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."iduser"`;
                        from.push(`LEFT JOIN ${TablesList.COURSE_SESSION_TIME_AGGREGATE} AS ${TablesListAliases.COURSE_SESSION_TIME_AGGREGATE} ON ${TablesListAliases.COURSE_SESSION_TIME_AGGREGATE}."id_user" = ${fieldJoinSessionTime} AND ${TablesListAliases.COURSE_SESSION_TIME_AGGREGATE}."course_id" = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."idcourse"`);
                    }
                    let fieldTime = `${TablesListAliases.COURSE_SESSION_TIME_AGGREGATE}."session_time"`;
                    if (this.info.type === ReportsTypes.COURSES_USERS || this.info.type === ReportsTypes.GROUPS_COURSES || this.info.type === ReportsTypes.SESSIONS_USER_DETAIL) {
                        fieldTime = `SUM(${fieldTime})`;
                    }
                    select.push(`${fieldTime} AS ${this.renderStringInQuerySelect(translations[FieldsList.STATS_SESSION_TIME])}`);
                    archivedSelect.push(`${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}."enrollment_time_spent" AS ${this.renderStringInQuerySelect(translations[FieldsList.STATS_SESSION_TIME])}`);
                    archivedGroupBy.push(`${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}."enrollment_time_spent"`);
                    break;
                case FieldsList.STATS_ENROLLED_USERS:
                    select.push(`COUNT(DISTINCT(${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."iduser")) AS ${this.renderStringInQuerySelect(translations[FieldsList.STATS_ENROLLED_USERS])}`);
                    break;
                case FieldsList.STATS_IN_PROGRESS_USERS:
                    if (this.info.type === ReportsTypes.SESSIONS_USER_DETAIL) {
                        select.push(`COUNT(DISTINCT(CASE WHEN ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."status" = ${EnrollmentStatuses.InProgress} THEN ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."iduser" ELSE null END)) AS ${this.renderStringInQuerySelect(translations[FieldsList.STATS_IN_PROGRESS_USERS])}`);
                    } else {
                        select.push(`SUM(CASE WHEN ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."status" = ${EnrollmentStatuses.InProgress} ${this.info.type !== ReportsTypes.GROUPS_COURSES ? `AND ${TablesListAliases.CORE_USER}."idst" IS NOT NULL` : ''} THEN 1 ELSE 0 END) AS ${this.renderStringInQuerySelect(translations[FieldsList.STATS_IN_PROGRESS_USERS])}`);
                    }
                    break;
                case FieldsList.STATS_IN_PROGRESS_USERS_PERCENTAGE:
                    switch (this.info.type) {
                        case ReportsTypes.SESSIONS_USER_DETAIL:
                        case ReportsTypes.GROUPS_COURSES:
                            select.push(`ROUND(((SUM(CASE WHEN ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."status" = ${EnrollmentStatuses.InProgress} THEN 1 ELSE 0 END) * 100.0) / COUNT(${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."iduser")), 2) AS ${this.renderStringInQuerySelect(translations[FieldsList.STATS_IN_PROGRESS_USERS_PERCENTAGE])}`);
                            break;
                        default:
                            select.push(`
                                TRUNCATE(
                                    CASE
                                        WHEN COUNT(DISTINCT(${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."iduser")) = 0 THEN 0
                                        ELSE ROUND(SUM(CASE WHEN ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."status" = ${EnrollmentStatuses.InProgress} AND ${TablesListAliases.CORE_USER}."idst" IS NOT NULL THEN 1 ELSE 0 END) * 100 / COUNT(DISTINCT(${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."iduser")))
                                    END
                                ) AS ${this.renderStringInQuerySelect(translations[FieldsList.STATS_IN_PROGRESS_USERS_PERCENTAGE])}`);
                            break;
                    }
                    break;
                case FieldsList.STATS_COMPLETED_USERS:
                    if (this.info.type === ReportsTypes.SESSIONS_USER_DETAIL) {
                        select.push(`COUNT(DISTINCT(CASE WHEN ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."status" = ${EnrollmentStatuses.Completed} THEN ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."iduser" ELSE null END)) AS ${this.renderStringInQuerySelect(translations[FieldsList.STATS_COMPLETED_USERS])}`);
                    } else {
                        select.push(`SUM(CASE WHEN ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."status" = ${EnrollmentStatuses.Completed} ${this.info.type !== ReportsTypes.GROUPS_COURSES ? `AND ${TablesListAliases.CORE_USER}."idst" IS NOT NULL` : ''} THEN 1 ELSE 0 END) AS ${this.renderStringInQuerySelect(translations[FieldsList.STATS_COMPLETED_USERS])}`);
                    }
                    break;
                case FieldsList.STATS_COMPLETED_USERS_PERCENTAGE:
                    switch (this.info.type) {
                        case ReportsTypes.SESSIONS_USER_DETAIL:
                        case ReportsTypes.GROUPS_COURSES:
                            select.push(`ROUND(((SUM(CASE WHEN ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."status" = ${EnrollmentStatuses.Completed} THEN 1 ELSE 0 END) * 100.0) / COUNT(${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."iduser")), 2) AS ${this.renderStringInQuerySelect(translations[FieldsList.STATS_COMPLETED_USERS_PERCENTAGE])}`);
                            break;
                        default:
                            select.push(`
                                TRUNCATE(
                                CASE
                                    WHEN COUNT(DISTINCT(${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."iduser")) = 0 THEN 0
                                    ELSE ROUND(SUM(CASE WHEN ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."status" = ${EnrollmentStatuses.Completed} AND ${TablesListAliases.CORE_USER}."idst" IS NOT NULL THEN 1 ELSE 0 END) * 100 / COUNT(DISTINCT(${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."iduser")))
                                END
                            ) AS ${this.renderStringInQuerySelect(translations[FieldsList.STATS_COMPLETED_USERS_PERCENTAGE])}`);
                            break;
                    }
                    break;
                case FieldsList.STATS_NOT_STARTED_USERS:
                    select.push(`COUNT(DISTINCT(
                                    CASE WHEN ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."status" = ${EnrollmentStatuses.Subscribed}
                                    THEN ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."iduser"
                                    ELSE null
                                 END)) AS ${this.renderStringInQuerySelect(translations[FieldsList.STATS_NOT_STARTED_USERS])}`);
                    break;
                case FieldsList.STATS_NOT_STARTED_USERS_PERCENTAGE:
                    switch (this.info.type) {
                        case ReportsTypes.SESSIONS_USER_DETAIL:
                        case ReportsTypes.GROUPS_COURSES:
                            select.push(`ROUND(((SUM(CASE WHEN ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."status" = ${EnrollmentStatuses.Subscribed} THEN 1 ELSE 0 END) * 100.0) / COUNT(${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."iduser")), 2) AS ${this.renderStringInQuerySelect(translations[FieldsList.STATS_NOT_STARTED_USERS_PERCENTAGE])}`);
                            break;
                        default:
                            select.push(`
                                    TRUNCATE(
                                        CASE
                                            WHEN COUNT(DISTINCT(${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."iduser")) = 0 THEN 0
                                            ELSE ROUND(SUM(CASE WHEN ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."status" <> ${EnrollmentStatuses.InProgress} AND ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."status" <> ${EnrollmentStatuses.Completed} AND ${TablesListAliases.CORE_USER}."idst" IS NOT NULL THEN 1 ELSE 0 END * 100) / COUNT(DISTINCT(${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."iduser")))
                                        END
                                    ) AS ${this.renderStringInQuerySelect(translations[FieldsList.STATS_NOT_STARTED_USERS_PERCENTAGE])}`);
                            break;
                    }
                    break;
                case FieldsList.STATS_USERS_ENROLLED_IN_COURSE:
                    select.push(`COUNT(DISTINCT(${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."iduser")) AS ${this.renderStringInQuerySelect(translations[FieldsList.STATS_USERS_ENROLLED_IN_COURSE])}`);
                    break;
                case FieldsList.STATS_COURSE_RATING:
                    groupBy.push(`${TablesListAliases.LEARNING_COURSE_RATING}."rate_average"`);
                    if (!join.includes(joinedTables.LEARNING_COURSE_RATING)) {
                        let joinField = '';
                        if (this.info.type === ReportsTypes.SESSIONS_USER_DETAIL) {
                            joinField = `${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."idcourse"`;
                        } else {
                            joinField = `${TablesListAliases.LEARNING_COURSE}."idcourse"`;
                        }
                        join.push(joinedTables.LEARNING_COURSE_RATING);
                        from.push(`LEFT JOIN ${TablesList.LEARNING_COURSE_RATING} AS ${TablesListAliases.LEARNING_COURSE_RATING} ON ${TablesListAliases.LEARNING_COURSE_RATING}."idcourse" = ${joinField}`);
                    }
                    select.push(`CAST(${TablesListAliases.LEARNING_COURSE_RATING}."rate_average" AS INTEGER) AS ${this.renderStringInQuerySelect(translations[FieldsList.STATS_COURSE_RATING])}`);
                    break;
                case FieldsList.STATS_ISSUED:
                    select.push(`COUNT(${TablesListAliases.CERTIFICATION_USER}."id_user") AS ${this.renderStringInQuerySelect(translations[FieldsList.STATS_ISSUED])}`);
                    break;
                case FieldsList.STATS_EXPIRED:
                    select.push(`
                                SUM(CASE
                                    WHEN ${TablesListAliases.CERTIFICATION_USER}."archived" = 0
                                        AND ${TablesListAliases.CERTIFICATION_USER}."expire_at" <= CURRENT_TIMESTAMP
                                    THEN 1
                                    ELSE 0
                                END) AS ${this.renderStringInQuerySelect(translations[FieldsList.STATS_EXPIRED])}`);
                    break;
                case FieldsList.STATS_ACTIVE:
                    select.push(`
                                SUM(CASE
                                    WHEN ${TablesListAliases.CERTIFICATION_USER}."archived" = 0
                                        AND ${TablesListAliases.CERTIFICATION_USER}."on_datetime" <= CURRENT_TIMESTAMP
                                        AND (${TablesListAliases.CERTIFICATION_USER}."expire_at" > CURRENT_TIMESTAMP
                                            OR ${TablesListAliases.CERTIFICATION_USER}."expire_at" IS NULL)
                                    THEN 1
                                    ELSE 0
                                END) AS ${this.renderStringInQuerySelect(translations[FieldsList.STATS_ACTIVE])}`);
                    break;
                case FieldsList.STATS_ARCHIVED:
                    select.push(`SUM(${TablesListAliases.CERTIFICATION_USER}."archived") AS ${this.renderStringInQuerySelect(translations[FieldsList.STATS_ARCHIVED])}`);
                    break;
                default:
                    return false;
            }
        return true;
    }

    protected querySelectSessionStatisticsFields(field: string, queryHelper: any): boolean {

        const {select, from, join, groupBy, translations} = queryHelper;

        switch (field) {
            case FieldsList.SESSION_USER_ENROLLED:
                if (!join.includes(joinedTables.LT_COURSEUSER_SESSION)) {
                    join.push(joinedTables.LT_COURSEUSER_SESSION);
                    from.push(`LEFT JOIN ${TablesList.LT_COURSEUSER_SESSION} AS ${TablesListAliases.LT_COURSEUSER_SESSION} ON ${TablesListAliases.LT_COURSEUSER_SESSION}."id_session" = ${TablesListAliases.LT_COURSE_SESSION}."id_session" AND ${TablesListAliases.LT_COURSEUSER_SESSION}."id_user" = ${TablesListAliases.CORE_USER}."idst"`);
                }
                select.push(`COUNT(DISTINCT(IFF(${TablesListAliases.LT_COURSEUSER_SESSION}."status" = 0, ${TablesListAliases.LT_COURSEUSER_SESSION}."id_user", null))) AS ${this.renderStringInQuerySelect(translations[FieldsList.SESSION_USER_ENROLLED])}`);
                break;
            case FieldsList.SESSION_USER_COMPLETED:
                if (!join.includes(joinedTables.LT_COURSEUSER_SESSION)) {
                    join.push(joinedTables.LT_COURSEUSER_SESSION);
                    from.push(`LEFT JOIN ${TablesList.LT_COURSEUSER_SESSION} AS ${TablesListAliases.LT_COURSEUSER_SESSION} ON ${TablesListAliases.LT_COURSEUSER_SESSION}."id_session" = ${TablesListAliases.LT_COURSE_SESSION}."id_session" AND ${TablesListAliases.LT_COURSEUSER_SESSION}."id_user" = ${TablesListAliases.CORE_USER}."idst"`);
                }
                select.push(`COUNT(DISTINCT(IFF(${TablesListAliases.LT_COURSEUSER_SESSION}."status" = 2, ${TablesListAliases.LT_COURSEUSER_SESSION}."id_user", null))) AS ${this.renderStringInQuerySelect(translations[FieldsList.SESSION_USER_COMPLETED])}`);
                break;
            case FieldsList.SESSION_USER_WAITING:
                if (!join.includes(joinedTables.LT_COURSEUSER_SESSION)) {
                    join.push(joinedTables.LT_COURSEUSER_SESSION);
                    from.push(`LEFT JOIN ${TablesList.LT_COURSEUSER_SESSION} AS ${TablesListAliases.LT_COURSEUSER_SESSION} ON ${TablesListAliases.LT_COURSEUSER_SESSION}."id_session" = ${TablesListAliases.LT_COURSE_SESSION}."id_session" AND ${TablesListAliases.LT_COURSEUSER_SESSION}."id_user" = ${TablesListAliases.CORE_USER}."idst"`);
                }
                select.push(`COUNT(DISTINCT(IFF(${TablesListAliases.LT_COURSEUSER_SESSION}."status" = -2, ${TablesListAliases.LT_COURSEUSER_SESSION}."id_user", null))) AS ${this.renderStringInQuerySelect(translations[FieldsList.SESSION_USER_WAITING])}`);
                break;
            case FieldsList.SESSION_USER_IN_PROGRESS:
                if (!join.includes(joinedTables.LT_COURSEUSER_SESSION)) {
                    join.push(joinedTables.LT_COURSEUSER_SESSION);
                    from.push(`LEFT JOIN ${TablesList.LT_COURSEUSER_SESSION} AS ${TablesListAliases.LT_COURSEUSER_SESSION} ON ${TablesListAliases.LT_COURSEUSER_SESSION}."id_session" = ${TablesListAliases.LT_COURSE_SESSION}."id_session" AND ${TablesListAliases.LT_COURSEUSER_SESSION}."id_user" = ${TablesListAliases.CORE_USER}."idst"`);
                }
                select.push(`COUNT(DISTINCT(IFF(${TablesListAliases.LT_COURSEUSER_SESSION}."status" = 1, ${TablesListAliases.LT_COURSEUSER_SESSION}."id_user", null))) AS ${this.renderStringInQuerySelect(translations[FieldsList.SESSION_USER_IN_PROGRESS])}`);
                break;
            case FieldsList.SESSION_COMPLETION_MODE:
                select.push(`
                            CASE
                                WHEN ${TablesListAliases.LT_COURSE_SESSION}."evaluation_type" = 3 THEN ${this.renderStringInQueryCase(translations[FieldTranslation.SESSION_MANUAL])}
                                WHEN ${TablesListAliases.LT_COURSE_SESSION}."evaluation_type" = 0 AND ${TablesListAliases.LT_COURSE_SESSION}."score_base" > 0 THEN ${this.renderStringInQueryCase(translations[FieldTranslation.SESSION_EVALUATION_BASED])}
                                WHEN ${TablesListAliases.LT_COURSE_SESSION}."evaluation_type" = 0 OR ${TablesListAliases.LT_COURSE_SESSION}."evaluation_type" = 2 THEN ${this.renderStringInQueryCase(translations[FieldTranslation.SESSION_ATTENDANCE_BASED])}
                                WHEN ${TablesListAliases.LT_COURSE_SESSION}."evaluation_type" = 1 THEN ${this.renderStringInQueryCase(translations[FieldTranslation.SESSION_TRAINING_MATERIAL_BASED])}
                                ELSE 'UNKNOWN'
                            END AS ${this.renderStringInQuerySelect(translations[FieldsList.SESSION_COMPLETION_MODE])}`);
                groupBy.push(`${TablesListAliases.LT_COURSE_SESSION}."evaluation_type"`);
                groupBy.push(`${TablesListAliases.LT_COURSE_SESSION}."score_base"`);
                break;
            case FieldsList.SESSION_EVALUATION_STATUS_PASSED:
                if (!join.includes(joinedTables.LT_COURSEUSER_SESSION)) {
                    join.push(joinedTables.LT_COURSEUSER_SESSION);
                    from.push(`LEFT JOIN ${TablesList.LT_COURSEUSER_SESSION} AS ${TablesListAliases.LT_COURSEUSER_SESSION} ON ${TablesListAliases.LT_COURSEUSER_SESSION}."id_session" = ${TablesListAliases.LT_COURSE_SESSION}."id_session" AND ${TablesListAliases.LT_COURSEUSER_SESSION}."id_user" = ${TablesListAliases.CORE_USER}."idst"`);
                }
                select.push(`IFF(${TablesListAliases.LT_COURSE_SESSION}."evaluation_type" = 0 AND ${TablesListAliases.LT_COURSE_SESSION}."score_base" > 0,
                                TO_CHAR(TRUNCATE(
                                    IFF(${this.getSqlUserWithEvaluationSnowflake()} > 0,
                                    COUNT(DISTINCT(IFF(${TablesListAliases.LT_COURSEUSER_SESSION}."evaluation_status" = 1, ${TablesListAliases.LT_COURSEUSER_SESSION}."id_user", null))) * 100 / ${this.getSqlUserWithEvaluationSnowflake()},
                                    0)
                                )),
                             '') AS ${this.renderStringInQuerySelect(translations[FieldsList.SESSION_EVALUATION_STATUS_PASSED])}`);
                groupBy.push(`${TablesListAliases.LT_COURSE_SESSION}."evaluation_type"`);
                groupBy.push(`${TablesListAliases.LT_COURSE_SESSION}."score_base"`);
                break;
            case FieldsList.SESSION_EVALUATION_STATUS_NOT_SET:
                if (!join.includes(joinedTables.LT_COURSEUSER_SESSION)) {
                    join.push(joinedTables.LT_COURSEUSER_SESSION);
                    from.push(`LEFT JOIN ${TablesList.LT_COURSEUSER_SESSION} AS ${TablesListAliases.LT_COURSEUSER_SESSION} ON ${TablesListAliases.LT_COURSEUSER_SESSION}."id_session" = ${TablesListAliases.LT_COURSE_SESSION}."id_session" AND ${TablesListAliases.LT_COURSEUSER_SESSION}."id_user" = ${TablesListAliases.CORE_USER}."idst"`);
                }
                select.push(`IFF(${TablesListAliases.LT_COURSE_SESSION}."evaluation_type" = 0 AND ${TablesListAliases.LT_COURSE_SESSION}."score_base" > 0,
                                TO_CHAR(TRUNCATE(COUNT(DISTINCT(
                                    IFF(${TablesListAliases.LT_COURSEUSER_SESSION}."evaluation_status" is null AND ${TablesListAliases.LT_COURSEUSER_SESSION}."evaluation_date" is not null,
                                    ${TablesListAliases.LT_COURSEUSER_SESSION}."id_user", null)))
                                )),
                             '') AS ${this.renderStringInQuerySelect(translations[FieldsList.SESSION_EVALUATION_STATUS_NOT_SET])}`);
                groupBy.push(`${TablesListAliases.LT_COURSE_SESSION}."evaluation_type"`);
                groupBy.push(`${TablesListAliases.LT_COURSE_SESSION}."score_base"`);
                break;
            case FieldsList.SESSION_EVALUATION_STATUS_NOT_PASSED:
                if (!join.includes(joinedTables.LT_COURSEUSER_SESSION)) {
                    join.push(joinedTables.LT_COURSEUSER_SESSION);
                    from.push(`LEFT JOIN ${TablesList.LT_COURSEUSER_SESSION} AS ${TablesListAliases.LT_COURSEUSER_SESSION} ON ${TablesListAliases.LT_COURSEUSER_SESSION}."id_session" = ${TablesListAliases.LT_COURSE_SESSION}."id_session" AND ${TablesListAliases.LT_COURSEUSER_SESSION}."id_user" = ${TablesListAliases.CORE_USER}."idst"`);
                }
                select.push(`IFF(${TablesListAliases.LT_COURSE_SESSION}."evaluation_type" = 0 AND ${TablesListAliases.LT_COURSE_SESSION}."score_base" > 0,
                                TO_CHAR(TRUNCATE(
                                    IFF(${this.getSqlUserWithEvaluationSnowflake()} > 0,
                                    COUNT(DISTINCT(IFF(${TablesListAliases.LT_COURSEUSER_SESSION}."evaluation_status" = -1, ${TablesListAliases.LT_COURSEUSER_SESSION}."id_user", null))) * 100 / ${this.getSqlUserWithEvaluationSnowflake()} ,0)
                                )),
                             '') AS ${this.renderStringInQuerySelect(translations[FieldsList.SESSION_EVALUATION_STATUS_NOT_PASSED])}`);
                groupBy.push(`${TablesListAliases.LT_COURSE_SESSION}."evaluation_type"`);
                groupBy.push(`${TablesListAliases.LT_COURSE_SESSION}."score_base"`);
                break;
            default:
                return false;
        }
        return true;
    }

    protected querySelectMobileAppStatisticsFields(field: string, queryHelper: any): boolean {

        const {select, from, join, groupBy, translations, archivedSelect} = queryHelper;

            switch (field) {
                case FieldsList.STATS_COURSE_ACCESS_FROM_MOBILE:
                    groupBy.push(`${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}."numberofactionsgolearn"`);
                    if (!join.includes(joinedTables.LEARNING_TRACKSESSION_AGGREGATE)) {
                        join.push(joinedTables.LEARNING_TRACKSESSION_AGGREGATE);
                        from.push(`LEFT JOIN ${TablesList.LEARNING_TRACKSESSION_AGGREGATE} AS ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE} ON ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}."iduser" = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."iduser" AND ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}."idcourse" = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."idcourse"`);
                    }
                    select.push(`
                            IFF ( ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}."numberofactionsgolearn" != 0, ${this.renderStringInQueryCase(translations[FieldTranslation.YES])}, ${this.renderStringInQueryCase(translations[FieldTranslation.NO])})
                            AS ${this.renderStringInQuerySelect(translations[FieldsList.STATS_COURSE_ACCESS_FROM_MOBILE])}`);
                    archivedSelect.push(`NULL AS ${this.renderStringInQuerySelect(translations[FieldsList.STATS_COURSE_ACCESS_FROM_MOBILE])}`);
                    break;
                case FieldsList.STATS_PERCENTAGE_OF_COURSE_FROM_MOBILE:
                    groupBy.push(`${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}."totaltime"`);
                    groupBy.push(`${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}."totaltimegolearn"`);
                    if (!join.includes(joinedTables.LEARNING_TRACKSESSION_AGGREGATE)) {
                        join.push(joinedTables.LEARNING_TRACKSESSION_AGGREGATE);
                        from.push(`LEFT JOIN ${TablesList.LEARNING_TRACKSESSION_AGGREGATE} AS ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE} ON ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}."iduser" = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."iduser" AND ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}."idcourse" = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."idcourse"`);
                    }
                    select.push(`
                            TRUNCATE(IFF ( ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}."totaltime" > 0, (${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}."totaltimegolearn" * 100)/${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}."totaltime", 0))
                            AS ${this.renderStringInQuerySelect(translations[FieldsList.STATS_PERCENTAGE_OF_COURSE_FROM_MOBILE])}`);
                    archivedSelect.push(`NULL AS ${this.renderStringInQuerySelect(translations[FieldsList.STATS_PERCENTAGE_OF_COURSE_FROM_MOBILE])}`);
                    break;
                case FieldsList.STATS_TIME_SPENT_FROM_MOBILE:
                    groupBy.push(`${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}."totaltimegolearn"`);
                    if (!join.includes(joinedTables.LEARNING_TRACKSESSION_AGGREGATE)) {
                        join.push(joinedTables.LEARNING_TRACKSESSION_AGGREGATE);
                        from.push(`LEFT JOIN ${TablesList.LEARNING_TRACKSESSION_AGGREGATE} AS ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE} ON ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}."iduser" = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."iduser" AND ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}."idcourse" = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."idcourse"`);
                    }
                    select.push(`
                            IFF ( ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}."totaltimegolearn" IS NOT NULL,${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}."totaltimegolearn", 0)
                            AS ${this.renderStringInQuerySelect(translations[FieldsList.STATS_TIME_SPENT_FROM_MOBILE])}`);
                    archivedSelect.push(`NULL AS ${this.renderStringInQuerySelect(translations[FieldsList.STATS_TIME_SPENT_FROM_MOBILE])}`);
                    break;
                case FieldsList.STATS_ACCESS_FROM_MOBILE:
                    if (!join.includes(joinedTables.LEARNING_TRACKSESSION_AGGREGATE)) {
                        join.push(joinedTables.LEARNING_TRACKSESSION_AGGREGATE);
                        from.push(`LEFT JOIN ${TablesList.LEARNING_TRACKSESSION_AGGREGATE} AS ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE} ON ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}."iduser" = ${TablesListAliases.CORE_USER}."idst" AND ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}."idcourse" = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."idcourse"`);
                    }
                    select.push(`SUM(CASE WHEN ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}."usergolearn" > 0 THEN 1 ELSE 0 END) AS ${this.renderStringInQuerySelect(translations[FieldsList.STATS_ACCESS_FROM_MOBILE])}`);
                    break;
                case FieldsList.STATS_PERCENTAGE_ACCESS_FROM_MOBILE:
                    if (!join.includes(joinedTables.LEARNING_TRACKSESSION_AGGREGATE)) {
                        join.push(joinedTables.LEARNING_TRACKSESSION_AGGREGATE);
                        from.push(`LEFT JOIN ${TablesList.LEARNING_TRACKSESSION_AGGREGATE} AS ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE} ON ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}."iduser" = ${TablesListAliases.CORE_USER}."idst" AND ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}."idcourse" = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."idcourse"`);
                    }
                    select.push(`TRUNCATE(SUM(CASE WHEN ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}."usergolearn" > 0 THEN 1 ELSE 0 END) * 100 / COUNT(DISTINCT(${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."iduser"))) AS ${this.renderStringInQuerySelect(translations[FieldsList.STATS_PERCENTAGE_ACCESS_FROM_MOBILE])}`);
                    break;
                default:
                    return false;
            }
        return true;
    }

    protected querySelectFlowStatisticsFields(field: string, queryHelper: any): boolean {
        if (!this.session.platform.checkPluginFlowEnabled()) return false;
        const {select, from, join, groupBy, translations, archivedSelect} = queryHelper;

            switch (field) {
                case FieldsList.STATS_USER_FLOW_YES_NO:
                    groupBy.push(`${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}."numberofactionsflow"`);
                    if (!join.includes(joinedTables.LEARNING_TRACKSESSION_AGGREGATE)) {
                        join.push(joinedTables.LEARNING_TRACKSESSION_AGGREGATE);
                        from.push(`LEFT JOIN ${TablesList.LEARNING_TRACKSESSION_AGGREGATE} AS ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE} ON ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}."iduser" = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."iduser" AND ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}."idcourse" = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."idcourse"`);
                    }
                    select.push(`
                            IFF ( ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}."numberofactionsflow" != 0, ${this.renderStringInQueryCase(translations[FieldTranslation.YES])}, ${this.renderStringInQueryCase(translations[FieldTranslation.NO])})
                            AS ${this.renderStringInQuerySelect(translations[FieldsList.STATS_USER_FLOW_YES_NO])}`);
                    archivedSelect.push(`'' AS ${this.renderStringInQuerySelect(translations[FieldsList.STATS_USER_FLOW_YES_NO])}`);
                    break;
                case FieldsList.STATS_USER_COURSE_FLOW_PERCENTAGE:
                    groupBy.push(`${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}."totaltime"`);
                    groupBy.push(`${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}."totaltimeflow"`);
                    if (!join.includes(joinedTables.LEARNING_TRACKSESSION_AGGREGATE)) {
                        join.push(joinedTables.LEARNING_TRACKSESSION_AGGREGATE);
                        from.push(`LEFT JOIN ${TablesList.LEARNING_TRACKSESSION_AGGREGATE} AS ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE} ON ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}."iduser" = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."iduser" AND ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}."idcourse" = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."idcourse"`);
                    }
                    select.push(`
                            TRUNCATE(IFF ( ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}."totaltime" > 0, (${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}."totaltimeflow" * 100)/${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}."totaltime", 0)
                            ) AS ${this.renderStringInQuerySelect(translations[FieldsList.STATS_USER_COURSE_FLOW_PERCENTAGE])}`);
                    archivedSelect.push(`NULL AS ${this.renderStringInQuerySelect(translations[FieldsList.STATS_USER_COURSE_FLOW_PERCENTAGE])}`);
                    break;
                case FieldsList.STATS_USER_COURSE_TIME_SPENT_BY_FLOW:
                    groupBy.push(`${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}."totaltimeflow"`);
                    if (!join.includes(joinedTables.LEARNING_TRACKSESSION_AGGREGATE)) {
                        join.push(joinedTables.LEARNING_TRACKSESSION_AGGREGATE);
                        from.push(`LEFT JOIN ${TablesList.LEARNING_TRACKSESSION_AGGREGATE} AS ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE} ON ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}."iduser" = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."iduser" AND ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}."idcourse" = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."idcourse"`);
                    }
                    select.push(`
                            IFF ( ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}."totaltimeflow" IS NOT NULL,${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}."totaltimeflow", 0)
                            AS ${this.renderStringInQuerySelect(translations[FieldsList.STATS_USER_COURSE_TIME_SPENT_BY_FLOW])}`);
                    archivedSelect.push(`NULL AS ${this.renderStringInQuerySelect(translations[FieldsList.STATS_USER_COURSE_TIME_SPENT_BY_FLOW])}`);
                    break;
                case FieldsList.STATS_USER_FLOW:
                    if (!join.includes(joinedTables.LEARNING_TRACKSESSION_AGGREGATE)) {
                        join.push(joinedTables.LEARNING_TRACKSESSION_AGGREGATE);
                        from.push(`LEFT JOIN ${TablesList.LEARNING_TRACKSESSION_AGGREGATE} AS ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE} ON ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}."iduser" = ${TablesListAliases.CORE_USER}."idst" AND ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}."idcourse" = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."idcourse"`);
                    }
                    select.push(`SUM(CASE WHEN ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}."userflow" > 0 THEN 1 ELSE 0 END) AS ${this.renderStringInQuerySelect(translations[FieldsList.STATS_USER_FLOW])}`);
                    break;
                case FieldsList.STATS_USER_FLOW_PERCENTAGE:
                    if (!join.includes(joinedTables.LEARNING_TRACKSESSION_AGGREGATE)) {
                        join.push(joinedTables.LEARNING_TRACKSESSION_AGGREGATE);
                        from.push(`LEFT JOIN ${TablesList.LEARNING_TRACKSESSION_AGGREGATE} AS ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE} ON ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}."iduser" = ${TablesListAliases.CORE_USER}."idst" AND ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}."idcourse" = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."idcourse"`);
                    }
                    select.push(`TRUNCATE(SUM(CASE WHEN ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}."userflow" > 0 THEN 1 ELSE 0 END) * 100 / COUNT(DISTINCT(${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."iduser"))) AS ${this.renderStringInQuerySelect(translations[FieldsList.STATS_USER_FLOW_PERCENTAGE])}`);
                    break;
                default:
                    return false;
            }
        return true;
    }

    protected querySelectFlowMsTeamsStatisticsFields(field: string, queryHelper: any): boolean {
        if (!this.session.platform.checkPluginFlowMsTeamsEnabled()) return false;
        const {select, from, join, groupBy, translations, archivedSelect} = queryHelper;

            switch (field) {
                case FieldsList.STATS_USER_FLOW_MS_TEAMS_YES_NO:
                    groupBy.push(`${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}."numberofactionsflowmsteams"`);
                    if (!join.includes(joinedTables.LEARNING_TRACKSESSION_AGGREGATE)) {
                        join.push(joinedTables.LEARNING_TRACKSESSION_AGGREGATE);
                        from.push(`LEFT JOIN ${TablesList.LEARNING_TRACKSESSION_AGGREGATE} AS ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE} ON ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}."iduser" = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."iduser" AND ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}."idcourse" = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."idcourse"`);
                    }
                    select.push(`
                                IFF ( ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}."numberofactionsflowmsteams" != 0, ${this.renderStringInQueryCase(translations[FieldTranslation.YES])}, ${this.renderStringInQueryCase(translations[FieldTranslation.NO])})
                                AS ${this.renderStringInQuerySelect(translations[FieldsList.STATS_USER_FLOW_MS_TEAMS_YES_NO])}`);
                    archivedSelect.push(`NULL AS ${this.renderStringInQuerySelect(translations[FieldsList.STATS_USER_FLOW_MS_TEAMS_YES_NO])}`);
                    break;
                case FieldsList.STATS_USER_COURSE_FLOW_MS_TEAMS_PERCENTAGE:
                    groupBy.push(`${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}."totaltime"`);
                    groupBy.push(`${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}."totaltimeflowmsteams"`);
                    if (!join.includes(joinedTables.LEARNING_TRACKSESSION_AGGREGATE)) {
                        join.push(joinedTables.LEARNING_TRACKSESSION_AGGREGATE);
                        from.push(`LEFT JOIN ${TablesList.LEARNING_TRACKSESSION_AGGREGATE} AS ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE} ON ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}."iduser" = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."iduser" AND ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}."idcourse" = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."idcourse"`);
                    }
                    select.push(`
                            TRUNCATE(
                            IFF ( ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}."totaltime" > 0, (${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}."totaltimeflowmsteams" * 100)/${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}."totaltime", 0))
                            AS ${this.renderStringInQuerySelect(translations[FieldsList.STATS_USER_COURSE_FLOW_MS_TEAMS_PERCENTAGE])}`);
                    archivedSelect.push(`NULL AS ${this.renderStringInQuerySelect(translations[FieldsList.STATS_USER_COURSE_FLOW_MS_TEAMS_PERCENTAGE])}`);
                    break;
                case FieldsList.STATS_USER_COURSE_TIME_SPENT_BY_FLOW_MS_TEAMS:
                    groupBy.push(`${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}."totaltimeflowmsteams"`);
                    if (!join.includes(joinedTables.LEARNING_TRACKSESSION_AGGREGATE)) {
                        join.push(joinedTables.LEARNING_TRACKSESSION_AGGREGATE);
                        from.push(`LEFT JOIN ${TablesList.LEARNING_TRACKSESSION_AGGREGATE} AS ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE} ON ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}."iduser" = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."iduser" AND ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}."idcourse" = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."idcourse"`);
                    }
                    select.push(`
                                IFF ( ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}."totaltimeflowmsteams" IS NOT NULL,${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}."totaltimeflowmsteams", 0)
                                AS ${this.renderStringInQuerySelect(translations[FieldsList.STATS_USER_COURSE_TIME_SPENT_BY_FLOW_MS_TEAMS])}`);
                    archivedSelect.push(`NULL AS ${this.renderStringInQuerySelect(translations[FieldsList.STATS_USER_COURSE_TIME_SPENT_BY_FLOW_MS_TEAMS])}`);
                    break;
                case FieldsList.STATS_USER_FLOW_MS_TEAMS:
                    if (!join.includes(joinedTables.LEARNING_TRACKSESSION_AGGREGATE)) {
                        join.push(joinedTables.LEARNING_TRACKSESSION_AGGREGATE);
                        from.push(`LEFT JOIN ${TablesList.LEARNING_TRACKSESSION_AGGREGATE} AS ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE} ON ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}."iduser" = ${TablesListAliases.CORE_USER}."idst" AND ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}."idcourse" = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."idcourse"`);
                    }
                    select.push(`SUM(CASE WHEN ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}."userflowmsteams" > 0 THEN 1 ELSE 0 END)
                            AS ${this.renderStringInQuerySelect(translations[FieldsList.STATS_USER_FLOW_MS_TEAMS])}`);
                    break;
                case FieldsList.STATS_USER_FLOW_MS_TEAMS_PERCENTAGE:
                    if (!join.includes(joinedTables.LEARNING_TRACKSESSION_AGGREGATE)) {
                        join.push(joinedTables.LEARNING_TRACKSESSION_AGGREGATE);
                        from.push(`LEFT JOIN ${TablesList.LEARNING_TRACKSESSION_AGGREGATE} AS ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE} ON ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}."iduser" = ${TablesListAliases.CORE_USER}."idst" AND ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}."idcourse" = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."idcourse"`);
                    }
                    select.push(`TRUNCATE(SUM(CASE WHEN ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}."userflowmsteams" > 0 THEN 1 ELSE 0 END) * 100 / COUNT(DISTINCT(${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."iduser")))
                            AS ${this.renderStringInQuerySelect(translations[FieldsList.STATS_USER_FLOW_MS_TEAMS_PERCENTAGE])}`);
                    break;
                default:
                    return false;
            }
        return true;
    }

    protected querySelectAssetStatisticsFields(field: string, queryHelper: any): boolean {
        const { select, from, join, groupBy, translations } = queryHelper;
        switch (field) {
            case FieldsList.ANSWERS:
                if (!join.includes(joinedTables.APP7020_ANSWER_AGGREGATE)) {
                    join.push(joinedTables.APP7020_ANSWER_AGGREGATE);
                    from.push(`LEFT JOIN ${TablesList.APP7020_ANSWER_AGGREGATE} AS ${TablesListAliases.APP7020_ANSWER_AGGREGATE}
                        ON ${TablesListAliases.APP7020_CONTENT}."id" = ${TablesListAliases.APP7020_ANSWER_AGGREGATE}."idcontent"`);
                }
                select.push(`
                    CASE WHEN ${TablesListAliases.APP7020_ANSWER_AGGREGATE}."count" IS NOT NULL
                         THEN ${TablesListAliases.APP7020_ANSWER_AGGREGATE}."count" ELSE 0
                    END AS ${this.renderStringInQuerySelect(translations[FieldsList.ANSWERS])}`);
                groupBy.push(`${TablesListAliases.APP7020_ANSWER_AGGREGATE}."count"`);
                break;
            case FieldsList.ANSWER_LIKES:
                if (!join.includes(joinedTables.APP7020_ANSWER_LIKE_AGGREGATE)) {
                    join.push(join.APP7020_ANSWER_LIKE_AGGREGATE);
                    from.push(`LEFT JOIN ${TablesList.APP7020_ANSWER_LIKE_AGGREGATE} AS ${TablesListAliases.APP7020_ANSWER_LIKE_AGGREGATE}
                        ON ${TablesListAliases.APP7020_CONTENT}."id" = ${TablesListAliases.APP7020_ANSWER_LIKE_AGGREGATE}."idcontent"`);
                }
                select.push(`
                    CASE WHEN ${TablesListAliases.APP7020_ANSWER_LIKE_AGGREGATE}."count" IS NOT NULL
                        THEN ${TablesListAliases.APP7020_ANSWER_LIKE_AGGREGATE}."count" ELSE 0
                    END AS ${this.renderStringInQuerySelect(translations[FieldsList.ANSWER_LIKES])}`);
                groupBy.push(`${TablesListAliases.APP7020_ANSWER_LIKE_AGGREGATE}."count"`);
                break;
            case FieldsList.ANSWER_DISLIKES:
                if (!join.includes(joinedTables.APP7020_ANSWER_DISLIKE_AGGREGATE)) {
                    join.push(joinedTables.APP7020_ANSWER_DISLIKE_AGGREGATE);
                    from.push(`LEFT JOIN ${TablesList.APP7020_ANSWER_DISLIKE_AGGREGATE} AS ${TablesListAliases.APP7020_ANSWER_DISLIKE_AGGREGATE}
                        ON ${TablesListAliases.APP7020_CONTENT}."id" = ${TablesListAliases.APP7020_ANSWER_DISLIKE_AGGREGATE}."idcontent"`);
                }
                select.push(`
                    CASE WHEN ${TablesListAliases.APP7020_ANSWER_DISLIKE_AGGREGATE}."count" IS NOT NULL
                        THEN ${TablesListAliases.APP7020_ANSWER_DISLIKE_AGGREGATE}."count" ELSE 0
                    END AS ${this.renderStringInQuerySelect(translations[FieldsList.ANSWER_DISLIKES])}`);
                groupBy.push(`${TablesListAliases.APP7020_ANSWER_DISLIKE_AGGREGATE}."count"`);
                break;
            case FieldsList.ASSET_RATING:
                if (!join.includes(joinedTables.APP7020_CONTENT_RATING)) {
                    join.push(joinedTables.APP7020_CONTENT_RATING);
                    from.push(`LEFT JOIN ${TablesList.APP7020_CONTENT_RATING} AS ${TablesListAliases.APP7020_CONTENT_RATING}
                        ON ${TablesListAliases.APP7020_CONTENT}."id" = ${TablesListAliases.APP7020_CONTENT_RATING}."idcontent"`);
                }
                select.push(`
                    CASE
                        WHEN COUNT(${TablesListAliases.APP7020_CONTENT_RATING}."id") > 0
                        THEN ROUND(CAST(SUM(${TablesListAliases.APP7020_CONTENT_RATING}."rating") AS DOUBLE) / CAST(COUNT(${TablesListAliases.APP7020_CONTENT_RATING}."id") AS DOUBLE), 2)
                        ELSE 0
                    END AS ${this.renderStringInQuerySelect(translations[FieldsList.ASSET_RATING])}`);
                break;
            case FieldsList.TOTAL_VIEWS:
                if (!join.includes(joinedTables.APP7020_CONTENT_HISTORY_TOTAL_VIEWS_AGGREGATE)) {
                    join.push(joinedTables.APP7020_CONTENT_HISTORY_TOTAL_VIEWS_AGGREGATE);
                    from.push(`LEFT JOIN ${TablesList.APP7020_CONTENT_HISTORY_TOTAL_VIEWS_AGGREGATE} AS ${TablesListAliases.APP7020_CONTENT_HISTORY_TOTAL_VIEWS_AGGREGATE}
                        ON ${TablesListAliases.APP7020_CONTENT}."id" = ${TablesListAliases.APP7020_CONTENT_HISTORY_TOTAL_VIEWS_AGGREGATE}."id"`);
                }
                select.push(`
                    CASE
                        WHEN ${TablesListAliases.APP7020_CONTENT_HISTORY_TOTAL_VIEWS_AGGREGATE}."totalviews" IS NOT NULL
                        THEN ${TablesListAliases.APP7020_CONTENT_HISTORY_TOTAL_VIEWS_AGGREGATE}."totalviews"
                        ELSE 0
                    END AS ${this.renderStringInQuerySelect(translations[FieldsList.TOTAL_VIEWS])}`);
                groupBy.push(`${TablesListAliases.APP7020_CONTENT_HISTORY_TOTAL_VIEWS_AGGREGATE}."totalviews"`);
                break;
            case FieldsList.BEST_ANSWERS:
                if (!join.includes(joinedTables.APP7020_BEST_ANSWER_AGGREGATE)) {
                    join.push(joinedTables.APP7020_BEST_ANSWER_AGGREGATE);
                    from.push(`LEFT JOIN ${TablesList.APP7020_BEST_ANSWER_AGGREGATE} AS ${TablesListAliases.APP7020_BEST_ANSWER_AGGREGATE}
                        ON ${TablesListAliases.APP7020_CONTENT}."id" = ${TablesListAliases.APP7020_BEST_ANSWER_AGGREGATE}."id"`);
                }
                select.push(`COUNT(DISTINCT(${TablesListAliases.APP7020_BEST_ANSWER_AGGREGATE}."id")) AS ${this.renderStringInQuerySelect(translations[FieldsList.BEST_ANSWERS])}`);
                break;
            case FieldsList.QUESTIONS:
                if (!join.includes(joinedTables.APP7020_QUESTION)) {
                    join.push(joinedTables.APP7020_QUESTION);
                    from.push(`LEFT JOIN ${TablesList.APP7020_QUESTION} AS ${TablesListAliases.APP7020_QUESTION}
                        ON ${TablesListAliases.APP7020_CONTENT}."id" = ${TablesListAliases.APP7020_QUESTION}."idcontent"`);
                }
                select.push(`COUNT(DISTINCT(${TablesListAliases.APP7020_QUESTION}."id")) AS ${this.renderStringInQuerySelect(translations[FieldsList.QUESTIONS])}`);
                break;
            case FieldsList.INVITED_PEOPLE:
                if (!join.includes(joinedTables.APP7020_INVITATIONS_AGGREGATE)) {
                    join.push(joinedTables.APP7020_INVITATIONS_AGGREGATE);
                    from.push(`LEFT JOIN ${TablesList.APP7020_INVITATIONS_AGGREGATE} AS ${TablesListAliases.APP7020_INVITATIONS_AGGREGATE}
                        ON ${TablesListAliases.APP7020_CONTENT}."id" = ${TablesListAliases.APP7020_INVITATIONS_AGGREGATE}."id"`);
                }
                select.push(`
                    CASE
                       WHEN ${TablesListAliases.APP7020_INVITATIONS_AGGREGATE}."count_invite_watch" IS NOT NULL
                       THEN ${TablesListAliases.APP7020_INVITATIONS_AGGREGATE}."count_invite_watch"
                       ELSE 0
                    END AS ${this.renderStringInQuerySelect(translations[FieldsList.INVITED_PEOPLE])}`);
                groupBy.push(`${TablesListAliases.APP7020_INVITATIONS_AGGREGATE}."count_invite_watch"`);
                break;
            case FieldsList.WATCHED:
                if (!join.includes(joinedTables.APP7020_CONTENT_HISTORY)) {
                    join.push(joinedTables.APP7020_CONTENT_HISTORY);
                    from.push(`LEFT JOIN ${TablesList.APP7020_CONTENT_HISTORY} AS ${TablesListAliases.APP7020_CONTENT_HISTORY}
                        ON ${TablesListAliases.APP7020_CONTENT}."id" = ${TablesListAliases.APP7020_CONTENT_HISTORY}."idcontent"`);
                }
                select.push(`
                    CASE
                        WHEN COUNT(DISTINCT(${TablesListAliases.APP7020_CONTENT_HISTORY}."id")) > 0 THEN ${this.renderStringInQueryCase(translations[FieldTranslation.YES])}
                        ELSE ${this.renderStringInQueryCase(translations[FieldTranslation.NO])}
                    END AS ${this.renderStringInQuerySelect(translations[FieldsList.WATCHED])}`);
                break;
            case FieldsList.NOT_WATCHED:
                if (!join.includes(joinedTables.APP7020_CONTENT_HISTORY)) {
                    join.push(joinedTables.APP7020_CONTENT_HISTORY);
                    from.push(`LEFT JOIN ${TablesList.APP7020_CONTENT_HISTORY} AS ${TablesListAliases.APP7020_CONTENT_HISTORY}
                        ON ${TablesListAliases.APP7020_CONTENT}."id" = ${TablesListAliases.APP7020_CONTENT_HISTORY}."idcontent"`);
                }
                select.push(`
                    CASE
                        WHEN COUNT(DISTINCT(${TablesListAliases.APP7020_CONTENT_HISTORY}."id")) > 0 THEN ${this.renderStringInQueryCase(translations[FieldTranslation.NO])}
                        ELSE ${this.renderStringInQueryCase(translations[FieldTranslation.YES])}
                    END AS ${this.renderStringInQuerySelect(translations[FieldsList.NOT_WATCHED])}`);
                break;
            case FieldsList.GLOBAL_WATCH_RATE:
                if (!join.includes(joinedTables.APP7020_INVITATIONS_AGGREGATE)) {
                    join.push(joinedTables.APP7020_INVITATIONS_AGGREGATE);
                    from.push(`LEFT JOIN ${TablesList.APP7020_INVITATIONS_AGGREGATE} AS ${TablesListAliases.APP7020_INVITATIONS_AGGREGATE}
                        ON ${TablesListAliases.APP7020_CONTENT}."id" = ${TablesListAliases.APP7020_INVITATIONS_AGGREGATE}."id"`);
                }
                if (!join.includes(joinedTables.APP7020_CONTENT_HISTORY_AGGREGATE)) {
                    join.push(joinedTables.APP7020_CONTENT_HISTORY_AGGREGATE);
                    from.push(`LEFT JOIN ${TablesList.APP7020_CONTENT_HISTORY_AGGREGATE} AS ${TablesListAliases.APP7020_CONTENT_HISTORY_AGGREGATE}
                        ON ${TablesListAliases.APP7020_CONTENT}."id" = ${TablesListAliases.APP7020_CONTENT_HISTORY_AGGREGATE}."id"`);
                }
                select.push(`
                        CONCAT(CASE
                            WHEN ${TablesListAliases.APP7020_INVITATIONS_AGGREGATE}."count_invite_watch" > 0
                            THEN CAST(ROUND(${TablesListAliases.APP7020_CONTENT_HISTORY_AGGREGATE}."views" * 100 / ${TablesListAliases.APP7020_INVITATIONS_AGGREGATE}."count_invite_watch") AS VARCHAR)
                            ELSE CAST(0 AS VARCHAR)
                        END, ' %') AS ${this.renderStringInQuerySelect(translations[FieldsList.GLOBAL_WATCH_RATE])}`);
                groupBy.push(`${TablesListAliases.APP7020_INVITATIONS_AGGREGATE}."count_invite_watch", ${TablesListAliases.APP7020_CONTENT_HISTORY_AGGREGATE}."views"`);
                break;
            case FieldsList.AVERAGE_REACTION_TIME:
                from.push(`LEFT JOIN ${TablesList.APP7020_INVITATIONS_AVERAGE_TIME} AS ${TablesListAliases.APP7020_INVITATIONS_AVERAGE_TIME}
                    ON ${TablesListAliases.APP7020_CONTENT}."id" = ${TablesListAliases.APP7020_INVITATIONS_AVERAGE_TIME}."idcontent"`);

                select.push(`${TablesListAliases.APP7020_INVITATIONS_AVERAGE_TIME}."reactiontime" AS ${this.renderStringInQuerySelect(translations[FieldsList.AVERAGE_REACTION_TIME])}`);
                groupBy.push(`${TablesListAliases.APP7020_INVITATIONS_AVERAGE_TIME}."reactiontime"`);
                break;
            default:
                return false;
        }

        return true;
    }

    protected querySelectCertificationFields(field: string, queryHelper: any): boolean {

        const {select, from, translations, groupBy} = queryHelper;

        switch (field) {
            // Certification fields
            case FieldsList.CERTIFICATION_TITLE:
                select.push(`${TablesListAliases.CERTIFICATION}."title" AS ${this.renderStringInQuerySelect(translations[FieldsList.CERTIFICATION_TITLE])}`);
                groupBy.push(`${TablesListAliases.CERTIFICATION}."title"`);
                break;
            case FieldsList.CERTIFICATION_CODE:
                select.push(`${TablesListAliases.CERTIFICATION}."code" AS ${this.renderStringInQuerySelect(translations[FieldsList.CERTIFICATION_CODE])}`);
                groupBy.push(`${TablesListAliases.CERTIFICATION}."code"`);
                break;
            case FieldsList.CERTIFICATION_DESCRIPTION:
                select.push(`${TablesListAliases.CERTIFICATION}."description" AS ${this.renderStringInQuerySelect(translations[FieldsList.CERTIFICATION_DESCRIPTION])}`);
                groupBy.push(`${TablesListAliases.CERTIFICATION}."description"`);
                break;
            case FieldsList.CERTIFICATION_DURATION:
                select.push(`
                        CASE
                            WHEN ${TablesListAliases.CERTIFICATION}."duration" = 0 THEN ${this.renderStringInQueryCase(FieldTranslation.NEVER)}
                            WHEN ${TablesListAliases.CERTIFICATION}."duration_unit" = ${this.renderStringInQueryCase('day')} THEN CONCAT(CAST(${TablesListAliases.CERTIFICATION}."duration" AS VARCHAR), ' ', ${this.renderStringInQueryCase(FieldTranslation.DAYS)})
                            WHEN ${TablesListAliases.CERTIFICATION}."duration_unit" = ${this.renderStringInQueryCase('week')} THEN CONCAT(CAST(${TablesListAliases.CERTIFICATION}."duration" AS VARCHAR), ' ', ${this.renderStringInQueryCase(FieldTranslation.WEEKS)})
                            WHEN ${TablesListAliases.CERTIFICATION}."duration_unit" = ${this.renderStringInQueryCase('month')} THEN CONCAT(CAST(${TablesListAliases.CERTIFICATION}."duration" AS VARCHAR), ' ', ${this.renderStringInQueryCase(FieldTranslation.MONTHS)})
                            WHEN ${TablesListAliases.CERTIFICATION}."duration_unit" = ${this.renderStringInQueryCase('year')} THEN CONCAT(CAST(${TablesListAliases.CERTIFICATION}."duration" AS VARCHAR), ' ', ${this.renderStringInQueryCase(FieldTranslation.YEARS)})
                            ELSE NULL
                        END AS ${this.renderStringInQuerySelect(translations[FieldsList.CERTIFICATION_DURATION])}`);
                groupBy.push(`${TablesListAliases.CERTIFICATION}."duration"`);
                groupBy.push(`${TablesListAliases.CERTIFICATION}."duration_unit"`);
                break;
            case FieldsList.CERTIFICATION_COMPLETED_ACTIVITY:
                from.push(`LEFT JOIN ${TablesList.LEARNING_COURSE} AS ${TablesListAliases.LEARNING_COURSE} ON ${TablesListAliases.LEARNING_COURSE}."idcourse" = ${TablesListAliases.CERTIFICATION_ITEM}."id_item"`);
                from.push(`LEFT JOIN ${TablesList.LEARNING_COURSEPATH} AS ${TablesListAliases.LEARNING_COURSEPATH} ON ${TablesListAliases.LEARNING_COURSEPATH}."id_path" = ${TablesListAliases.CERTIFICATION_ITEM}."id_item"`);
                if (this.session.platform.checkPluginTranscriptEnabled()) {
                    from.push(`LEFT JOIN ${TablesList.TRANSCRIPTS_RECORD} AS ${TablesListAliases.TRANSCRIPTS_RECORD} ON ${TablesListAliases.TRANSCRIPTS_RECORD}."id_record" = ${TablesListAliases.CERTIFICATION_ITEM}."id_item"`);
                    from.push(`LEFT JOIN ${TablesList.TRANSCRIPTS_COURSE} AS ${TablesListAliases.TRANSCRIPTS_COURSE} ON ${TablesListAliases.TRANSCRIPTS_COURSE}."id" = ${TablesListAliases.TRANSCRIPTS_RECORD}."course_id"`);
                }
                select.push(`
                            CASE
                                WHEN ${TablesListAliases.CERTIFICATION_ITEM}."item_type" = ${this.renderStringInQueryCase('plan')} THEN ${TablesListAliases.LEARNING_COURSEPATH}."path_name"
                                WHEN ${TablesListAliases.CERTIFICATION_ITEM}."item_type" = ${this.renderStringInQueryCase('course')} THEN  ${TablesListAliases.LEARNING_COURSE}."name"
                                ` + (this.session.platform.checkPluginTranscriptEnabled() ? `WHEN ${TablesListAliases.CERTIFICATION_ITEM}."item_type" = ${this.renderStringInQueryCase('transcript')} THEN CASE WHEN ${TablesListAliases.TRANSCRIPTS_RECORD}."course_name" <> '' THEN ${TablesListAliases.TRANSCRIPTS_RECORD}."course_name" ELSE ${TablesListAliases.TRANSCRIPTS_COURSE}."course_name" END` : '') + `
                                ELSE ''
                            END AS ${this.renderStringInQuerySelect(translations[FieldsList.CERTIFICATION_COMPLETED_ACTIVITY])}`);
                break;
            case FieldsList.CERTIFICATION_ISSUED_ON:
                select.push(`${this.queryConvertTimezone(`${TablesListAliases.CERTIFICATION_USER}."on_datetime"`)} AS ${this.renderStringInQuerySelect(translations[FieldsList.CERTIFICATION_ISSUED_ON])}`);
                break;
            case FieldsList.CERTIFICATION_TO_RENEW_IN:
                select.push(`${this.queryConvertTimezone(`${TablesListAliases.CERTIFICATION_USER}."expire_at"`)} AS ${this.renderStringInQuerySelect(translations[FieldsList.CERTIFICATION_TO_RENEW_IN])}`);
                break;
            case FieldsList.CERTIFICATION_STATUS:
                select.push(`
                            CASE
                                WHEN ${TablesListAliases.CERTIFICATION_USER}."archived" = 0 AND ${TablesListAliases.CERTIFICATION_USER}."on_datetime" <= CURRENT_TIMESTAMP AND (${TablesListAliases.CERTIFICATION_USER}."expire_at" > CURRENT_TIMESTAMP OR ${TablesListAliases.CERTIFICATION_USER}."expire_at" IS NULL)
                                  THEN ${this.renderStringInQueryCase(translations[FieldTranslation.CERTIFICATION_ACTIVE])}
                                WHEN ${TablesListAliases.CERTIFICATION_USER}."archived" = 0 AND ${TablesListAliases.CERTIFICATION_USER}."expire_at" < CURRENT_TIMESTAMP THEN ${this.renderStringInQueryCase(translations[FieldTranslation.CERTIFICATION_EXPIRED])}
                                WHEN ${TablesListAliases.CERTIFICATION_USER}."archived" = 1 THEN ${this.renderStringInQueryCase(translations[FieldTranslation.CERTIFICATION_ARCHIVED])}
                            END AS ${this.renderStringInQuerySelect(translations[FieldsList.CERTIFICATION_STATUS])}`);
                break;
            default:
                return false;
        }
        return true;
    }

    protected querySelectSurveyFields(field: string, queryHelper: any): boolean {

        const { select, from, join, groupBy, translations } = queryHelper;

        switch (field) {
            case FieldsList.SURVEY_TITLE:
                select.push(`${TablesListAliases.LEARNING_POLL}."title" AS ${this.renderStringInQuerySelect(translations[FieldsList.SURVEY_TITLE])}`);
                groupBy.push(`${TablesListAliases.LEARNING_POLL}."title"`);
                break;
            case FieldsList.SURVEY_ID:
                select.push(`${TablesListAliases.LEARNING_POLL}."id_poll" AS ${this.renderStringInQuerySelect(translations[FieldsList.SURVEY_ID])}`);
                groupBy.push(`${TablesListAliases.LEARNING_POLL}."id_poll"`);
                break;
            case FieldsList.SURVEY_DESCRIPTION:
                select.push(`${TablesListAliases.LEARNING_POLL}."description" AS ${this.renderStringInQuerySelect(translations[FieldsList.SURVEY_DESCRIPTION])}`);
                groupBy.push(`${TablesListAliases.LEARNING_POLL}."description"`);
                break;
            case FieldsList.SURVEY_TRACKING_TYPE:
                if (!join.includes(joinedTables.LEARNING_REPOSITORY_OBJECT)) {
                    join.push(joinedTables.LEARNING_REPOSITORY_OBJECT);
                    from.push(`LEFT JOIN ${TablesList.LEARNING_REPOSITORY_OBJECT} AS ${TablesListAliases.LEARNING_REPOSITORY_OBJECT} ON ${TablesListAliases.LEARNING_REPOSITORY_OBJECT}."id_object" = ${TablesListAliases.LEARNING_ORGANIZATION}."id_object"`);
                }
                select.push(`CASE
                        WHEN ${TablesListAliases.LEARNING_REPOSITORY_OBJECT}."shared_tracking" > 0
                            THEN ${this.renderStringInQueryCase(translations[FieldTranslation.SHARED_TRACKING])}
                            ELSE ${this.renderStringInQueryCase(translations[FieldTranslation.LOCAL_TRACKING])}
                        END AS ${this.renderStringInQuerySelect(translations[FieldsList.SURVEY_TRACKING_TYPE])}`);
                groupBy.push(`${TablesListAliases.LEARNING_REPOSITORY_OBJECT}."shared_tracking"`);
                break;
            default:
                return false;
        }
        return true;
    }

    protected querySelectSurveyQuestionAnswerFields(field: string, queryHelper: any): boolean {

        const { select, from, join, groupBy, translations } = queryHelper;

        switch (field) {
            case FieldsList.SURVEY_COMPLETION_ID:
                select.push(`${TablesListAliases.LEARNING_POLLTRACK}."id_track" AS ${this.renderStringInQuerySelect(translations[FieldsList.SURVEY_COMPLETION_ID])}`);
                groupBy.push(`${TablesListAliases.LEARNING_POLLTRACK}."id_track"`);
                break;
            case FieldsList.ANSWER_USER:
                if (!join.includes(joinedTables.LEARNING_POLLQUEST_ANSWER)) {
                    join.push(joinedTables.LEARNING_POLLQUEST_ANSWER);
                    from.push(`LEFT JOIN ${TablesList.LEARNING_POLLQUEST_ANSWER} AS ${TablesListAliases.LEARNING_POLLQUEST_ANSWER} ON ${TablesListAliases.LEARNING_POLLQUEST_ANSWER}."id_answer" = ${TablesListAliases.LEARNING_POLLTRACK_ANSWER}."id_answer"
                        AND ${TablesListAliases.LEARNING_POLLQUEST}."type_quest" IN ('${LOQuestTypes.CHOICE}', '${LOQuestTypes.CHOICE_MULTIPLE}', '${LOQuestTypes.INLINE_CHOICE}')`);
                }
                if (!join.includes(joinedTables.LEARNING_POLL_LIKERT_SCALE)) {
                    join.push(joinedTables.LEARNING_POLL_LIKERT_SCALE);
                    from.push(`LEFT JOIN ${TablesList.LEARNING_POLL_LIKERT_SCALE} AS ${TablesListAliases.LEARNING_POLL_LIKERT_SCALE} ON ${TablesListAliases.LEARNING_POLL_LIKERT_SCALE}."id_poll" = ${TablesListAliases.LEARNING_POLLTRACK}."id_poll"
                        AND ${TablesListAliases.LEARNING_POLLQUEST}."type_quest" = '${LOQuestTypes.LIKERT_SCALE}'
                        AND ${TablesListAliases.LEARNING_POLLTRACK_ANSWER}."id_answer" = ${TablesListAliases.LEARNING_POLLQUEST}."id_answer"
                        AND ${TablesListAliases.LEARNING_POLL_LIKERT_SCALE}."id" = try_cast(${TablesListAliases.LEARNING_POLLTRACK_ANSWER}."more_info" as integer)`);
                }
                select.push(`
                        CASE
                            WHEN ${TablesListAliases.LEARNING_POLLQUEST}."type_quest" = '${LOQuestTypes.CHOICE}' THEN MAX(${TablesListAliases.LEARNING_POLLQUEST_ANSWER}."answer")
                            WHEN ${TablesListAliases.LEARNING_POLLQUEST}."type_quest" = '${LOQuestTypes.CHOICE_MULTIPLE}' THEN LISTAGG(DISTINCT(${TablesListAliases.LEARNING_POLLQUEST_ANSWER}."answer"), ', ')
                            WHEN ${TablesListAliases.LEARNING_POLLQUEST}."type_quest" = '${LOQuestTypes.INLINE_CHOICE}' THEN MAX(${TablesListAliases.LEARNING_POLLQUEST_ANSWER}."answer")
                            WHEN ${TablesListAliases.LEARNING_POLLQUEST}."type_quest" = '${LOQuestTypes.EXTENDED_TEXT}' THEN MAX(${TablesListAliases.LEARNING_POLLTRACK_ANSWER}."more_info")
                            WHEN ${TablesListAliases.LEARNING_POLLQUEST}."type_quest" = '${LOQuestTypes.LIKERT_SCALE}' THEN MAX(${TablesListAliases.LEARNING_POLL_LIKERT_SCALE}."title")
                            ELSE NULL
                        END AS ${this.renderStringInQuerySelect(translations[FieldsList.ANSWER_USER])}`);
                groupBy.push(`${TablesListAliases.LEARNING_POLLQUEST}."type_quest"`);
                break;
            case FieldsList.SURVEY_COMPLETION_DATE:
                select.push(`${this.queryConvertTimezone(`${TablesListAliases.LEARNING_COMMONTRACK}."last_complete"`)} AS ${this.renderStringInQuerySelect(translations[FieldsList.SURVEY_COMPLETION_DATE])}`);
                groupBy.push(`${TablesListAliases.LEARNING_COMMONTRACK}."last_complete"`);
                break;
            case FieldsList.QUESTION:
                select.push(`${TablesListAliases.LEARNING_POLLQUEST}."title_quest" AS ${this.renderStringInQuerySelect(translations[FieldsList.QUESTION])}`);
                groupBy.push(`${TablesListAliases.LEARNING_POLLQUEST}."title_quest"`);
                break;
            case FieldsList.QUESTION_ID:
                select.push(`${TablesListAliases.LEARNING_POLLQUEST}."id_quest" AS ${this.renderStringInQuerySelect(translations[FieldsList.QUESTION_ID])}`);
                groupBy.push(`${TablesListAliases.LEARNING_POLLQUEST}."id_quest"`);
                break;
            case FieldsList.QUESTION_MANDATORY:
                select.push(`
                    CASE WHEN CAST(${TablesListAliases.LEARNING_POLLQUEST}."mandatory" AS INTEGER) > 0
                        THEN ${this.renderStringInQueryCase(translations[FieldTranslation.YES])}
                        ELSE ${this.renderStringInQueryCase(translations[FieldTranslation.NO])}
                    END AS ${this.renderStringInQuerySelect(translations[FieldsList.QUESTION_MANDATORY])}`);
                groupBy.push(`${TablesListAliases.LEARNING_POLLQUEST}."mandatory"`);
                break;
            case FieldsList.QUESTION_TYPE:
                select.push(`CASE
                        WHEN ${TablesListAliases.LEARNING_POLLQUEST}."type_quest" = '${LOQuestTypes.CHOICE}' THEN ${this.renderStringInQueryCase(translations[FieldTranslation.CHOICE])}
                        WHEN ${TablesListAliases.LEARNING_POLLQUEST}."type_quest" = '${LOQuestTypes.CHOICE_MULTIPLE}' THEN ${this.renderStringInQueryCase(translations[FieldTranslation.CHOICE_MULTIPLE])}
                        WHEN ${TablesListAliases.LEARNING_POLLQUEST}."type_quest" = '${LOQuestTypes.INLINE_CHOICE}' THEN ${this.renderStringInQueryCase(translations[FieldTranslation.INLINE_CHOICE])}
                        WHEN ${TablesListAliases.LEARNING_POLLQUEST}."type_quest" = '${LOQuestTypes.EXTENDED_TEXT}' THEN ${this.renderStringInQueryCase(translations[FieldTranslation.EXTENDED_TEXT])}
                        WHEN ${TablesListAliases.LEARNING_POLLQUEST}."type_quest" = '${LOQuestTypes.LIKERT_SCALE}' THEN ${this.renderStringInQueryCase(translations[FieldTranslation.LIKERT_SCALE])}
                        ELSE ${TablesListAliases.LEARNING_POLLQUEST}."type_quest"
                    END AS ${this.renderStringInQuerySelect(translations[FieldsList.QUESTION_TYPE])}
                `);
                groupBy.push(`${TablesListAliases.LEARNING_POLLQUEST}."type_quest"`);
                break;
            default:
                return false;
        }
        return true;
    }

    protected queryBuilderAdditionalFieldValue(typeField = 'lcf."type"', valueField = 'lcfv."field_value"'): string {
        return `CASE
                WHEN ${typeField} = 'date' AND IFF(TRY_TO_DATE(${valueField}, 'YYYY-MM-DD') IS NOT NULL,FALSE,TRUE)
                    THEN
                        CASE
                            WHEN IFF(TRY_TO_NUMBER(${valueField}) IS NOT NULL,TRUE,FALSE)
                                 THEN TO_DATE(DATEADD(day, ${valueField}, '1970-01-01'))::string
                            ELSE NULL
                        END
                ELSE ${valueField} END AS "field_value"`
    }

    protected additionalUserFieldQueryWith(additionalFieldsId: any[]) {
        return `${TablesList.USERS_ADDITIONAL_FIELDS_TRANSLATIONS} AS (
                SELECT cufv."id_field", cufv."id_user", ${this.queryBuilderAdditionalFieldValue('cuf."type"', 'cufv."field_value"')}
                FROM ${TablesList.CORE_USER_FIELD_VALUE} AS cufv
                INNER JOIN ${TablesList.CORE_USER_FIELD} AS cuf ON cuf."id_field" = cufv."id_field" AND cuf."type" NOT IN ('country', 'dropdown')
                WHERE cufv."id_field" IN (${additionalFieldsId.join(', ')})
                UNION
                SELECT cufv."id_field", cufv."id_user", cc."name_country" AS "field_value"
                FROM ${TablesList.CORE_USER_FIELD_VALUE} AS cufv
                INNER JOIN ${TablesList.CORE_USER_FIELD} AS cuf ON cuf."id_field" = cufv."id_field" AND cuf."type" = 'country'
                LEFT JOIN ${TablesList.CORE_COUNTRY} AS cc ON cc."id_country" = cufv."field_value"
                WHERE cufv."id_field" IN (${additionalFieldsId.join(', ')})
                UNION
                SELECT cufv."id_field", cufv."id_user", cufdt."translation" AS "field_value"
                FROM ${TablesList.CORE_USER_FIELD_VALUE} AS cufv
                INNER JOIN ${TablesList.CORE_USER_FIELD} AS cuf ON cuf."id_field" = cufv."id_field" AND cuf."type" = 'dropdown'
                LEFT JOIN ${TablesList.CORE_USER_FIELD_DROPDOWN_TRANSLATIONS} AS cufdt ON cufdt."id_option" = cufv."field_value" AND cufdt."lang_code" = '${this.session.user.getLang()}'
                WHERE cufv."id_field" IN (${additionalFieldsId.join(', ')})
            )`;
    }

    protected queryWithUserAdditionalFields(field: string, queryHelper: any, userExtraFields: any): boolean {
        if (!this.isUserExtraField(field)) {
            return false;
        }
        const {select, archivedSelect, from, join, groupBy, userAdditionalFieldsSelect, userAdditionalFieldsFrom, userAdditionalFieldsId, translations} = queryHelper;

        const fieldId = parseInt(field.replace('user_extrafield_', ''), 10);
        const fields = userExtraFields.data.items.filter((x) => parseInt(x.id, 10) === fieldId);
        if (!fields?.length) {
            return false;
        }
        const userField = fields[0];
        let isTheJoinAlreadyHandled = true;
        if (!join.includes(joinedTables.CORE_USER_FIELD_VALUE)) {
            join.push(joinedTables.CORE_USER_FIELD_VALUE);
            let joinField = `${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."iduser"`;
            switch (this.info.type) {
                case ReportsTypes.USERS:
                case ReportsTypes.USER_CONTRIBUTIONS:
                case ReportsTypes.VIEWER_ASSET_DETAILS:
                    joinField = `${TablesListAliases.CORE_USER}."idst"`;
                    break;
                case ReportsTypes.USERS_CERTIFICATIONS:
                    joinField = `${TablesListAliases.CERTIFICATION_USER}."id_user"`;
                    break;
                case ReportsTypes.USERS_BADGES:
                    joinField = `${TablesListAliases.GAMIFICATION_ASSIGNED_BADGES}."id_user"`;
                    break;
                case ReportsTypes.USERS_EXTERNAL_TRAINING:
                    joinField = `${TablesListAliases.TRANSCRIPTS_RECORD}."id_user"`;
                    break;
                case ReportsTypes.USERS_LP:
                    joinField = `${TablesListAliases.LEARNING_COURSEPATH_USER}."iduser"`;
                    break;
                case ReportsTypes.ECOMMERCE_TRANSACTION:
                    joinField = `${TablesListAliases.ECOMMERCE_TRANSACTION}."id_user"`;
                    break;
            }
            from.push(`LEFT JOIN ${TablesList.CORE_USER_FIELD_VALUE_WITH} AS ${TablesListAliases.CORE_USER_FIELD_VALUE} ON ${TablesListAliases.CORE_USER_FIELD_VALUE}."id_user" = ${joinField}`);
            isTheJoinAlreadyHandled = false;
        }
        switch (userField.type) {
            case AdditionalFieldsTypes.CodiceFiscale:
            case AdditionalFieldsTypes.FreeText:
            case AdditionalFieldsTypes.GMail:
            case AdditionalFieldsTypes.ICQ:
            case AdditionalFieldsTypes.MSN:
            case AdditionalFieldsTypes.Skype:
            case AdditionalFieldsTypes.Textfield:
            case AdditionalFieldsTypes.Yahoo:
            case AdditionalFieldsTypes.Upload:
            case AdditionalFieldsTypes.Dropdown:
            case AdditionalFieldsTypes.Country:
                select.push(`${TablesListAliases.CORE_USER_FIELD_VALUE}."field_${fieldId}" AS ${this.renderStringInQuerySelect(userField.title)}`);
                archivedSelect.push(`NULL AS ${this.renderStringInQuerySelect(userField.title)}`);
                userAdditionalFieldsSelect.push(`"field_${fieldId}"`);
                userAdditionalFieldsFrom.push(`"field_${fieldId}"`);
                userAdditionalFieldsId.push(fieldId);
                groupBy.push(`${TablesListAliases.CORE_USER_FIELD_VALUE}."field_${fieldId}"`);
                break;
            case AdditionalFieldsTypes.Date:
                select.push(`TO_CHAR(${TablesListAliases.CORE_USER_FIELD_VALUE}."field_${fieldId}", 'YYYY-MM-DD') AS ${this.renderStringInQuerySelect(userField.title)}`);
                archivedSelect.push(`NULL AS ${this.renderStringInQuerySelect(userField.title)}`);
                userAdditionalFieldsSelect.push(`"field_${fieldId}"::date AS "field_${fieldId}"`);
                userAdditionalFieldsFrom.push(`"field_${fieldId}"`);
                userAdditionalFieldsId.push(fieldId);
                groupBy.push(`${TablesListAliases.CORE_USER_FIELD_VALUE}."field_${fieldId}"`);
                break;
            case AdditionalFieldsTypes.YesNo:
                select.push(`CASE
                                 WHEN ${TablesListAliases.CORE_USER_FIELD_VALUE}."field_${fieldId}" = 1 THEN ${this.renderStringInQueryCase(translations[FieldTranslation.YES])}
                                 WHEN ${TablesListAliases.CORE_USER_FIELD_VALUE}."field_${fieldId}" = 2 THEN ${this.renderStringInQueryCase(translations[FieldTranslation.NO])}
                                 ELSE ''
                             END AS ${this.renderStringInQuerySelect(userField.title)}`);
                archivedSelect.push(`'' AS ${this.renderStringInQuerySelect(userField.title)}`);
                userAdditionalFieldsSelect.push(`"field_${fieldId}"::number AS "field_${fieldId}"`);
                userAdditionalFieldsFrom.push(`"field_${fieldId}"`);
                userAdditionalFieldsId.push(fieldId);
                groupBy.push(`${TablesListAliases.CORE_USER_FIELD_VALUE}."field_${fieldId}"`);
                break;
            default:
                if (!isTheJoinAlreadyHandled) {
                    join.pop();
                    from.pop();
                }
                return false;
        }
        return true;
    }

    protected additionalCourseFieldQueryWith(additionalFieldsId: any[]) {
        return `${TablesList.COURSES_ADDITIONAL_FIELDS_TRANSLATIONS} AS (
                SELECT lcfv."id_field", lcfv."id_course", ${this.queryBuilderAdditionalFieldValue()}
                FROM ${TablesList.LEARNING_COURSE_FIELD_VALUE} AS lcfv
                INNER JOIN ${TablesList.LEARNING_COURSE_FIELD} AS lcf ON lcf."id_field" = lcfv."id_field" AND lcf."type" <> 'dropdown'
                WHERE lcfv."id_field" IN (${additionalFieldsId.join(', ')})
                UNION
                SELECT lcfv."id_field", lcfv."id_course", lcfdt."translation" AS "field_value"
                FROM ${TablesList.LEARNING_COURSE_FIELD_VALUE} AS lcfv
                INNER JOIN ${TablesList.LEARNING_COURSE_FIELD} AS lcf ON lcf."id_field" = lcfv."id_field" AND lcf."type" = 'dropdown'
                LEFT JOIN ${TablesList.LEARNING_COURSE_FIELD_DROPDOWN_TRANSLATIONS} AS lcfdt ON lcfdt."id_option" = lcfv."field_value" AND lcfdt."lang_code" = '${this.session.user.getLang()}'
                WHERE lcfv."id_field" IN (${additionalFieldsId.join(', ')})
            )`;
    }

    protected queryWithCourseAdditionalFields(field: string, queryHelper: any, courseExtraFields: any): boolean {
        if (!this.isCourseExtraField(field)) {
            return false;
        }
        const {select, archivedSelect, from, join, groupBy, courseAdditionalFieldsSelect, courseAdditionalFieldsFrom, courseAdditionalFieldsId, translations} = queryHelper;

        const fieldId = parseInt(field.replace('course_extrafield_', ''), 10);
        const fields = courseExtraFields.data.items.filter((field) => field.id === fieldId);
        if (!fields?.length) {
            return false;
        }

        const courseField = fields[0];
        let isTheJoinAlreadyHandled = true;

        const isToggleUsersLearningPlansReportEnhancement = this.info.type === ReportsTypes.USERS_LP && this.session.platform.isToggleUsersLearningPlansReportEnhancement();
        if(isToggleUsersLearningPlansReportEnhancement){
            this.joinLpCoursesTables(join, from)
        }

        if (!join.includes(joinedTables.LEARNING_COURSE_FIELD_VALUE)) {
            join.push(joinedTables.LEARNING_COURSE_FIELD_VALUE);

            let joinConditionLearningCourseFieldValueWith = `${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."idcourse"`;
            if (isToggleUsersLearningPlansReportEnhancement) {
                joinConditionLearningCourseFieldValueWith =`${TablesListAliases.LEARNING_COURSE}."idcourse"`;
            }

            from.push(`LEFT JOIN ${TablesList.LEARNING_COURSE_FIELD_VALUE_WITH} AS ${TablesListAliases.LEARNING_COURSE_FIELD_VALUE}
                ON ${TablesListAliases.LEARNING_COURSE_FIELD_VALUE}."id_course" = ${joinConditionLearningCourseFieldValueWith}`);
            isTheJoinAlreadyHandled = false;
        }
        switch (courseField.type) {
            case AdditionalFieldsTypes.Textarea:
            case AdditionalFieldsTypes.Textfield:
            case AdditionalFieldsTypes.Dropdown:
                select.push(`${TablesListAliases.LEARNING_COURSE_FIELD_VALUE}."field_${fieldId}" AS ${this.renderStringInQuerySelect(courseField.name.value)}`);
                archivedSelect.push(`NULL AS ${this.renderStringInQuerySelect(courseField.name.value)}`);
                courseAdditionalFieldsSelect.push(`"field_${fieldId}"`);
                courseAdditionalFieldsFrom.push(`"field_${fieldId}"`);
                courseAdditionalFieldsId.push(fieldId);
                groupBy.push(`${TablesListAliases.LEARNING_COURSE_FIELD_VALUE}."field_${fieldId}"`);
                break;
            case AdditionalFieldsTypes.Date:
                select.push(`TO_CHAR(${TablesListAliases.LEARNING_COURSE_FIELD_VALUE}."field_${fieldId}", 'YYYY-MM-DD') AS ${this.renderStringInQuerySelect(courseField.name.value)}`);
                archivedSelect.push(`NULL AS ${this.renderStringInQuerySelect(courseField.name.value)}`);
                courseAdditionalFieldsSelect.push(`"field_${fieldId}"::date AS "field_${fieldId}"`);
                courseAdditionalFieldsFrom.push(`"field_${fieldId}"`);
                courseAdditionalFieldsId.push(fieldId);
                groupBy.push(`${TablesListAliases.LEARNING_COURSE_FIELD_VALUE}."field_${fieldId}"`);
                break;
            case AdditionalFieldsTypes.YesNo:
                select.push(`
                                CASE
                                    WHEN ${TablesListAliases.LEARNING_COURSE_FIELD_VALUE}."field_${fieldId}" = 1 THEN ${this.renderStringInQueryCase(translations[FieldTranslation.YES])}
                                    ELSE ${this.renderStringInQueryCase(translations[FieldTranslation.NO])}
                                END AS ${this.renderStringInQuerySelect(courseField.name.value)}`);
                archivedSelect.push(`'' AS ${this.renderStringInQuerySelect(courseField.name.value)}`);
                courseAdditionalFieldsSelect.push(`"field_${fieldId}"::number AS "field_${fieldId}"`);
                courseAdditionalFieldsFrom.push(`"field_${fieldId}"`);
                courseAdditionalFieldsId.push(fieldId);
                groupBy.push(`${TablesListAliases.LEARNING_COURSE_FIELD_VALUE}."field_${fieldId}"`);
                break;
            default:
                if (!isTheJoinAlreadyHandled) {
                    join.pop();
                    from.pop();
                }
                return false;
        }
        return true;
    }

    protected additionalLpFieldQueryWith(additionalFieldsId: any[]) {
        return `${TablesList.LEARNING_PLAN_ADDITIONAL_FIELDS_TRANSLATIONS} AS (
                SELECT lcfv."id_field", lcfv."id_path", ${this.queryBuilderAdditionalFieldValue()}
                FROM ${TablesList.LEARNING_COURSEPATH_FIELD_VALUE} AS lcfv
                INNER JOIN ${TablesList.LEARNING_COURSE_FIELD} AS lcf ON lcf."id_field" = lcfv."id_field" AND lcf."type" <> 'dropdown'
                WHERE lcfv."id_field" IN (${additionalFieldsId.join(', ')})
                UNION
                SELECT lcfv."id_field", lcfv."id_path", lcfdt."translation" AS "field_value"
                FROM ${TablesList.LEARNING_COURSEPATH_FIELD_VALUE} AS lcfv
                INNER JOIN ${TablesList.LEARNING_COURSE_FIELD} AS lcf ON lcf."id_field" = lcfv."id_field" AND lcf."type" = 'dropdown'
                LEFT JOIN ${TablesList.LEARNING_COURSE_FIELD_DROPDOWN_TRANSLATIONS} AS lcfdt ON lcfdt."id_option" = lcfv."field_value" AND lcfdt."lang_code" = '${this.session.user.getLang()}'
                WHERE lcfv."id_field" IN (${additionalFieldsId.join(', ')})
            )`;
    }

    protected queryWithLpAdditionalFields(field: string, queryHelper: any, lpExtraFields: any): boolean {
        if (!this.session.platform.isToggleNewLearningPlanManagementAndReportEnhancement() || !this.isLearningPlanExtraField(field)) {
            return false;
        }
        const {select, from, join, groupBy, lpAdditionalFieldsSelect, lpAdditionalFieldsFrom, lpAdditionalFieldsIds} = queryHelper;

        const fieldId = parseInt(field.replace('lp_extrafield_', ''), 10);
        const fields = lpExtraFields.data.items.filter((field) => field.id === fieldId);

        if (!fields?.length) {
            return false;
        }

        const lpField = fields[0];

        switch (lpField.type) {
            case AdditionalFieldsTypes.Textarea:
            case AdditionalFieldsTypes.Textfield:
            case AdditionalFieldsTypes.Dropdown:
                select.push(`${TablesListAliases.LEARNING_COURSEPATH_FIELD_VALUE}."field_${fieldId}" AS ${this.renderStringInQuerySelect(lpField.name.value)}`);
                lpAdditionalFieldsSelect.push(`"field_${fieldId}"`);
                lpAdditionalFieldsFrom.push(`"field_${fieldId}"`);
                lpAdditionalFieldsIds.push(fieldId);
                groupBy.push(`${TablesListAliases.LEARNING_COURSEPATH_FIELD_VALUE}."field_${fieldId}"`);
                break;
            case AdditionalFieldsTypes.Date:
                select.push(`TO_CHAR(${TablesListAliases.LEARNING_COURSEPATH_FIELD_VALUE}."field_${fieldId}", 'YYYY-MM-DD') AS ${this.renderStringInQuerySelect(lpField.name.value)}`);
                lpAdditionalFieldsSelect.push(`"field_${fieldId}"::date AS "field_${fieldId}"`);
                lpAdditionalFieldsFrom.push(`"field_${fieldId}"`);
                lpAdditionalFieldsIds.push(fieldId);
                groupBy.push(`${TablesListAliases.LEARNING_COURSEPATH_FIELD_VALUE}."field_${fieldId}"`);
                break;
            default:
                return false;
        }

        if (!join.includes(joinedTables.LEARNING_COURSEPATH_FIELD_VALUE)) {
            join.push(joinedTables.LEARNING_COURSEPATH_FIELD_VALUE);
            from.push(`LEFT JOIN ${TablesList.LEARNING_COURSEPATH_FIELD_VALUE_WITH} AS ${TablesListAliases.LEARNING_COURSEPATH_FIELD_VALUE}
                        ON ${TablesListAliases.LEARNING_COURSEPATH_FIELD_VALUE}."id_path" = ${TablesListAliases.LEARNING_COURSEPATH}."id_path"`);
        }

        return true;
    }

    protected additionalExternalTrainingFieldQueryWith(additionalFieldsId: any[]) {
        return `${TablesList.EXTERNAL_TRAINING_ADDITIONAL_FIELDS_TRANSLATIONS} AS (
                SELECT lcfv."id_field", lcfv."id_record", ${this.queryBuilderAdditionalFieldValue()}
                FROM ${TablesList.TRANSCRIPTS_FIELD_VALUE} AS lcfv
                INNER JOIN ${TablesList.TRANSCRIPTS_FIELD} AS lcf ON lcf."id_field" = lcfv."id_field" AND lcf."type" <> 'dropdown'
                WHERE lcfv."id_field" IN (${additionalFieldsId.join(', ')})
                UNION
                SELECT lcfv."id_field", lcfv."id_record", lcfdt."translation" AS "field_value"
                FROM ${TablesList.TRANSCRIPTS_FIELD_VALUE} AS lcfv
                INNER JOIN ${TablesList.TRANSCRIPTS_FIELD} AS lcf ON lcf."id_field" = lcfv."id_field" AND lcf."type" = 'dropdown'
                LEFT JOIN ${TablesList.TRANSCRIPTS_FIELD_DROPDOWN_TRANSLATIONS} AS lcfdt ON lcfdt."id_option" = lcfv."field_value" AND lcfdt."lang_code" = '${this.session.user.getLang()}'
                WHERE lcfv."id_field" IN (${additionalFieldsId.join(', ')})
            )`;
    }

    protected queryWithExternalTrainingAdditionalFields(field: string, queryHelper: any, externalTrainingExtraFields: any): boolean {
        if (!this.isExternalActivityExtraField(field)) {
            return false;
        }
        const {select, externalTrainingAdditionalFieldsSelect, externalTrainingAdditionalFieldsFrom, externalTrainingAdditionalFieldsId, from, join} = queryHelper;
        const fieldId = parseInt(field.replace('external_activity_extrafield_', ''), 10);
        const fields = externalTrainingExtraFields.data.items.filter((x) => x.id === fieldId);
        if (!fields?.length) {
            return false;
        }
        let isTheJoinAlreadyHandled = true;
        if (!join.includes(joinedTables.TRANSCRIPTS_FIELD_VALUE)) {
            join.push(joinedTables.TRANSCRIPTS_FIELD_VALUE);
            from.push(`LEFT JOIN ${TablesList.TRANSCRIPTS_FIELD_VALUE_WITH} AS ${TablesListAliases.TRANSCRIPTS_FIELD_VALUE} ON ${TablesListAliases.TRANSCRIPTS_FIELD_VALUE}."id_record" = ${TablesListAliases.TRANSCRIPTS_RECORD}."id_record"`);
            isTheJoinAlreadyHandled = false;
        }
        const extTrainingField = fields[0];
        switch (extTrainingField.type) {
            case AdditionalFieldsTypes.Textarea:
            case AdditionalFieldsTypes.Textfield:
            case AdditionalFieldsTypes.Dropdown:
                select.push(`${TablesListAliases.TRANSCRIPTS_FIELD_VALUE}."field_${fieldId}" AS ${this.renderStringInQuerySelect(extTrainingField.title)}`);
                externalTrainingAdditionalFieldsSelect.push(`"field_${fieldId}"`);
                externalTrainingAdditionalFieldsFrom.push(`"field_${fieldId}"`);
                externalTrainingAdditionalFieldsId.push(fieldId);
                break;
            case AdditionalFieldsTypes.Date:
                select.push(`TO_CHAR(${TablesListAliases.TRANSCRIPTS_FIELD_VALUE}."field_${fieldId}", 'YYYY-MM-DD') AS ${this.renderStringInQuerySelect(extTrainingField.title)}`);
                externalTrainingAdditionalFieldsSelect.push(`"field_${fieldId}"::date AS "field_${fieldId}"`);
                externalTrainingAdditionalFieldsFrom.push(`"field_${fieldId}"`);
                externalTrainingAdditionalFieldsId.push(fieldId);
                break;
            default:
                if (!isTheJoinAlreadyHandled) {
                    join.pop();
                    from.pop();
                }
                return false;
        }
        return true;
    }

    protected queryWithCourseUserAdditionalFields(field: string, queryHelper: any, courseuserExtraFields: any): boolean {
        if (!this.isCourseUserExtraField(field)) {
            return false;
        }
        const {select, archivedSelect, from, groupBy} = queryHelper;

        const fieldId = parseInt(field.replace('courseuser_extrafield_', ''), 10);
        const fields = courseuserExtraFields.data.filter((x) => x.id === fieldId);
        if (!fields?.length) {
            return false;
        }
        const isToggleUsersLearningPlansReportEnhancement = this.info.type === ReportsTypes.USERS_LP && this.session.platform.isToggleUsersLearningPlansReportEnhancement();

        const courseuserField = fields[0];
        switch (courseuserField.type) {
            case AdditionalFieldsTypes.Textarea:
            case AdditionalFieldsTypes.Text:
            case AdditionalFieldsTypes.Date:
                if (isToggleUsersLearningPlansReportEnhancement) {
                    select.push(`MAX(
                        JSON_EXTRACT_PATH_TEXT(
                            ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."enrollment_fields",
                            '${this.renderStringInQuerySelect(courseuserField.id.toString())}'))
                        AS ${this.renderStringInQuerySelect(courseuserField.name)}`);
                    break;
                }
                groupBy.push(`${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."enrollment_fields"`);
                select.push(`JSON_EXTRACT_PATH_TEXT(${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."enrollment_fields", '${this.renderStringInQuerySelect(courseuserField.id.toString())}') AS ${this.renderStringInQuerySelect(courseuserField.name)}`);
                archivedSelect.push(`NULL AS ${this.renderStringInQuerySelect(courseuserField.name)}`);
                break;
            case AdditionalFieldsTypes.Dropdown:
                from.push(`LEFT JOIN ${TablesList.LEARNING_ENROLLMENT_FIELDS_DROPDOWN} AS ${TablesListAliases.LEARNING_ENROLLMENT_FIELDS_DROPDOWN}_${fieldId}
                    ON ${TablesListAliases.LEARNING_ENROLLMENT_FIELDS_DROPDOWN}_${fieldId}."id" = CAST(JSON_EXTRACT_PATH_TEXT(${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."enrollment_fields", '${this.renderStringInQuerySelect(courseuserField.id.toString())}') AS INTEGER)`);

                if (isToggleUsersLearningPlansReportEnhancement) {
                        select.push(`MAX(IFF(${TablesListAliases.LEARNING_ENROLLMENT_FIELDS_DROPDOWN}_${fieldId}."translation" LIKE '%"${this.session.user.getLangCode()}":%',
                        JSON_EXTRACT_PATH_TEXT(${TablesListAliases.LEARNING_ENROLLMENT_FIELDS_DROPDOWN}_${fieldId}."translation", '"${this.session.user.getLangCode()}"'),
                        JSON_EXTRACT_PATH_TEXT(${TablesListAliases.LEARNING_ENROLLMENT_FIELDS_DROPDOWN}_${fieldId}."translation", '"${this.session.platform.getDefaultLanguageCode()}"')
                    )) AS ${this.renderStringInQuerySelect(courseuserField.name)}`);
                    break;
                }

                groupBy.push(`${TablesListAliases.LEARNING_ENROLLMENT_FIELDS_DROPDOWN}_${fieldId}."translation"`);
                select.push(`IFF(${TablesListAliases.LEARNING_ENROLLMENT_FIELDS_DROPDOWN}_${fieldId}."translation" LIKE '%"${this.session.user.getLangCode()}":%',
                                    JSON_EXTRACT_PATH_TEXT(${TablesListAliases.LEARNING_ENROLLMENT_FIELDS_DROPDOWN}_${fieldId}."translation", '"${this.session.user.getLangCode()}"'),
                                    JSON_EXTRACT_PATH_TEXT(${TablesListAliases.LEARNING_ENROLLMENT_FIELDS_DROPDOWN}_${fieldId}."translation", '"${this.session.platform.getDefaultLanguageCode()}"')
                                ) AS ${this.renderStringInQuerySelect(courseuserField.name)}`);
                archivedSelect.push(`'' AS ${this.renderStringInQuerySelect(courseuserField.name)}`);
                break;
            default:
                return false;
        }
        return true;
    }

    protected additionalClassroomFieldQueryWith(additionalFieldsId: any[]) {
        return `${TablesList.CLASSROOM_ADDITIONAL_FIELDS_TRANSLATIONS} AS (
                SELECT lcfv."id_field", lcfv."id_session", ${this.queryBuilderAdditionalFieldValue()}
                FROM ${TablesList.LT_COURSE_SESSION_FIELD_VALUES} AS lcfv
                INNER JOIN ${TablesList.LEARNING_COURSE_FIELD} AS lcf ON lcf."id_field" = lcfv."id_field" AND lcf."type" <> 'dropdown'
                WHERE lcfv."id_field" IN (${additionalFieldsId.join(', ')})
                UNION
                SELECT lcfv."id_field", lcfv."id_session", lcfdt."translation" AS "field_value"
                FROM ${TablesList.LT_COURSE_SESSION_FIELD_VALUES} AS lcfv
                INNER JOIN ${TablesList.LEARNING_COURSE_FIELD} AS lcf ON lcf."id_field" = lcfv."id_field" AND lcf."type" = 'dropdown'
                LEFT JOIN ${TablesList.LEARNING_COURSE_FIELD_DROPDOWN_TRANSLATIONS} AS lcfdt ON lcfdt."id_option" = lcfv."field_value" AND lcfdt."lang_code" = '${this.session.user.getLang()}'
                WHERE lcfv."id_field" IN (${additionalFieldsId.join(', ')})
            )`;
    }

    protected queryWithClassroomAdditionalFields(field: string, queryHelper: any, classroomExtraFields: any): boolean {
        if (!this.isClassroomExtraField(field)) {
            return false;
        }

        const { select, archivedSelect, from, join, groupBy, classroomAdditionalFieldsSelect, classroomAdditionalFieldsFrom, classroomAdditionalFieldsId } = queryHelper;

        const fieldId = parseInt(field.replace('classroom_extrafield_', ''), 10);
        const fields = classroomExtraFields.data.items.filter((x) => parseInt(x.id, 10) === fieldId);
        if (!fields?.length) {
            return false;
        }

        const classroomField = fields[0];

        let isTheJoinAlreadyHandled = true;
        if (!join.includes(joinedTables.LT_COURSE_SESSION_FIELD_VALUES)) {
            join.push(joinedTables.LT_COURSE_SESSION_FIELD_VALUES);
            from.push(`LEFT JOIN ${TablesList.LT_COURSE_SESSION_FIELD_VALUES_WITH} AS ${TablesListAliases.LT_COURSE_SESSION_FIELD_VALUES}
                                ON ${TablesListAliases.LT_COURSE_SESSION_FIELD_VALUES}."id_session" = ${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}."id_session"`);
            isTheJoinAlreadyHandled = false;
        }
        switch (classroomField.type) {
            case AdditionalFieldsTypes.Textarea:
            case AdditionalFieldsTypes.Textfield:
            case AdditionalFieldsTypes.Dropdown:
                select.push(`${TablesListAliases.LT_COURSE_SESSION_FIELD_VALUES}."field_${fieldId}" AS ${this.renderStringInQuerySelect(this.setAdditionalFieldTranslation(classroomField))}`);
                archivedSelect.push(`'' AS ${this.renderStringInQuerySelect(this.setAdditionalFieldTranslation(classroomField))}`);
                classroomAdditionalFieldsSelect.push(`"field_${fieldId}"`);
                classroomAdditionalFieldsFrom.push(`"field_${fieldId}"`);
                classroomAdditionalFieldsId.push(fieldId);
                groupBy.push(`${TablesListAliases.LT_COURSE_SESSION_FIELD_VALUES}."field_${fieldId}"`);
                break;
            case AdditionalFieldsTypes.Date:
                select.push(`TO_CHAR(${TablesListAliases.LT_COURSE_SESSION_FIELD_VALUES}."field_${fieldId}", 'YYYY-MM-DD') AS ${this.renderStringInQuerySelect(this.setAdditionalFieldTranslation(classroomField))}`);
                archivedSelect.push(`NULL AS ${this.renderStringInQuerySelect(this.setAdditionalFieldTranslation(classroomField))}`);
                classroomAdditionalFieldsSelect.push(`"field_${fieldId}"::date AS "field_${fieldId}"`);
                classroomAdditionalFieldsFrom.push(`"field_${fieldId}"`);
                classroomAdditionalFieldsId.push(fieldId);
                groupBy.push(`${TablesListAliases.LT_COURSE_SESSION_FIELD_VALUES}."field_${fieldId}"`);
                break;
            default:
                if (!isTheJoinAlreadyHandled) {
                    join.pop();
                    from.pop();
                }
                return false;
        }
        return true;
    }

    protected additionalFieldQueryWith(additionalFieldsFrom: any[], additionalFieldsSelect: any[], additionalFieldsId: any[], baseField: string, tableNameWith: TablesList, tableNameFrom: TablesList): string {
        additionalFieldsFrom.unshift(`"${baseField}"`);
        additionalFieldsSelect.unshift(`"${baseField}"`);

        return `${tableNameWith} AS (
                SELECT ${additionalFieldsSelect.join(', ')}
                FROM ${tableNameFrom}
                PIVOT(MAX("field_value") FOR "id_field" IN (${additionalFieldsId.join(', ')}))
                AS p (${additionalFieldsFrom.join(', ')})
            )`;
    }

    protected getCoreUserBranchesWiths(checkPuVisibility: boolean): string[] {
        const withs: string[] = [];

        withs.push(`${TablesList.CORE_GROUP_MEMBERS_BRANCHES} AS (
            SELECT cgm."idst", cgm."idstmember"
            FROM (
                SELECT "idst"
                FROM ${TablesList.CORE_GROUP}
                WHERE "groupid" like '/oc_%'
                OR "groupid" like '/ocd_%'
            ) AS cg
            JOIN ${TablesList.CORE_GROUP_MEMBERS} AS cgm ON cgm."idst" = cg."idst"
            GROUP BY cgm."idst", cgm."idstmember"
        )`);

        withs.push(`${TablesList.CORE_ORG_CHART_TREE_TRANSLATIONS} AS (
            SELECT tree."idorg",
            CASE
                WHEN ANY_VALUE(t."translation") IS NOT NULL AND ANY_VALUE(t."translation") <> '' THEN ANY_VALUE(t."translation")
                ELSE ANY_VALUE(td."translation")
            END AS "translation"
            FROM ${TablesList.CORE_ORG_CHART_TREE} AS tree
            LEFT JOIN ${TablesList.CORE_ORG_CHART} AS t ON t."id_dir" = tree."idorg" AND t."lang_code" = '${this.session.user.getLang()}'
            LEFT JOIN ${TablesList.CORE_ORG_CHART} AS td ON td."id_dir" = tree."idorg" AND td."lang_code" = '${this.session.platform.getDefaultLanguage()}'
            GROUP BY tree."idorg", tree."ileft", tree."iright"
        )`);

        withs.push(`${TablesList.CORE_ORG_CHART_PATHS_REFACTORED} AS (
            SELECT
                coct."idorg",
                LISTAGG(
                    coctt."translation",
                    ' > '
                ) WITHIN GROUP (ORDER BY coctp."ileft") AS "path",
                coct."code" AS "code",
                MAX(IFF(coct."idorg" = coctt."idorg", coctt."translation", NULL)) AS "single_translation"
            FROM ${TablesList.CORE_ORG_CHART_TREE} AS coct
            INNER JOIN ${TablesList.CORE_ORG_CHART_TREE} AS coctp ON coctp."ileft" <= coct."ileft" AND coctp."iright" >= coct."iright" AND coctp."ileft" <> 1 and coct."ileft" <> 1
            INNER JOIN ${TablesList.CORE_ORG_CHART_TREE_TRANSLATIONS} AS coctt ON coctt."idorg" = coctp."idorg"
            GROUP BY coct."idorg", coct."code"
        )`);

        const joinPuUsers = `JOIN ${TablesList.CORE_USER_PU} AS pu_users ON pu_users."user_id" = cgm."idstmember" AND pu_users."puser_id" = ${this.session.user.getIdUser()}`;
        const joinPuBranches = `JOIN ${TablesList.CORE_USER_PU} AS pu_branches ON pu_branches."user_id" = cgm."idst" AND pu_branches."puser_id" = ${this.session.user.getIdUser()}`;

        withs.push(`${TablesList.CORE_USER_BRANCHES_REFACTORED} AS (
            SELECT
                cgm."idstmember" AS "idst",
                LISTAGG(
                    DISTINCT cocp."code",
                    ', '
                ) WITHIN GROUP (ORDER BY cocp."code") AS "codes",
                LISTAGG(
                    DISTINCT cocp."path",
                    ', '
                ) WITHIN GROUP (ORDER BY cocp."path") AS "branches",
                LISTAGG(
                    DISTINCT cocp."single_translation",
                    ', '
                ) WITHIN GROUP (ORDER BY cocp."single_translation") AS "branches_names"
            FROM core_group_members_branches AS cgm
            ${this.session.user.isPowerUser() && checkPuVisibility ? `${joinPuUsers} ${joinPuBranches}` : ``}
            JOIN core_org_chart_tree AS coct ON (coct."idst_oc" = cgm."idst" OR coct."idst_ocd" = cgm."idst") AND "ileft" <> 1
            JOIN core_org_chart_paths_refactored AS cocp ON cocp."idorg" = coct."idorg"
            GROUP BY  cgm."idstmember"
        )`);

        return withs;
    }

    protected getCourseAssignmentTypeSelectField(addArbitrary: boolean, translations: { [key: string]: string }): string {
        let assignmentTypeField = `${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.assignment_type`;
        if (this.session.platform.isDatalakeV3ToggleActive()) {
            assignmentTypeField = this.convertToDatalakeV3(assignmentTypeField);
        } else {
            if (addArbitrary) {
                assignmentTypeField = `ARBITRARY(${assignmentTypeField})`;
            }
        }

        return `
            CASE
                WHEN ${assignmentTypeField} = ${AssignmentTypes.Mandatory} THEN ${this.renderStringInQueryCase(translations[FieldTranslation.ASSIGNMENT_TYPE_MANDATORY])}
                WHEN ${assignmentTypeField} = ${AssignmentTypes.Required} THEN ${this.renderStringInQueryCase(translations[FieldTranslation.ASSIGNMENT_TYPE_REQUIRED])}
                WHEN ${assignmentTypeField} = ${AssignmentTypes.Recommended} THEN ${this.renderStringInQueryCase(translations[FieldTranslation.ASSIGNMENT_TYPE_RECOMMENDED])}
                WHEN ${assignmentTypeField} = ${AssignmentTypes.Optional} THEN ${this.renderStringInQueryCase(translations[FieldTranslation.ASSIGNMENT_TYPE_OPTIONAL])}
                ELSE CAST (${assignmentTypeField} as varchar)
            END AS ${this.renderStringInQuerySelect(translations[FieldsList.COURSEUSER_ASSIGNMENT_TYPE])}`;
    }

    protected getLPSubQuery(puId: number, lpFilter: number[] = []): string {
        let query = 'SELECT DISTINCT cupc."path_id" ' +
            'FROM core_user_pu_coursepath AS cupc ' +
            'JOIN learning_coursepath AS lc ON lc."id_path" = cupc."path_id" ' +
            `WHERE cupc."puser_id" = ${puId} `;
        if (lpFilter.length > 0) {
            query += `AND cupc."path_id" IN (${lpFilter.join(', ')}) `;
        }

        return query;
    }

    protected queryConvertTimezone(column: string, format = ''): string {
        const sourceTimezone = 'UTC';
        const targetTimezone = this.info.timezone;
        return `TO_CHAR(CONVERT_TIMEZONE('${sourceTimezone}', '${targetTimezone}', ${column}), '${format || 'YYYY-MM-DD HH24:MI:SS'}')`;
    }

    private getSqlUserWithEvaluationSnowflake() {
        return `COUNT(DISTINCT(IFF(${TablesListAliases.LT_COURSEUSER_SESSION}."evaluation_status" is not null AND ${TablesListAliases.LT_COURSEUSER_SESSION}."evaluation_date" is not null , ${TablesListAliases.LT_COURSEUSER_SESSION}."id_user", null)))`;
    }
}
