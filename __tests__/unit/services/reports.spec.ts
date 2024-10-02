import Hydra, { UsersResponse } from '../../../src/services/hydra';
import { ReportService } from '../../../src/services/report';
import { Fullname, Report, TimeFrameOptions } from '../../../src/models/custom-report';
import { ReportManagerInfo } from '../../../src/models/report-manager';

describe('Report service', () => {

    const mock = jest.fn();
    const hydraService = new mock() as Hydra;
    hydraService.getUsers = async (userIds): Promise<UsersResponse> => {
        return {
            data: {
                12345: {
                    firstname: 'luca',
                    lastname: 'skywalker',
                    userid: '12345'
                },
                12312: {
                    firstname: 'luca 2',
                    lastname: 'skywalker 2',
                    userid: '12312'
                },
                22345: {
                    firstname: 'luca 3',
                    lastname: 'skywalker 3',
                    userid: '22345'
                }
            }
        };
    };

    it('Should be able to convert a report record from dynamo metadata to aamons', () => {
        // ... call the first method
        const reportService: ReportService = new ReportService(hydraService);
        const dynamoReport = {
            author: 0,
            conditions: 'allConditions',
            courses: {
                all: true,
                courses: [],
                categories: [],
                instructors: [],
                entitiesLimits: {},
                courseType: 0
            },
            creationDate: '2019-09-23 14:05:25',
            deleted: false,
            description: 'description',
            fields: [
                'user_userid',
                'course_name'
            ],
            hideDeactivated: true,
            showOnlyLearners: true,
            hideExpiredUsers: true,
            idReport: '4764e33f-1962-4939-861f-7c59ad8525e9',
            lastEdit: '2019-09-23 14:05:25',
            lastEditBy: {
                avatar: '',
                firstname: '',
                idUser: 0,
                lastname: '',
                username: ''
            },
            planning: {
                active: false,
                option: {
                    every: 1,
                    isPaused: false,
                    recipients: [],
                    scheduleFrom: '',
                    timeFrame: TimeFrameOptions.days,
                    startHour: '12:11',
                    timezone: 'Europe/Rome'
                }
            },
            platform: 'hydra.docebosaas.com',
            standard: true,
            title: 'title',
            type: 'Users - Courses',
            visibility: {
                branches: [],
                groups: [],
                type: 0,
                users: []
            },
            sortingOptions: {
                orderBy: 'asc',
                selectedField: 'user_userid',
                selector: 'default'
            },
            timezone: 'Europe/Rome',
            loginRequired: true,
            isReportDownloadPermissionLink: false,
            isCustomColumnSortingEnabled: false
        };

        const aamonReport: Report = reportService.convertDynamoReportMetadataToAamonReportMetadata(dynamoReport);

        const createdByDesc = new Fullname();

        const expectedReport: Report = {
            name: 'title',
            type: 'Users - Courses',
            createdBy: '',
            creationDate: '2019-09-23 14:05:25',
            description: 'description',
            idReport: '4764e33f-1962-4939-861f-7c59ad8525e9',
            visibility: 0,
            standard: true,
            createdByDescription: createdByDesc,
            planning: {
                active: false,
                option: {
                    every: 1,
                    recipients: [],
                    timeFrame: TimeFrameOptions.days,
                    isPaused: false,
                    scheduleFrom: '',
                    startHour: '12:11',
                    timezone: 'Europe/Rome'
                }
            }
        };

        expect(aamonReport).toEqual(expectedReport);
    });

    it('Should be able to convert and enrich multiple reports record from dynamo', async () => {
        const reportService: ReportService = new ReportService(hydraService);

        const enrichedReports: Report[] = await reportService.convertDynamoReportsMetadataToAamonReportsMetadata(dynamoReports);

        expect(enrichedReports).toEqual(convertedDynamoReports);
    });


    // Test data
    const dynamoReports: ReportManagerInfo[] = [
        {
            idReport: 'aaa',
            title: 'title 1',
            description: 'description 1',
            standard: true,
            type: 'users-courses',
            author: 12345,
            creationDate: '01/01/2001',
            visibility: {
                type: 1,
                branches: [],
                groups: [],
                users: []
            },
            deleted: false,
            fields: [],
            lastEdit: 'this should be a date, not a string!',
            lastEditBy: {
                avatar: '',
                firstname: 'pippo',
                idUser: 1,
                lastname: 'boh',
                username: 'pippo.boh'
            },
            planning: {
                active: true,
                option: undefined
            },
            platform: 'test.docebosaas.com',
            sortingOptions: {
                orderBy: 'a field',
                selectedField: 'a field',
                selector: 'a selector',
            },
            timezone: 'Europe/Rome',
            loginRequired: true,
            isReportDownloadPermissionLink: false,
            isCustomColumnSortingEnabled: false
        },
        {
            idReport: 'bbb',
            title: 'title 2',
            description: 'description 2',
            standard: true,
            type: 'groups-courses',
            author: 22345,
            creationDate: '02/01/2009',
            visibility: {
                type: 2,
                branches: [],
                groups: [],
                users: []
            },
            deleted: false,
            fields: [],
            lastEdit: 'this should be a date, not a string!',
            lastEditBy: {
                avatar: '',
                firstname: 'pippo2',
                idUser: 12,
                lastname: 'boh2',
                username: 'pippo.boh2'
            },
            planning: {
                active: true,
                option: undefined
            },
            platform: 'test1.docebosaas.com',
            sortingOptions: {
                orderBy: 'a field',
                selectedField: 'a field',
                selector: 'a selector'
            },
            timezone: 'Europe/Rome',
            loginRequired: true,
            isReportDownloadPermissionLink: false,
            isCustomColumnSortingEnabled: false
        }
    ];
    const convertedDynamoReports: Report[] = [
        {
            name: 'title 1',
            type: 'users-courses',
            createdBy: 12345,
            creationDate: '01/01/2001',
            description: 'description 1',
            idReport: 'aaa',
            visibility: 1,
            standard: true,
            createdByDescription: {
                firstname: 'luca',
                lastname: 'skywalker',
                userId: '12345',
            },
            planning: {
                active: true,
                option: {
                    every: 1,
                    isPaused: false,
                    recipients: [],
                    scheduleFrom: '',
                    timeFrame: TimeFrameOptions.days,
                    startHour: '',
                    timezone: ''
                }
            }
        },
        {
            name: 'title 2',
            type: 'groups-courses',
            createdBy: 22345,
            creationDate: '02/01/2009',
            description: 'description 2',
            idReport: 'bbb',
            visibility: 2,
            standard: true,
            createdByDescription: {
                firstname: 'luca 3',
                lastname: 'skywalker 3',
                userId: '22345',
            },
            planning: {
                active: true,
                option: {
                    every: 1,
                    isPaused: false,
                    recipients: [],
                    scheduleFrom: '',
                    timeFrame: TimeFrameOptions.days,
                    startHour: '',
                    timezone: ''
                }
            }
        }
    ];
});
