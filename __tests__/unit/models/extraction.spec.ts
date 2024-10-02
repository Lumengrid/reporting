import { ExtractionComponent } from '../../../src/models/extraction.component';
import { ExtractionMapper, ExtractionModel } from '../../../src/reports/interfaces/extraction.interface';
import { TimeFrameOptions } from '../../../src/models/custom-report';
import { configureMockExpressHttpContext, MockExpressHttpContext } from '../../utils';

describe('Extraction Component', () => {

    let mockExpressHttpContext: MockExpressHttpContext;
    beforeEach(() => {
        mockExpressHttpContext = configureMockExpressHttpContext();
    });

    afterEach(() => {
        mockExpressHttpContext.afterEachRestoreAllMocks();
    });

    it('should return 2 daily reports to extract', async () => {
        const extraction = new ExtractionComponent();

        const report: ExtractionModel[] = [
            {
                idReport: '123',
                author: 123,
                platform: 'hydra.docebosaas.com',
                planning: {
                    active: true,
                    option: {
                        recipients: [],
                        every: 1,
                        isPaused: false,
                        timeFrame: TimeFrameOptions.days,
                        scheduleFrom: '2019-09-26T12:40:57.582Z',
                        startHour: '11:11',
                        timezone: 'Europe/Rome'
                    }
                }
            },
            {
                idReport: '124',
                author: 123,
                platform: 'hydra.docebosaas.com',
                planning: {
                    active: true,
                    option: {
                        recipients: [],
                        every: 1,
                        isPaused: false,
                        timeFrame: TimeFrameOptions.days,
                        scheduleFrom: '2019-09-26T12:40:57.582Z',
                        startHour: '11:11',
                        timezone: 'Europe/Rome'
                    }
                }
            }
        ];

        const result: ExtractionMapper = await extraction.filterScheduledByPeriod(new Date('2019-10-07'), report, '');

        expect(result).toBeDefined();
        expect(result['hydra.docebosaas.com'][123].length).toEqual(2);
    });

    it('should return 2 reports, one weekly and one daily', async () => {
        const extraction = new ExtractionComponent();
        const report: ExtractionModel[] = [
            {
                idReport: '123',
                author: 123,
                platform: 'hydra.docebosaas.com',
                planning: {
                    active: true,
                    option: {
                        recipients: [],
                        every: 1,
                        isPaused: false,
                        timeFrame: TimeFrameOptions.days,
                        scheduleFrom: '2019-09-26T12:40:57.582Z',
                        startHour: '11:11',
                        timezone: 'Europe/Rome'
                    }
                }
            },
            {
                idReport: '124',
                author: 123,
                platform: 'hydra.docebosaas.com',
                planning: {
                    active: true,
                    option: {
                        recipients: [],
                        every: 1,
                        isPaused: false,
                        timeFrame: TimeFrameOptions.weeks,
                        scheduleFrom: '2019-10-07T12:40:57.582Z',
                        startHour: '11:11',
                        timezone: 'Europe/Rome'
                    }
                }
            }
        ];

        const result: ExtractionMapper = await extraction.filterScheduledByPeriod(new Date('2019-10-07'), report, '');

        expect(result).toBeDefined();
        expect(result['hydra.docebosaas.com'][123].length).toEqual(2);
    });

    it('should return 3 reports monthly scheduled', async () => {
        const extraction = new ExtractionComponent();
        const report: ExtractionModel[] = [
            {
                idReport: '123',
                author: 123,
                platform: 'hydra.docebosaas.com',
                planning: {
                    active: true,
                    option: {
                        recipients: [],
                        every: 1,
                        isPaused: false,
                        timeFrame: TimeFrameOptions.months,
                        scheduleFrom: '2019-07-31T12:40:57.582Z', // this is returned
                        startHour: '11:11',
                        timezone: 'Europe/Rome'
                    }
                }
            },
            {
                idReport: '124',
                author: 123,
                platform: 'hydra.docebosaas.com',
                planning: {
                    active: true,
                    option: {
                        recipients: [],
                        every: 1,
                        isPaused: false,
                        timeFrame: TimeFrameOptions.months,
                        scheduleFrom: '2019-08-31T12:40:57.582Z', // this is returned
                        startHour: '11:11',
                        timezone: 'Europe/Rome'
                    }
                }
            },
            {
                idReport: '125',
                author: 123,
                platform: 'hydra.docebosaas.com',
                planning: {
                    active: true,
                    option: {
                        recipients: [],
                        every: 1,
                        isPaused: false,
                        timeFrame: TimeFrameOptions.months,
                        scheduleFrom: '2019-08-30T12:40:57.582Z', // this is returned
                        startHour: '11:11',
                        timezone: 'Europe/Rome'
                    }
                }
            },
            {
                idReport: '126',
                author: 123,
                platform: 'hydra.docebosaas.com',
                planning: {
                    active: true,
                    option: {
                        recipients: [],
                        every: 1,
                        isPaused: false,
                        timeFrame: TimeFrameOptions.months,
                        scheduleFrom: '2019-08-29T12:40:57.582Z', // this is not returned
                        startHour: '11:11',
                        timezone: 'Europe/Rome'
                    }
                }
            }
        ];

        const result: ExtractionMapper = await extraction.filterScheduledByPeriod(new Date('2019-09-30'), report, '');

        expect(result).toBeDefined();
        expect(result['hydra.docebosaas.com'][123].length).toEqual(3);
    });

});
