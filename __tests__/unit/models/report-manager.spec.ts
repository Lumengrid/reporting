import SessionManager from '../../../src/services/session/session-manager.session';
import { UsersCoursesManager } from '../../../src/models/report-users-courses';
import { DynamoReport } from '../../../src/services/dynamo';
import Hydra from '../../../src/services/hydra';
import PlatformManager from '../../../src/services/session/platform-manager.session';

describe('Report Manager service', () => {

  const mock = jest.fn();
  const sessionsManagerMock = new mock() as SessionManager;
  sessionsManagerMock.getHydra = (): Hydra => {
    return {} as Hydra;
  };
  sessionsManagerMock.platform = new mock() as PlatformManager;
  sessionsManagerMock.platform.checkPluginESignatureEnabled = (): boolean => {
    return false;
  };
  sessionsManagerMock.platform.checkPluginFlowEnabled = (): boolean => {
    return false;
  };
  sessionsManagerMock.platform.checkPluginFlowMsTeamsEnabled = (): boolean => {
    return false;
  };
  sessionsManagerMock.platform.isToggleMultipleEnrollmentCompletions = (): boolean => {
    return false;
  };
  sessionsManagerMock.platform.isLearningplansAssignmentTypeActive = (): boolean => {
    return false;
  };
  sessionsManagerMock.platform.isCoursesAssignmentTypeActive = (): boolean => {
    return false;
  };
  const dynamoReportMock = new mock() as DynamoReport;

  it('Should be able to generate a report slugname date for the export file', () => {
    const reportManagerService = new UsersCoursesManager(sessionsManagerMock, dynamoReportMock);
    const testDate: Date = new Date('03-25-2019');

    const reportExtractionSlugName: string = reportManagerService.convertDateObjectToExportDate(testDate);

    expect(reportExtractionSlugName).toEqual('20190325');
  });

  it('Should be able to convert date to internal date format', () => {
    const reportManagerService = new UsersCoursesManager(sessionsManagerMock, dynamoReportMock);
    const testDate: Date = new Date('03-25-2019');

    const reportExtractionSlugName: string = reportManagerService.convertDateObjectToDate(testDate);

    expect(reportExtractionSlugName).toEqual('2019-03-25');
  });

  it('Should be able to convert datetime to internal datetime format', () => {
    const reportManagerService = new UsersCoursesManager(sessionsManagerMock, dynamoReportMock);
    const testDate: Date = new Date('03-25-2019 12:32:55');

    const reportExtractionSlugName: string = reportManagerService.convertDateObjectToDatetime(testDate);

    expect(reportExtractionSlugName).toEqual('2019-03-25 12:32:55');
  });

});
