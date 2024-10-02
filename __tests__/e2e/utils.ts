import axios, { AxiosResponse } from 'axios';
import { ReportsTypes } from '../../src/reports/constants/report-types';
import * as dotenv from 'dotenv';
import * as fs from 'fs';

type LoginResponse = {
  data: {
    data: {
      access_token: string,
      refresh_token: string,
      id_user: number
    }
  }
};

export class E2ETestUtils {

  static adminUserName = 'staff.support';
  static adminPassword = '!mandocebo1379';

  static puUserName = 'staff.support.pu';
  static puPassword = '!mandocebo1379';

  static userName = 'staff.support.user';
  static password = '!mandocebo1379';

  static loginEndpoint = 'https://hydra.docebosaas.com/manage/v1/user/login';

  /**
   * Load env-e2e env
   */
  public static loadTestEnv(): void {
    const pathEnvTest = '.env-test';

    if (fs.existsSync(pathEnvTest)) {
      dotenv.config({ path: `${pathEnvTest}` });
    } else {
      console.log('Missing .env-test file, copy it from .env-test-dist');
    }
  }

  /**
   * Get a godadmin access token to use in tests
   *
   * @throws LoginError
   */
  public static async loginAsGodAdmin(): Promise<string> {
    return E2ETestUtils.login(E2ETestUtils.adminUserName, E2ETestUtils.adminPassword);
  }

  /**
   * Get a power user access token to use in tests
   *
   * @todo implements different power users with different report permissions
   * @throws LoginError
   */
  public static loginAsPowerUser(permissions: any) {
    return E2ETestUtils.login(E2ETestUtils.puUserName, E2ETestUtils.puPassword);
  }

  /**
   * Get a standard user access token to use in tests
   *
   * @throws LoginError
   */
  public static loginAsUser() {
    return E2ETestUtils.login(E2ETestUtils.userName, E2ETestUtils.password);
  }

  private static async login(username: string, password: string): Promise<string> {
    let loginResponse: LoginResponse;
    try {
      loginResponse = await axios.post(
        E2ETestUtils.loginEndpoint, {
          username,
          password
        });
    } catch (e) {
      // Cannot perform login
      throw new LoginErrorException();
    }

    return loginResponse.data.data.access_token;
  }

  static async createReport(reportMetadata: object, reportDetails: object, adminToken: string): Promise<string> {
    const testReportCreationResponse = await axios.post(
      'https://hydra.docebosaas.com/analytics/v1/reports',
      reportMetadata,
      {headers: {Authorization: `Bearer ${adminToken}`}}
    );
    const testReportId = testReportCreationResponse.data.data.idReport;

    const testReportGetResponse = await axios.get(
      'https://hydra.docebosaas.com/analytics/v1/reports/' + testReportId,
      {headers: {Authorization: `Bearer ${adminToken}`}}
    );
    const testReportDetails = testReportGetResponse.data.data;

    await axios.put(
      'https://hydra.docebosaas.com/analytics/v1/reports/' + testReportId,
      Object.assign(testReportDetails, reportDetails),
      {headers: {Authorization: `Bearer ${adminToken}`}}
    );

    return testReportId;
  }

  static async createCustomReportTypes(reportMetadata: object, adminToken: string): Promise<AxiosResponse> {
    const testReportCreationResponse = await axios.post(
        'https://hydra.docebosaas.com/analytics/v1/custom-report-types',
        reportMetadata,
        {headers: {Authorization: `Bearer ${adminToken}`}}
    );

    return testReportCreationResponse;
  }

  static async editCustomReportTypes(testCustomReportTypeId: string, adminToken: string, customReportTypes: object): Promise<AxiosResponse> {
    try {
      return await axios.put(
          'https://hydra.docebosaas.com/analytics/v1/custom-report-types/' + testCustomReportTypeId,
          customReportTypes,
          {headers: {Authorization: `Bearer ${adminToken}`}}
      );
    } catch (error) {
      throw error;
    }
  }

  static async getCustomReportTypesIndex(adminToken: string): Promise<AxiosResponse> {
    return await axios.get(
        'https://hydra.docebosaas.com/analytics/v1/custom-report-types',
        {headers: {Authorization: `Bearer ${adminToken}`}}
    );
  }

  static async getCustomReportTypes(testCustomReportTypeId: string, adminToken: string): Promise<AxiosResponse> {
    return await axios.get(
        'https://hydra.docebosaas.com/analytics/v1/custom-report-types/' + testCustomReportTypeId,
        {headers: {Authorization: `Bearer ${adminToken}`}}
    );
  }

  static async customReportTypesPreview(testCustomReportTypeId: string, adminToken: string, query: string, jsonString?: string): Promise<AxiosResponse> {
    return await axios.post(
          'https://hydra.docebosaas.com/analytics/v1/custom-report-types/' + testCustomReportTypeId + '/preview',
          {sql: query, json: jsonString},
          {headers: {Authorization: `Bearer ${adminToken}`}}
      );
  }

  static async customReportTypesResults(testCustomReportTypeId: string, adminToken: string, queryExecutionId: string): Promise<AxiosResponse> {
    try {
      return await axios.get(
          'https://hydra.docebosaas.com/analytics/v1/custom-report-types/' + testCustomReportTypeId + '/preview/' + queryExecutionId,
          {headers: {Authorization: `Bearer ${adminToken}`}}
      );
    } catch (error) {
      throw error;
    }
  }

  static async getSession(adminToken: string): Promise<AxiosResponse> {
    try {
      return await axios.get(
          'https://hydra.docebosaas.com/report/v1/report/session',
          {headers: {Authorization: `Bearer ${adminToken}`}}
      );
    } catch (error) {
      throw error;
    }
  }

  static async createQueryBuilderReport(testCustomReportTypeId: string, adminToken: string): Promise<AxiosResponse> {
    const data = {
      type: ReportsTypes.QUERY_BUILDER_DETAIL,
      queryBuilderId: testCustomReportTypeId,
      name: 'You shouldn\'t see this report',
      description: ''
    };
    return await axios.post(
        'https://hydra.docebosaas.com/analytics/v1/reports/',
        data,
        {headers: {Authorization: `Bearer ${adminToken}`}}
    );
  }

  static async getAllReportsByQueryBuilderId(testCustomReportTypeId: string, adminToken: string): Promise<AxiosResponse> {
      return await axios.get(
          'https://hydra.docebosaas.com/analytics/v1/custom-report-types/' + testCustomReportTypeId + '/reports',
          {headers: {Authorization: `Bearer ${adminToken}`}}
      );
  }

  static async deleteCustomReportTypes(testCustomReportTypeId: string, adminToken: string): Promise<AxiosResponse> {
    return await axios.delete(
        `https://hydra.docebosaas.com/analytics/v1/custom-report-types/${testCustomReportTypeId}`,
        {headers: {Authorization: `Bearer ${adminToken}`}}
    );
  }

  static async deleteReport(testReportId: string, adminToken: string): Promise<void> {
    const testReportDeletionResponse = await axios.delete(
      `https://hydra.docebosaas.com/analytics/v1/reports/${testReportId}`,
      {headers: {Authorization: `Bearer ${adminToken}`}}
    );

    expect(testReportDeletionResponse.status).toBe(200);

    return;
  }

  static async checkToggleQueryBuilder(adminToken?: string): Promise<boolean> {
    let isCustomReportTypesToggleActive = false;
    if (!adminToken) {
       adminToken = await this.loginAsGodAdmin();
    }

    try {
      await E2ETestUtils.getCustomReportTypesIndex(adminToken);
      isCustomReportTypesToggleActive = true;
    } catch (error) {
      isCustomReportTypesToggleActive = false;
    }

    return isCustomReportTypesToggleActive;
  }

  static async checkToggleDatalakeV2(adminToken?: string): Promise<boolean> {
    if (!adminToken) {
      adminToken = await this.loginAsGodAdmin();
    }

    const session = await E2ETestUtils.getSession(adminToken);

    return !session.data.data.platform.toggles.toggleForceDatalakeV1 && !session.data.data.platform.toggles.toggleDatalakeV3;
  }

  static async checkToggleDatalakeV3(adminToken?: string): Promise<boolean> {
    if (!adminToken) {
      adminToken = await this.loginAsGodAdmin();
    }

    const session = await E2ETestUtils.getSession(adminToken);

    return !session.data.data.platform.toggles.toggleForceDatalakeV1 && session.data.data.platform.toggles.toggleDatalakeV3;
  }
}

class LoginErrorException {
}
