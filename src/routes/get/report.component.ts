import { Dynamo } from '../../services/dynamo';
import { ReportService } from '../../services/report';
import { Report, VisibilityTypes } from '../../models/custom-report';
import SessionManager from '../../services/session/session-manager.session';
import { ReportManagerInfo } from '../../models/report-manager';

export class ReportsComponent {

    constructor(
        private readonly dynamoService: Dynamo,
        private readonly reportsService: ReportService
    ) { }

    /**
     * getReports
     */
    public async getReports(session: SessionManager): Promise<Report[]> {
        // get reports from dynamo
        let dynamoReports: ReportManagerInfo[] = await this.dynamoService.getReports(session) as ReportManagerInfo[];
        // Apply visibility filter just for PU
        if (session.user.isPowerUser()) {
            const reportsFiltered: ReportManagerInfo[] = [];
            for (const dynamoReport of dynamoReports) {
                // If the user id the author of the report add it to the list
                if (dynamoReport.author === session.user.getIdUser() || dynamoReport.visibility.type === VisibilityTypes.ALL_GODADMINS_AND_PU) {
                    reportsFiltered.push(dynamoReport);
                } else if (dynamoReport.visibility.type === VisibilityTypes.ALL_GODADMINS_AND_SELECTED_PU) {
                    // Check for specific visibility
                    // Visibility by users
                    if (dynamoReport.visibility.users && dynamoReport.visibility.users.length > 0 && dynamoReport.visibility.users.map(a => a.id).indexOf(session.user.getIdUser()) !== -1) {
                        reportsFiltered.push(dynamoReport);
                        continue;
                    }

                    // Visibility by groups
                    if (dynamoReport.visibility.groups && dynamoReport.visibility.groups.length > 0) {
                        let found = false;
                        for (const group of session.user.getUserGroups()) {
                            if (dynamoReport.visibility.groups.map(a => a.id).indexOf(group) !== -1) {
                                reportsFiltered.push(dynamoReport);
                                found = true;
                                break;
                            }
                        }
                        if (found) {
                            continue;
                        }
                    }

                    // Visibility by branches
                    if (dynamoReport.visibility.branches && dynamoReport.visibility.branches.length > 0) {
                        const descendants: number[] = [];
                        const noDescendants: number[] = [];

                        dynamoReport.visibility.branches.forEach(element => {
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
                            let found = false;
                            for (const branch of session.user.getUserBranches()) {
                                if (noDescendants.indexOf(branch) !== -1) {
                                    reportsFiltered.push(dynamoReport);
                                    found = true;
                                    break;
                                }
                            }
                            if (found) {
                                continue;
                            }
                        }

                        // Check for branch assignment with parents
                        if (descendants.length > 0) {
                            for (const branch of session.user.getUserBranchesWithParents()) {
                                if (descendants.indexOf(branch) !== -1) {
                                    reportsFiltered.push(dynamoReport);
                                    break;
                                }
                            }
                            continue;
                        }
                    }
                }
            }

            dynamoReports = reportsFiltered;
        }

        // convert reports to aamon format
        const reports: Report[] = await this.reportsService.convertDynamoReportsMetadataToAamonReportsMetadata(dynamoReports);

        return reports;
    }

}
