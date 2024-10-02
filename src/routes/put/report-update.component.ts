import { Request } from 'express';

import { LearningPlans, ReportManagerInfoResponse } from '../../models/report-manager';
import { ReportManagerSwitcher } from '../../models/report-switcher';
import SessionManager from '../../services/session/session-manager.session';
import { ReportCheckChanges } from '../../reports/components/ReportCheckChanges';
import { loggerFactory } from '../../services/logger/logger-factory';

export class ReportUpdateComponent {
    req: Request;
    session: SessionManager;

    constructor(req: Request, session: SessionManager) {
        this.req = req;
        this.session = session;
    }

    public async getReportUpdate(): Promise<ReportManagerInfoResponse> {
        let response: ReportManagerInfoResponse;
        const reportHandler = await ReportManagerSwitcher(this.session, this.req.params.id_report);
        const data = this.req.body;
        // For learningplans convert id into number to avoid wrong previous saved type as string
        if (data.learningPlans && data.learningPlans.learningPlans.length > 0) {
            data.learningPlans.learningPlans = data.learningPlans.learningPlans.map((lp: LearningPlans) => {
                lp.id = parseInt(lp.id as string, 10);
                return lp;
            });
        }
        const infoBefore = reportHandler.info;
        response = await reportHandler.updateReport(data);
        const infoAfter = reportHandler.info;
        if (response.success) {
            const processChanges = new ReportCheckChanges(loggerFactory.buildLogger('[ReportUpdate]'), this.session.hydra);
            await processChanges.checkForUpdateReportEvent(infoBefore, infoAfter);
        }
        return response;
    }

}
