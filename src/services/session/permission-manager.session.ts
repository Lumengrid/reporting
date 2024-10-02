import UserManager from './user-manager.session';

export default class PermissionManager {
    public canViewReportPermission(user: UserManager) {
        return user.canViewReport() || user.canUpdateReport();
    }
    public canUpdateReportPermission(user: UserManager) {
        return user.canUpdateReport();
    }
    public canBeManagerPermission(user: UserManager) {
        return user.canBeManager();
    }
}
