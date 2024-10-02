export interface RefreshTokenItem {
    platform: string;
    currentMonthlyTokens: number;
    currentDailyTokens: number;
    lastRequest: string;
    lastReset: string;
}

export interface PlatformRefreshTokens {
    platformMonthlyTokens: number;
    platformDailyTokens: number;
}