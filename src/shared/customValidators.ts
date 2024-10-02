import moment from 'moment-timezone';

/**
 * Check if the timezone string is a valid timezone
 * @param timezone
 */
export const isValidTimezone = (timezone: string) => {
    return moment.tz.zone(timezone) !== null;
};

/**
 * Check if the time string has the correct format (eg. '10:00', '11:00'..)
 * (No minutes different from 00 allowed)
 * @param time
 */
export const isOClockTime = (time: string) => {
    const regex = new RegExp('^([0-1]?[0-9]|2[0-3]):00$');
    return regex.test(time);
};
