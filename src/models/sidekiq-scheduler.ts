import Config from '../config';
import { BadRequestException } from '../exceptions/bad-request.exception';
import { SidekiqSchedulerItem, SidekiqSchedulerWorkerClass } from '../reports/interfaces/extraction.interface';
import { Utils } from '../reports/utils';
import { isOClockTime, isValidTimezone } from '../shared/customValidators';
import { redisFactory } from '../services/redis/RedisFactory';

/* Create the job item to store in Redis so that Sidekiq can read it and perform the action needed
The final object should be like this
{
    "class": "AddNewTask",
    "args": [
      "22438cc6f3d1f9c425d870ce95695e4c334d2621",
      "0 0 * * *",
      "\/usr\/bin\/wget  --spider --no-check-certificate -q -t 1 '${redis.API_GATEWAY}/aamon/placeholder-endpoint?hash_id=${idReport}'",
      "hydra.docebosaas.com"
    ],
    "retry": true,
    "enqueued_at": 1633512315.463147
  }

or can be this
{
    "class": "RemoveTask",
    "args": [
      "22438cc6f3d1f9c425d870ce95695e4c334d2621",
      "hydra.docebosaas.com"
    ],
    "retry": true,
    "enqueued_at": 25443512315.789147
  }
*/


export class SidekiqScheduler {
  workerClass: SidekiqSchedulerWorkerClass;
  args: string[];

  public constructor(readonly platform: string) {}

  public async activateScheduling(idReport: string, planningOption: {scheduleFrom: string; startHour: string; timezone: string; }): Promise<void> {

    const utils = new Utils();

    // Convert the time to the selected Timezone and UTC
    const { scheduleFrom, startHour, timezone } = planningOption;

    if (!isOClockTime(startHour)) {
      throw new BadRequestException(`The startHour param for the reportId ${idReport} has a wrong format`);
    }

    if (!isValidTimezone(timezone)) {
      throw new BadRequestException(`The timezone param for the reportId ${idReport} isn't not valid`);
    }

    const finalStartHour = utils.getDateTimeInTimezoneAndUTC(scheduleFrom, startHour, timezone).format('H:m');

    // Cron Timing
    const timeSplit = finalStartHour.split(':');
    const cronHour = timeSplit[0];
    const cronMin = timeSplit[1];
    const cronTiming = `${cronMin} ${cronHour} * * *`;
    const redis = redisFactory.getRedis();

    // Command URL to launch from Sidekiq
    const apiGatewayUrl = await redis.getAPIGatewayParam(this.platform);
    if (apiGatewayUrl === '' || apiGatewayUrl === null) {
      throw new Error(`The API Gateway URL for the Sidekiq scheduling is not set in Redis DB 0`);
    }

    const apiGatewayPort = await redis.getAPIGatewayPortParam(this.platform);

    const config = new Config();
    const internalUrlPrefix = config.internalUrlPrefix;

    let url = `https://${apiGatewayUrl}${internalUrlPrefix}`;
    if (apiGatewayPort === 80) {
      url =  `http://${apiGatewayUrl}${internalUrlPrefix}`;
    }

    const getAPIGatewaySSLVerify = await redis.getAPIGatewaySSLVerify(this.platform);
    let sslVerify = '--no-check-certificate';
    if (getAPIGatewaySSLVerify !== 0) {
      sslVerify = '';
    }
    const command = `\/usr\/bin\/wget --header='Host: ${this.platform}' --spider ${sslVerify} -q -t 1 '${url}/reports/${idReport}/sidekiq-schedulation/${this.platform}'`;

    this.workerClass =  SidekiqSchedulerWorkerClass.ADD_NEW_TASK;
    this.args = [idReport, cronTiming, command, this.platform];

    await this.storeItem();

  }

  // This method stores the "RemoveTask" Jobs in Redis
  public async removeScheduling(idReport: string): Promise<void> {

    this.workerClass =  SidekiqSchedulerWorkerClass.REMOVE_TASK;
    this.args = [idReport, this.platform];

    await this.storeItem();
  }

  // Store the Sidekiq scheduler Job in Redis (both Worker Class)
  public async storeItem(): Promise<void> {
    const utils = new Utils();

    const item: SidekiqSchedulerItem = {
      class: this.workerClass,
      args: this.args,
      retry: true,
      enqueued_at: utils.getMicroTime() as number
    };

    const sidekiq = await redisFactory.getSidekiqClient();
    await sidekiq.storeSidekiqSchedulerItem(item);
  }

}

