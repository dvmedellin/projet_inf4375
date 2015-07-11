package contractsearch

class ContractIndexingJob {
    static triggers = {
        cron name:'cronTrigger', startDelay:10000, cronExpression: '0/10 * * * * ?'
    }

    def execute() {
        // execute job
        'csvtodb.py'.execute()
    }
}
