package contractsearch

import org.apache.http.client.fluent.*
import org.apache.entity.*
import groovy.json.*

class ContractIndexingJob {
    static triggers = {
        cron name:'cronTrigger', startDelay:10000, cronExpression: '0/15 * * * * ?'
    }

    def execute() {
        //print('executing')
        'python3 ./csvtodb.py'.execute()
    }
}
