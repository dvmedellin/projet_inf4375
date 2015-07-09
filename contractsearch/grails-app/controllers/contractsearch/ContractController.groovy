package contractsearch

class ContractController {

    def index() { 
        render(Contract.list() as grails.converters.JSON)
    }
}
