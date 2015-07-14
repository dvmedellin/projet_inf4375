package contractsearch

import org.apache.http.client.fluent.*
import org.apache.http.client.utils.*
import org.apache.entity.*

class ContractController {

    def index() {
        render(Contract.list() as grails.converters.JSON)
    }

    def search() {
        def searchResults = Request.Get(new URIBuilder('http://localhost:9200/contracts/_search')
        .addParameter('q',params.q ?: '*').build()).execute().returnContent().asString()

        render(text:searchResults,type:'application/json',encoding:'utf-8')
    }
}
