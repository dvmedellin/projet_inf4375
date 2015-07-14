import contractsearch.*
import org.apache.http.client.fluent.*
import org.apache.entity.*
import groovy.json.*

class BootStrap {

    def init = { servletContext ->
        def contracts = Request.Get('http://localhost:9200/contracts/_search/?size=20000')
            .execute().returnContent().asString()

        def slurper = new JsonSlurper()
        def result = slurper.parseText(contracts).hits.hits

        result.each {
            new Contract(
                fournisseur:result['_source']['FOURNISSEUR'].toString(),
                no_dossier:result['_source']['NO_DOSSIER'].toString(),
                direction:result['_source']['DIRECTION'].toString(),
                service:result['_source']['SERVICE'].toString(),
                description:result['_source']['DESCRIPTION'].toString(),
                activite:result['_source']['ACTIVITÉ'].toString(),
                no_decision:result['_source']['NO_DECISION'].toString(),
                approbateur:result['_source']['APPROBATEUR'].toString(),
                date:result['_source']['DATE'].toString(),
                montant:result['_source']['MONTANT'].toString(),
                repartition:result['_source']['REPARTITION'].toString()).save(flush:true)
        }
    }
    def destroy = {
    }
}
