package de.appsist.service.lbd.queries;

import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.logging.Logger;
import org.vertx.java.core.logging.impl.LoggerFactory;

public class LBDSparQLQueries
{
    // needed prefix string to build SparQL queries
    private static final String PREFIXSTRING = "BASE <http://www.appsist.de/ontology/> PREFIX app: <http://www.appsist.de/ontology/>";

    // Eventbus address
    private static final String SPARQLREQUESTS = "appsist:requests:semwiki";
    
    private static final Logger log = LoggerFactory.getLogger(LBDSparQLQueries.class);
    
    public static void getMeasuresForStates(String states, EventBus eb,
            Handler<Message<String>> stringHandler)
    // corresponds to SPARQL rule in "APPsist Adaptionsregeln" document section 1.1
    {
        String sparqlQuery = PREFIXSTRING + "SELECT DISTINCT ?massnahme ?zustand "
                + "WHERE { VALUES ?zustand " + states + " ?zustand app:bedingt ?massnahme .}";
        sendSparQLQuery(sparqlQuery, eb, stringHandler);
    }



    // find all stations which are part of a list of workplace groups
    // String apg looks like {app:state1 ... app:statem app:machine1 ... app:machinen
    // app:station1 ... stationo}
    public static void getContentsForStatesMachinesStations(String ids, String stelle, EventBus eb,
            Handler<Message<String>> stringHandler)
    {
        String sparqlQuery = PREFIXSTRING + " SELECT DISTINCT ?inhalt ?vorschau WHERE { VALUES ?i "
                + ids + "{ ?inhalt app:informiertUeber ?i . ?inhalt app:hasPreview ?vorschau "
                + "{{ ?inhalt app:informiertUeber ?i . FILTER NOT EXISTS {?inhalt app:hatZielgruppe ?y}}"
                + "UNION {?inhalt app:informiertUeber ?i . ?inhalt app:hatZielgruppe " + stelle
                + " }}}}";
        
        sendSparQLQuery(sparqlQuery, eb, stringHandler);
    }

    // find all stations which are part of a list of workplace groups
    // String items looks like {app:occupationgroup}
    public static void getContentsForOccupationgroupProducts(String items, EventBus eb,
            Handler<Message<String>> stringHandler)
    {

        String sparqlQuery = PREFIXSTRING + "SELECT DISTINCT ?inhalt " + "WHERE { VALUES ?items "
                + items + " ?inhalt app:informiertUeber ?item .} "
                + " UNION { ?item rdfs:subClassOf* app:Beschaeftigungsgruppe ."
                + "?bgsubclass rdfe:subClassOf* ?bg; app:hatFunktion ?fkt . ?inhalt app:informiertUeber ?fkt . } }";

        sendSparQLQuery(sparqlQuery, eb, stringHandler);
    }

    private static void sendSparQLQuery(String sparQLQuery, EventBus eb,
            Handler<Message<String>> stringHandler)
    {
        JsonObject sQuery = new JsonObject().putString("query", sparQLQuery);
        //System.out.println("[LBD] - sending query:"+sparQLQuery );
        eb.send(SPARQLREQUESTS, new JsonObject().putObject("sparql", sQuery),
                stringHandler);
    }

    // ---------------------------------------------------------------
    //
    // Queries for Nebenzeit
    //
    // ---------------------------------------------------------------

    // find all stations which are part of a list of workplace groups
    // Rule 3.1.2.2 in document "Adaptionsregeln"
    // String items looks like {<http://www.appsist.de/ontology/Maschinenbediener> ...}
    public static void getItemsContent(String items, String stelle, EventBus eb,
            Handler<Message<String>> stringHandler)
    {
        String sparqlQuery = PREFIXSTRING
                + " SELECT DISTINCT ?inhalt ?vorschau  WHERE { VALUES ?item " + items
                + " {{ ?inhalt app:informiertUeber ?item . FILTER NOT EXISTS {?inhalt app:hatZielgruppe ?y} OPTIONAL {?inhalt app:hasPreview ?vorschau}} "
                + " UNION { ?inhalt app:informiertUeber ?item . ?inhalt app:hatZielgruppe " + stelle
                + " OPTIONAL {?inhalt app:hasPreview ?vorschau}"
                + "}}"
                + " UNION { ?item rdfs:subClassOf* app:Stelle . ?item rdfs:subClassOf* ?bg . "
                + " ?bg app:hatAufgabe ?fkt . ?inhalt app:informiertUeber ?fkt . OPTIONAL {?inhalt app:hasPreview ?vorschau} }}";
        log.info("getItemsContent SPARQL: " + sparqlQuery);
        sendSparQLQuery(sparqlQuery, eb, stringHandler);
    }
    
    // find all stations which are part of a list of workplace groups
    // Rule 3.1.2.3 in document "Adaptionsregeln"
    // String items looks like {<http://www.appsist.de/ontology/Maschinenbediener> ...}
    public static void getProductionItemsRelevantForPosition(String stelle, EventBus eb,
            Handler<Message<String>> stringHandler)
    {
        String sparqlQuery = PREFIXSTRING
                + " SELECT DISTINCT ?inhalt ?vorschau  WHERE { VALUES ?stelle "
                + stelle + " ?stelle app:interagiertMit ?pg . { "
                + "{ ?inhalt app:informiertUeber ?pg . ?inhalt app:hasPreview ?vorschau } UNION {"
                + " ?pgs app:isPartOf ?pg . ?inhalt app:informiertUeber ?pgs . ?inhalt app:hasPreview ?vorschau}}}";
        sendSparQLQuery(sparqlQuery, eb, stringHandler);
    }

    // find all stations which are part of a list of workplace groups
    // Rule 3.1.2.4 in document "Adaptionsregeln"
    // String items looks like {<http://www.appsist.de/ontology/Maschinenbediener> ...}
    public static void getProductionItemsRelevantForPositionMeasures(String stelle, EventBus eb,
            Handler<Message<String>> stringHandler)
    {
        String sparqlQuery = PREFIXSTRING
                + " SELECT DISTINCT ?inhalt ?vorschau WHERE { VALUES ?stelle "
                + stelle + " ?stelle app:hatAufgabe ?aufgabe . "
                + " ?aufgabe app:hatMassnahme ?massnahme . " + " ?massnahme app:benoetigt ?pg ."
                + " ?inhalt app:informiertUeber ?pg . ?inhalt app:hasPreview ?vorschau}";
        sendSparQLQuery(sparqlQuery, eb, stringHandler);
    }
}
