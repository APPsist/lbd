package de.appsist.service.lbd;

import java.util.*;

import org.vertx.java.core.*;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.logging.Logger;
import org.vertx.java.core.logging.impl.LoggerFactory;
import org.vertx.java.platform.Verticle;

import com.fasterxml.jackson.databind.ObjectMapper;

import de.appsist.commons.event.UserOnlineEvent;
import de.appsist.commons.misc.StatusSignalConfiguration;
import de.appsist.commons.misc.StatusSignalSender;
import de.appsist.commons.util.EventUtil;
import de.appsist.service.auth.connector.AuthServiceConnector;
import de.appsist.service.auth.connector.model.Session;
import de.appsist.service.iid.server.connector.IIDConnector;
import de.appsist.service.iid.server.model.*;
import de.appsist.service.lbd.addresses.Addresses;
import de.appsist.service.lbd.queries.LBDSparQLQueries;
import de.appsist.service.measuresservice.model.LocalState;
import de.appsist.service.measuresservice.queries.BasicSparQLQueries;
import de.appsist.service.usermodel.model.EmployeeDevelopmentGoals;

/*
 * This verticle is executed with the module itself, i.e. initializes all components required by the service.
 * The super class provides two main objects used to interact with the container:
 * - <code>vertx</code> provides access to the Vert.x runtime like event bus, servers, etc.
 * - <code>container</code> provides access to the container, e.g, for accessing the module configuration an the logging mechanism.  
 */
public class LBDMainVerticle
    extends Verticle
{
	private JsonObject config;
    private BasePathRouteMatcher routeMatcher;
    private String basePath;
    private EventBus eb;
    private AuthServiceConnector authConn;
    private static final Logger log = LoggerFactory.getLogger(LBDMainVerticle.class);

    private IIDConnector conn;
    // this map stores sessionIds(keys) and userIds (values)
    private final Map<String, String> sessionUserID = new HashMap<String, String>();

    // this map stores items to search learningcontents for
    private final Map<String, LinkedHashMap<String, String>> sessionSideContents = new HashMap<String, LinkedHashMap<String, String>>();

    private final Map<String, String> sessionCurrentPosition = new HashMap<String, String>();
    private final Map<String, Set<String>> sessionWorkplaceGroups = new HashMap<String, Set<String>>();
    private final Map<String, Set<String>> sessionDevelopmentGoals = new HashMap<String, Set<String>>();
    private final Map<String, Set<String>> sessionStations = new HashMap<String, Set<String>>();
    private final Map<String, Set<String>> sessionMachines = new HashMap<String, Set<String>>();
    private final Map<String, Set<String>> sessionStates = new HashMap<String, Set<String>>();
    private final Map<String, Set<LocalState>> sessionLocalStates = new HashMap<String, Set<LocalState>>();

    private final Set<String> usersInNebenzeit = new HashSet<String>();

    // storage objects for Nebenzeit
    private final Map<String, EmployeeDevelopmentGoals> sessionDGobject = new HashMap<String, EmployeeDevelopmentGoals>();

    // eventbus adress to receive and trigger the start of a learning session
    public final String LBDTRIGGERADRESS = Addresses.START_LEARNING_SESSION;

    // default preview image
    private final static String defaultPreviewImage = "defaultLIPreview.jpg";

    // to reduce calls to the ontology we store already retrieved labels in
    private Map<String, String> knowledgeItemLabels = new HashMap<String, String>();

    // activate debugging
    private final boolean isDebug = true;

    // specifiy directory for external content
    // String externalContentDirectory =
    // "/Users/midi01/Work/svn_repositories/AppSist-svn/content/allsorts/externalContent/";
    String externalContentDirectory = "/services/cds/static/externalContent/";
	@Override
	public void start() {
        if (container.config() != null && container.config().size() > 0) {
			config = container.config();
		} else {
            log.warn("Warning: No configuration applied! Using default settings.");
			config = getDefaultConfiguration();
		}


		/*
		 * In this method the verticle is registered at the event bus in order to receive messages. 
		 */
        this.eb = vertx.eventBus();
        // initialize Authentication Service Connector
        authConn = new AuthServiceConnector(this.eb, AuthServiceConnector.SERVICE_ID);

        conn = new IIDConnector(this.eb, IIDConnector.DEFAULT_ADDRESS);
        // /*
        // * This block initializes the HTTP interface for the service.
        // */
        this.basePath = config.getObject("webserver").getString("basePath");
        initializeHTTPRouting();
        vertx.createHttpServer().requestHandler(routeMatcher)
                .listen(config.getObject("webserver").getInteger("port"));
		
        if (isDebug)
            log.info(
                "APPsist service \"Lernbedarfsdienst\" has been initialized with the following configuration:\n"
                        + config.encodePrettily());
        initializeEventBusHandler();
        // log.info("testrun");
        // String sid = "abrakadabra";
        // Set<String> wpgTest = new HashSet<String>();
        // wpgTest.add("app:Großserien_6:_Normzylinder");
        // wpgTest.add("app:Großserien_4:_Stromregelventile_1");
        // sessionWorkplaceGroups.put(sid, wpgTest);
        // requestStationsInWorkplaceGroups(sid);
        JsonObject statusSignalObject = config.getObject("statusSignal");
        StatusSignalConfiguration statusSignalConfig;
        if (statusSignalObject != null) {
          statusSignalConfig = new StatusSignalConfiguration(statusSignalObject);
        } else {
          statusSignalConfig = new StatusSignalConfiguration();
        }

        StatusSignalSender statusSignalSender =
          new StatusSignalSender("lbd", vertx, statusSignalConfig);
        statusSignalSender.start();

	}
	
	@Override
	public void stop() {
        if (isDebug)
            log.info("APPsist service \"Lernbedarfsdienst\" has been stopped.");
	}
	
	/**
	 * Create a configuration which used if no configuration is passed to the module.
	 * @return Configuration object.
	 */
	private static JsonObject getDefaultConfiguration() {
		JsonObject defaultConfig =  new JsonObject();
		JsonObject webserverConfig = new JsonObject();
        webserverConfig.putNumber("port", 7088);
        webserverConfig.putString("basePath", "/services/lbd");
		webserverConfig.putString("statics", "www");
		defaultConfig.putObject("webserver", webserverConfig);
		return defaultConfig;
	}
	
	/**
	 * In this method the handlers for the event bus are initialized.
	 */
	private void initializeEventBusHandler() {

        // handler which reacts on Trigger message
        Handler<Message<JsonObject>> lbdTriggeredHandler = new Handler<Message<JsonObject>>()
        {
			
			@Override
			public void handle(Message<JsonObject> message) {
				JsonObject messageBody = message.body();
                String sessionId = messageBody.getString("sid");
                String token = messageBody.getString("token");
                switch (message.address()) {
                    case LBDTRIGGERADRESS :
                        getUserId(sessionId, token);
					break;
                    default :
                        // TODO store unregistered access attempts
                        break;
				}
			}
		};
		
		// Handlers are always registered for a specific address. 
        vertx.eventBus().registerHandler(this.LBDTRIGGERADRESS, lbdTriggeredHandler);

        // handler for machinestate changes
        Handler<Message<JsonObject>> userOnlineEventHandler = new Handler<Message<JsonObject>>()
        {

            public void handle(Message<JsonObject> jsonMessage)
            {
                if (isDebug) {
                    log.debug("[Lernbedarfs-Dienst] - MainVerticle content of user online event: "
                            + jsonMessage.body());
                }
                UserOnlineEvent uoe = EventUtil.parseEvent(jsonMessage.body().toMap(),
                        UserOnlineEvent.class);
                sessionUserID.put(uoe.getSessionId(), uoe.getUserId());
            }
        };
        vertx.eventBus().registerHandler(Addresses.USER_ONLINE,
                userOnlineEventHandler);

        // handler for users switching workstate
        Handler<Message<JsonObject>> userActivitySwitchHandler = new Handler<Message<JsonObject>>()
        {
            @Override
            public void handle(Message<JsonObject> jsonMessage)
            {
                JsonObject body = jsonMessage.body();
                String sessionId = body.getString("sessionId");
                String userId = sessionUserID.get(sessionId);
                boolean isNebenzeit = body.getString("activity").equals("side");
                if (isNebenzeit) {
                    usersInNebenzeit.add(sessionId);
                }
                else {
                    usersInNebenzeit.remove(sessionId);
                }
                if (isDebug) {
                    log.debug("[Lernbedarfs-Dienst] userActivitySwitchHandler jsonMessage"
                            + jsonMessage.body());
                    if (isNebenzeit) {
                        log.debug("[Lernbedarfs-Dienst] Mitarbeiter '" + userId
                                + "' befindet sich in Nebenzeit");
                    }
                    else {
                        log.debug("[Lernbedarfs-Dienst] Mitarbeiter '" + userId
                                + "' befindet sich in Haupttätigkeit");
                    }
                }
                requestUserInformation(sessionId, userId, "token");
            }
        };
        this.eb.registerHandler(Addresses.USER_ACTIVITY_SWITCH, userActivitySwitchHandler);
	}
	
	/**
	 * In this method the HTTP API build using a route matcher.
	 */
	private void initializeHTTPRouting() {
        routeMatcher = new BasePathRouteMatcher(this.basePath);

		final String staticFileDirecotry = config.getObject("webserver").getString("statics");
		
		routeMatcher.get("/testSparql", new Handler<HttpServerRequest>(){

            @Override
            public void handle(final HttpServerRequest request)
            {
                Handler<Message<String>> stringHandler = new Handler<Message<String>>(){

                    @Override
                    public void handle(Message<String> messageString)
                    {
                        if (isDebug)
                            log.debug(messageString.body());
                        JsonObject result = new JsonObject(messageString.body());
                        Set<String> resultSet = new HashSet<String>();
                        JsonArray messageArray = result.getObject("results").getArray("bindings");
                        Iterator<Object> messageArrayIterator = messageArray.iterator();
                        while (messageArrayIterator.hasNext()) {
                            Object currentArrayEntry = messageArrayIterator.next();
                            if (currentArrayEntry instanceof JsonObject) {
                                resultSet.add(((JsonObject) currentArrayEntry).getObject("inhalt")
                                        .getString("value"));
                            }
                            else {
                                if (isDebug)
                                    log.error("Expected JsonObject. Found "
                                        + currentArrayEntry.getClass());
                            }
                        }
                        if (isDebug)
                            log.debug(resultSet);
                        request.response().end(messageString.body());

                    }
                    
                };
                LBDSparQLQueries.getContentsForStatesMachinesStations(
                        "{app:LoctiteLeer app:FettWenig app:S20 app:S10}",
                        sparqlPrefix("festo/AnlagenoperatorGPZ"), eb, stringHandler);
            }
		    
        });
		
		/*
		 * The following rules are applied in the order given during the initialization.
		 * The first rule which matches the request is applied and the latter rules are ignored. 
		 */
		
		/*
		 * This rule applies to all request of type GET to a path like "/entries/abc".
		 * The path segment "abc" is being made available as request parameter "id".
		 */
		routeMatcher.get("/entries/:id", new Handler<HttpServerRequest>() {
			
			@Override
			public void handle(HttpServerRequest request) {
				String id = request.params().get("id");
				request.response().end("Received request for entry '" + id + "'.");
			}
		});
		routeMatcher.get("/addSproutKnowledgeItem/",	new Handler<HttpServerRequest>(){

			@Override
			public void handle(HttpServerRequest event) {
				// TODO Auto-generated method stub
				addSproutKnowledgeItem();
				event.response().end();
			}
		});

		
		routeMatcher.post("/addSproutKnowledgeItem/",	new Handler<HttpServerRequest>(){

			@Override
			public void handle(HttpServerRequest event) {
				// TODO Auto-generated method stub
				addSproutKnowledgeItem();
				event.response().end();
			}
		});
		
		
		/*
		 * This entry serves files from a directory specified in the configuration.
		 * In the default configuration, the files are served from "src/main/resources/www", which is packaged with the module. 
		 */
		routeMatcher.getWithRegEx("/.*", new Handler<HttpServerRequest>() {
			
			@Override
			public void handle(HttpServerRequest request) {
				request.response().sendFile(staticFileDirecotry + request.path());
			}
		});
	}
	
		private void addSproutKnowledgeItem() {
		// TODO Auto-generated method stub
			for (String sessionId: sessionUserID.keySet()){
				List<ServiceItem> serviceItemList = new ArrayList<ServiceItem>();
				LearningObjectItemBuilder loib = new LearningObjectItemBuilder();
	            String sendMessageActionAddress = Addresses.OPEN_EXTERNAL_CONTENT;
	            String contentId = "/services/cds/static/externalContent/5_DSBC_Basiswissen/index.html?skipcheck";
	            JsonObject jo = new JsonObject();
	            jo.putString("sessionId", sessionId).putString("token", "token").putString("processId", contentId);
	            JsonObject bo = new JsonObject();
	            bo.putObject("body", jo);
	            if (isDebug) {
	                log.debug("[lbd] bo: " + bo.encodePrettily());
	            }

	            SendMessageAction sma = new SendMessageAction(sendMessageActionAddress, bo);
	            loib.setId("lbd-14").setPriority(14)
	            	.isExternal()
	            	.setMimeType(detectMimeType(contentId))
	                    .setTitle("DSBC Basiswissen")
	                    .setService("lbd")
	                    .setImageUrl(externalContentDirectory + "thumbnails/5_DSBC_Basiswissen.jpg")
	                    .setAction(sma);
	            serviceItemList.add(loib.build());
	            conn.addServiceItems(sessionId, serviceItemList, null);			}
			// Build Learning Service Item
			// add to KnowledgeItem Catalogue
		}
    private void getUserId(String sessionId, final String token)
    {
        AsyncResultHandler<Session> sessionHandler = new AsyncResultHandler<Session>()
        {

            @Override
            public void handle(AsyncResult<Session> sessionRequest)
            {
                if (sessionRequest.succeeded()) {
                    Session session = sessionRequest.result();
                    if (null != session) {
                        requestUserInformation(session, token);
                    }
                }
            }

        };
        authConn.getSession(sessionId, token, sessionHandler);
    }

    private void requestUserInformation(String sessionId, String userId, String token)
    {
        this.sessionUserID.put(sessionId, userId);
        JsonObject request = new JsonObject();
        request.putString("sid", sessionId);
        request.putString("userId", userId);
        request.putString("token", token);
        Handler<Message<JsonObject>> userInformationHandler = new Handler<Message<JsonObject>>()
        {

            @Override
            public void handle(Message<JsonObject> message)
            {
                JsonObject messageBody = message.body();
                if (isDebug)
                    log.debug("lbd - requestUserInformation");
                processUserInformation(messageBody);

            }

        };
        if (isDebug)
            log.debug("Sending request for userInformation" + request);
        eb.send(Addresses.USER_GET_INFORMATION, request, userInformationHandler);
    }

    private void requestUserInformation(Session session, String token)
    {
        this.sessionUserID.put(session.getId(), session.getUserId());
        JsonObject request = new JsonObject();
        request.putString("sid", session.getId());
        request.putString("userId", session.getUserId());
        request.putString("token", token);
        Handler<Message<JsonObject>> userInformationHandler = new Handler<Message<JsonObject>>(){

            @Override
            public void handle(Message<JsonObject> message)
            {
                JsonObject messageBody = message.body();
                processUserInformation(messageBody);

            }
            
        };
        eb.send(Addresses.USER_GET_INFORMATION, request, userInformationHandler);
    }

    private void processUserInformation(JsonObject messageBody)
    {
            log.debug("[Lernbedarf-Dienst] - processUserInformation" + messageBody);

        JsonObject userInformation = messageBody.getObject("userInformation");
        // store information about user in corresponding maps
        String sessionId = messageBody.getString("sid");
        try {
            // de.appsist.service.usermodel.model.User user = new ObjectMapper().readValue(
            // userInformation.getString("userObject"),
            // de.appsist.service.usermodel.model.User.class);
            Set<String> apgs;
            Set<String> devgoals;
            EmployeeDevelopmentGoals edg;
            String employeeType;
            // if (null != user) {
            // apgs = user.getWorkplaceGroups();
            // devgoals = user.getDevelopmentGoals();
            // edg = user.getDevelopmentGoalsObject();
            // employeeType = user.getEmployeeType();
            //
            // }
            // else {
                apgs = new ObjectMapper().readValue(userInformation.getString("workplaceGroups"),
                        Set.class);
                devgoals = new ObjectMapper()
                        .readValue(userInformation.getString("developmentGoals"), Set.class);
                edg = new ObjectMapper().readValue(
                        userInformation.getString("developmentGoalsObject"),
                        EmployeeDevelopmentGoals.class);
            employeeType = userInformation.getString("employeeType");
            // }
            
            if (isDebug) {
                // log.debug("[Lernbedarf-Dienst] - user" + user.toString());
                log.debug("[Lernbedarf-Dienst] - currentPosition: " + employeeType);
                log.debug("[lbd] - developmentgoals: " + devgoals);
                log.debug("[lbd] - workplaceGroups: " + apgs);
                log.debug("[lbd] - employeeDevelopmentGoals: " + edg.getItems());
            }

            sessionCurrentPosition.put(sessionId, employeeType);
            sessionDevelopmentGoals.put(sessionId, devgoals);
            sessionWorkplaceGroups.put(sessionId, apgs);
            sessionDGobject.put(sessionId, edg);

        }
        catch (Exception e) {
            e.printStackTrace();
        }
        if (isDebug) {
            log.debug("[Lernbedarfs-Dienst] - userInformation currentWorkstate: "
                    + usersInNebenzeit.contains(sessionId));

        }

        if (!usersInNebenzeit.contains(sessionId)) {
            requestStationsInWorkplaceGroups(sessionId);
        }
        else {
            buildLearningMaterialListSide(sessionId);
        }
    }

    // rule 3.1.1
    private void requestStationsInWorkplaceGroups(final String sessionId)
    {
        try{
            // transform JsonString to JavaObject
            
            String workplaceGroups = "{";
                for (String workplaceGroup: sessionWorkplaceGroups.get(sessionId)){
                workplaceGroups += " " + sparqlPrefix(workplaceGroup);
                }
            workplaceGroups+="}";
            
            Handler<Message<String>> stationsInWorkplaceGroupsHandler = new Handler<Message<String>>() {

                @Override
                public void handle(Message<String> stringMessage)
                {
                    JsonObject result = new JsonObject(stringMessage.body());
                    Set<String> resultSet = new HashSet<String>();
                    JsonArray messageArray = result.getObject("results").getArray("bindings");
                    Iterator<Object> messageArrayIterator = messageArray.iterator();
                    while (messageArrayIterator.hasNext()) {
                        Object currentArrayEntry = messageArrayIterator.next();
                        if (currentArrayEntry instanceof JsonObject) {
                            resultSet.add(((JsonObject) currentArrayEntry).getObject("device")
                                    .getString("value"));
                        }
                        else {
                            if (isDebug)
                                log.debug("Expected JsonObject. Found "
                                        + currentArrayEntry.getClass());
                        }
                    }
                    sessionStations.put(sessionId, resultSet);
                    requestCurrentStatesForStations(sessionId, resultSet);
                }
                
            };
            

            BasicSparQLQueries.getStationsInWorkplaceGroups(workplaceGroups, eb,
                    stationsInWorkplaceGroupsHandler);
            
        } catch(Exception e){
            e.printStackTrace();
        }
        
    }

    private void requestCurrentStatesForStations(String sessionId, Set<String> stationSet)
    {
        String stateStation = "{";
        for (String station : stationSet) {

            stateStation += "(<http://www.appsist.de/ontology/FunkionsfaehigerZustand> "
                    + sparqlPrefix(station) + ")";
        }
        stateStation += "}";
        requestLocalStates(sessionId, stateStation);
    }

    // Rule 3.1.1.1 in document "Adaptionsregeln" ("Lokale Zustaende")
    private void requestLocalStates(final String sessionId, String stateStation)
    {
        Handler<Message<String>> localStatesHandler = new Handler<Message<String>>(){

            @Override
            public void handle(Message<String> messageString)
            {
                JsonObject result = new JsonObject(messageString.body());
                JsonArray messageArray = result.getObject("results").getArray("bindings");
                // log.debug(messageArray.encodePrettily());
                Iterator<Object> jsonArrayIterator = messageArray.iterator();
                Set<LocalState> lsSet = new HashSet<LocalState>();

                Set<String> localStates = new HashSet<String>();
                while (jsonArrayIterator.hasNext()) {
                    Object iteratorObject = jsonArrayIterator.next();
                    if (iteratorObject instanceof JsonObject) {
                        JsonObject jsonObject = (JsonObject) iteratorObject;
                        LocalState ls = new LocalState(
                                sparqlPrefix(jsonObject.getObject("z").getString("value")),
                                sparqlPrefix(jsonObject.getObject("station").getString("value")),
                                jsonObject.getObject("p").getString("value"));
                        lsSet.add(ls);
                        localStates.add(ls.getState());
                    }
                }
                sessionLocalStates.put(sessionId, lsSet);
                sessionStates.put(sessionId, localStates);
                requestMachinesInWorkplaceGroups(sessionId);
            }
        };
        if (isDebug) {
            log.debug("[Lernbedarfsdienst] zustand station tupel: " + stateStation);
        }
        BasicSparQLQueries.getLocalStates(stateStation, eb, localStatesHandler);
    }

    // Rule 1.2 in document "Adaptionsregeln"
    private void requestMachinesInWorkplaceGroups(final String sessionId)
    {

        Handler<Message<String>> machinesInWorkplaceGroupsHandler = new Handler<Message<String>>()
        {

            @Override
            public void handle(Message<String> stringMessage)
            {
                JsonObject result = new JsonObject(stringMessage.body());
                Set<String> resultSet = new HashSet<String>();
                JsonArray messageArray = result.getObject("results").getArray("bindings");
                // log.debug("[Lernbedarf-Dienst] - Anlagen und Stationen in der APG: "
                // + messageArray.encodePrettily());
                Iterator<Object> messageArrayIterator = messageArray.iterator();
                while (messageArrayIterator.hasNext()) {
                    Object currentArrayEntry = messageArrayIterator.next();
                    if (currentArrayEntry instanceof JsonObject) {
                        resultSet.add(((JsonObject) currentArrayEntry).getObject("device")
                                .getString("value"));
                    }
                    else {
                        if (isDebug)
                            log.debug("Expected JsonObject. Found " + currentArrayEntry.getClass());
                    }
                }
                sessionMachines.put(sessionId, resultSet);
                requestLearningMaterialListMain(sessionId);
            }

        };
        Set<String> workplaceGroups = this.sessionWorkplaceGroups.get(sessionId);
        String workplaceGroupsString = "{";
        for (String workplaceGroup : workplaceGroups) {
            workplaceGroupsString += " " + sparqlPrefix(workplaceGroup);
        }
        workplaceGroupsString += "}";
        BasicSparQLQueries.getStationsInWorkplaceGroups(workplaceGroupsString, eb,
                machinesInWorkplaceGroupsHandler);
    }

    private void requestLearningMaterialListMain(final String sessionId)
    {
        Handler<Message<String>> learningMaterialListMainHandler = new Handler<Message<String>>(){

            @Override
            public void handle(Message<String> messageString)
            {
                JsonObject result = new JsonObject(messageString.body());
                Map<String, String> resultMap = new HashMap<String, String>();
                log.debug("[Lernbedarf-Dienst] - messageString: " + result.encodePrettily());
                JsonArray messageArray = result.getObject("results").getArray("bindings");
                Iterator<Object> messageArrayIterator = messageArray.iterator();
                while (messageArrayIterator.hasNext()) {
                    Object currentArrayEntry = messageArrayIterator.next();
                    if (currentArrayEntry instanceof JsonObject) {
                        JsonObject currentArrayEntryJson = (JsonObject) currentArrayEntry;
                        String inhalt = currentArrayEntryJson.getObject("inhalt")
                                .getString("value");
                        String vorschau = currentArrayEntryJson.getObject("vorschau")
                                .getString("value", defaultPreviewImage);
                        resultMap.put(inhalt, vorschau);
                    }
                    else {
                        if (isDebug)
                            log.error("Expected JsonObject. Found "
                                + currentArrayEntry.getClass());
                    }
                }

                log.debug("[Lernbedarf-Dienst resultMap=" + resultMap);
                retrieveLabelsFor(sessionId, resultMap);
                // buildLearningMaterialList(sessionId, resultSet);
            }
        };
        Set<String> idSet = new HashSet<String>();
        idSet.addAll(this.sessionStates.get(sessionId));
        idSet.addAll(this.sessionMachines.get(sessionId));
        idSet.addAll(this.sessionStations.get(sessionId));
        String ids = "{";
        for (String id : idSet) {
            ids += " " + sparqlPrefix(id);
        }
        ids += "}";
        String stelle = this.sessionCurrentPosition.get(sessionId);
        if (isDebug) {
            log.debug("[Lernbedarf-Dienst] - ids: " + ids);
            log.debug("[Lernbedarf-Dienst] - stelle: " + stelle);
        }

        LBDSparQLQueries.getContentsForStatesMachinesStations(ids, sparqlPrefix(stelle), eb,
                learningMaterialListMainHandler);
    }

    private void buildLearningMaterialList(String sessionId, Map<String, String> contentIds)
    {
        if (isDebug) {
            log.debug("[lbd] - Finished learning material lookup");
            log.debug("[lbd] - Found: " + contentIds);
        }

        List<ServiceItem> serviceItemList = new ArrayList<ServiceItem>();
        int priority = 1;
        for (String contentId : contentIds.keySet()) {
            LearningObjectItemBuilder loib = new LearningObjectItemBuilder();
            String sendMessageActionAddress = Addresses.OPEN_EXTERNAL_CONTENT;
            String originalContentId = contentId;
            if (contentId.startsWith("http://www.appsist.de/ontology/")) {
                sendMessageActionAddress = Addresses.START_LEARNING_OBJECT;
            }
            else {
                loib.isExternal();
                loib.setMimeType(detectMimeType(contentId));
                if (!contentId.startsWith("http")) {
                    // according to specification contentId has to be of the form
                    // file:///static/xxx
                    // and has to be expanded to file://path/to/content/xxx
                    contentId = contentId.replaceFirst("file:///static/", externalContentDirectory);
                }
            }
            JsonObject jo = new JsonObject();
            jo.putString("sessionId", sessionId).putString("token", "token").putString("processId",
                    contentId);
            JsonObject bo = new JsonObject();
            bo.putObject("body", jo);
            if (isDebug) {
                log.debug("[lbd] bo: " + bo.encodePrettily());
            }

            SendMessageAction sma = new SendMessageAction(sendMessageActionAddress, bo);
            loib.setId("lbd-" + priority).setPriority(priority++)
                    .setTitle(prettifyContentId(contentId))
                    .setService("lbd")
                    .setImageUrl(
externalContentDirectory + "thumbnails/"
                            + contentIds.get(originalContentId))
                    .setAction(sma);

            serviceItemList.add(loib.build());
        }
        if (isDebug) {
            log.debug("[lbd] - ServiceItemList #entries " + serviceItemList.size());
        }
        // always add assessment test service item
        serviceItemList.add(buildSproutAssessmentServiceItem(sessionId));
        
        conn.purgeServiceItems(sessionId, "lbd", null);
        conn.addServiceItems(sessionId, serviceItemList, null);

    }

    private void buildLearningMaterialListSide(final String sessionId)
    {
        EmployeeDevelopmentGoals edg = this.sessionDGobject.get(sessionId);
        // rule 3.1.2.5 add mandatory contents for user

        LinkedHashMap<String, String> contentIDPreviewMap = new LinkedHashMap<String, String>();
        String previewFilename ="MPSStationRoboterProgrammierhandbuch.png";
        for (String contentId : edg.getContents()) {
            log.info("Nebentätigkeit Vertiefungsmaterial hinzugefügt: " + contentId);
            
            if ("file:///static/7_Basiswissen_Pneumatik_HM/index.html?skipcheck".equals(contentId)){
            	previewFilename="7_Basiswissen_Pneumatik_HM.png";
            }
            
            if ("http://www.appsist.de/ontology/demonstrator/b2280e82-2d6f-4888-9edc-6cf5ccc8bd08".equals(contentId)){
            	previewFilename="b2280e82-2d6f-4888-9edc-6cf5ccc8bd08.png";
            }
                      
            contentIDPreviewMap.put(contentId, previewFilename);
        }
        this.sessionSideContents.put(sessionId, contentIDPreviewMap);

        final LinkedHashMap<String, String> finalLhm = contentIDPreviewMap;

        Handler<Message<String>> handleItemsContent = new Handler<Message<String>>()
        {

            @Override
            public void handle(Message<String> arg0)
            {
                if (isDebug) {
                    log.debug("[Lernbedarf-Dienst] - handleItemsContent:" + arg0.body());
                }

                // build HashMap with measure/label
                JsonObject jsonObject = new JsonObject(arg0.body());
                JsonArray jsonArray = jsonObject.getObject("results").getArray("bindings");

                Iterator<Object> jsonArrayIterator = jsonArray.iterator();
                while (jsonArrayIterator.hasNext()) {
                    Object currentObject = jsonArrayIterator.next();
                    if (currentObject instanceof JsonObject) {
                        JsonObject currentJsonObject = (JsonObject) currentObject;
                        String item, vorschau = "";
                        if (null != currentJsonObject.getObject("inhalt")) {
                            item = currentJsonObject.getObject("inhalt").getString("value");
                        }
                        else {
                            continue;
                        }
                        if (null != currentJsonObject.getObject("vorschau")) {
                            vorschau = currentJsonObject.getObject("vorschau").getString("value",
                                    defaultPreviewImage);
                        }
                        else {
                            vorschau = defaultPreviewImage;
                        }

                        finalLhm.put(item, vorschau);
                    }
                }

                requestProductionItemsRelevantForPosition(sessionId, finalLhm);
            }
        };
        String sideItems = "{";
        sideItems = sideItems + sparqlPrefix(edg.getPosition());
        for (String prodItem : edg.getItems()) {
            sideItems += sparqlPrefix(prodItem);
        }
        sideItems += "}";
        log.debug("calling adaption rule 3.1.2.2 with items: " + sideItems);
        // call rule 3.1.2.2
        LBDSparQLQueries.getItemsContent(sideItems,
                sparqlPrefix(this.sessionCurrentPosition.get(sessionId)), eb, handleItemsContent);
    }

    private void requestProductionItemsRelevantForPosition(final String sessionId,
            final LinkedHashMap<String, String> suggestedLearningItems)
    {

        Handler<Message<String>> handleProductionItemsRelevantForPositions = new Handler<Message<String>>()
        {

            @Override
            public void handle(Message<String> arg0)
            {
                if (isDebug)
                    log.debug("handleContentRelevantForPositions:" + arg0.body());
                // build HashMap with measure/label
                JsonObject jsonObject = new JsonObject(arg0.body());
                JsonArray jsonArray = jsonObject.getObject("results").getArray("bindings");

                Iterator<Object> jsonArrayIterator = jsonArray.iterator();
                while (jsonArrayIterator.hasNext()) {
                    Object currentObject = jsonArrayIterator.next();
                    if (currentObject instanceof JsonObject) {
                        JsonObject currentJsonObject = (JsonObject) currentObject;
                        String item = currentJsonObject.getObject("inhalt").getString("value");
                        String vorschau = currentJsonObject.getObject("vorschau").getString("value",
                                defaultPreviewImage);
                        suggestedLearningItems.put(item, vorschau);
                    }
                }

                requestProductionItemsUsedInMeasuresRelevantForPosition(sessionId,
                        suggestedLearningItems);
            }
        };
        String positions = "{";
        EmployeeDevelopmentGoals edg = this.sessionDGobject.get(sessionId);
        positions += sparqlPrefix(edg.getPosition());
        positions += "}";
        // log.debug("[LBD] - calling adaption rule 3.1.2.3 with items: " + positions);
        // call rule 3.1.2.2
        LBDSparQLQueries.getProductionItemsRelevantForPosition(positions, eb,
                handleProductionItemsRelevantForPositions);
    }

    private void requestProductionItemsUsedInMeasuresRelevantForPosition(final String sessionId,
            final LinkedHashMap<String, String> suggestedLearningItems)
    {

        Handler<Message<String>> handleProductionItemsRelevantForPositionMeasures = new Handler<Message<String>>()
        {

            @Override
            public void handle(Message<String> arg0)
            {
                if (isDebug)
                    log.debug("handleContentRelevantForPositions:" + arg0.body());
                // build HashMap with measure/label
                JsonObject jsonObject = new JsonObject(arg0.body());
                JsonArray jsonArray = jsonObject.getObject("results").getArray("bindings");

                Iterator<Object> jsonArrayIterator = jsonArray.iterator();
                while (jsonArrayIterator.hasNext()) {
                    Object currentObject = jsonArrayIterator.next();
                    if (currentObject instanceof JsonObject) {
                        JsonObject currentJsonObject = (JsonObject) currentObject;
                        String item = currentJsonObject.getObject("inhalt").getString("value");
                        String vorschau = currentJsonObject.getObject("vorschau").getString("value",
                                defaultPreviewImage);
                        suggestedLearningItems.put(item, vorschau);
                    }
                }

                orderSuggestedLearningItems(sessionId, suggestedLearningItems);
            }
        };
        String positions = "{";
        EmployeeDevelopmentGoals edg = this.sessionDGobject.get(sessionId);
        positions += sparqlPrefix(edg.getPosition());
        positions += "}";
        // log.debug("[LBD] - calling adaption rule 3.1.2.4 with items: " + positions);
        // call rule 3.1.2.4
        LBDSparQLQueries.getProductionItemsRelevantForPositionMeasures(positions, eb,
                handleProductionItemsRelevantForPositionMeasures);
    }

    // requests which entries of suggestedLearningItems have already been read from usermodel
    // reorders entries in suggestedLearningItems
    // (Unread mandatory, Unread, Read)
    private void orderSuggestedLearningItems(String sessionId,
            LinkedHashMap<String, String> suggestedLearningItems)
    {
        // TODO
        // Method requests which entries of suggestedLearningItems have already been read from
        // usermodel
        // remove read entries from suggestedLearningItems
        // re-insert read entries
        this.sessionSideContents.put(sessionId, suggestedLearningItems);

        // find labels for entries
        retrieveLabelsFor(sessionId, suggestedLearningItems);
    }

    // this method surrounds full Ontology URIs with less/greater than characters

    private String sparqlPrefix(String original)
    {
        if (null == original) {
            return "";
        }
        original = original.trim();
        String result = "";
        if (!original.startsWith("<")) {
            result = "<" + original;
        }
        if (!original.endsWith(">")) {
            result += ">";
        }
        return result;
    }

    private String prettifyContentId(String contentId)
    {
        if (this.knowledgeItemLabels.keySet().contains(contentId)) {
            return this.knowledgeItemLabels.get(contentId);
        }
        else {
            return contentId;
        }
    }

    private void addKnowledgeItemLabel(String itemId, String itemLabel){
        this.knowledgeItemLabels.put(itemId.replace("file:///static/", externalContentDirectory),
                itemLabel);
    }

    private void retrieveLabelsFor(final String sessionId,
            final Map<String, String> finalMeasureMap)
    {
        Handler<Message<String>> handleMeasureLabels = new Handler<Message<String>>()
        {

            @Override
            public void handle(Message<String> arg0)
            {
                // build HashMap with measure/label
                JsonObject jsonObject = new JsonObject(arg0.body());
                JsonArray jsonArray = jsonObject.getObject("results").getArray("bindings");
                Iterator<Object> jsonArrayIterator = jsonArray.iterator();
                while (jsonArrayIterator.hasNext()) {
                    Object currentObject = jsonArrayIterator.next();
                    if (currentObject instanceof JsonObject) {
                        JsonObject currentJsonObject = (JsonObject) currentObject;
                        String measure = currentJsonObject.getObject("oc").getString("value");
                        String label = currentJsonObject.getObject("label").getString("value");
                        addKnowledgeItemLabel(measure, label);
                    }
                }

                buildLearningMaterialList(sessionId, finalMeasureMap);
            }
        };
        List<String> finalMeasureList = new ArrayList<String>();
        for (String m : finalMeasureMap.keySet()) {
            if (!this.knowledgeItemLabels.containsKey(m)) {
                finalMeasureList.add(m);
                // this.knowledgeItemLabels.put(m, m);
            }
        }

        String measuresForQuery = "{";
        for (String measure : finalMeasureList) {
            measuresForQuery = measuresForQuery + " <" + measure + ">";
        }
        measuresForQuery += "}";
        BasicSparQLQueries.getLabelFor(measuresForQuery, "de", eb, handleMeasureLabels);
    }

    private String detectMimeType(String contentId)
    {
        String result = "unknown";
        String fileExtension = contentId.substring(contentId.lastIndexOf(".") + 1).toLowerCase();

        switch (fileExtension) {
            case "pdf" :
                result = "application/pdf";
                break;
            case "doc" :
                result = "application/msword";
                break;
            default :
                result = "text/html";
                break;
        }
        return result;
    }
    
    private ServiceItem buildSproutAssessmentServiceItem(String sessionId){
            LearningObjectItemBuilder loib = new LearningObjectItemBuilder();
            String sendMessageActionAddress = Addresses.OPEN_EXTERNAL_CONTENT;
            String contentId = "/services/cds/static/externalContent/d69a640a-5526-42dd-b121-d8102b1ad4be/index.html";
            JsonObject jo = new JsonObject();
            jo.putString("sessionId", sessionId).putString("token", "token").putString("processId", contentId);
            JsonObject bo = new JsonObject();
            bo.putObject("body", jo);
            if (isDebug) {
                log.debug("[lbd] bo: " + bo.encodePrettily());
            }

            SendMessageAction sma = new SendMessageAction(sendMessageActionAddress, bo);
            loib.setId("lbd-13").setPriority(13)
            	.isExternal()
            	.setMimeType(detectMimeType(contentId))
                    .setTitle("Übung: Bauteile des Zylinders")
                    .setService("lbd")
                    .setImageUrl(externalContentDirectory + "thumbnails/d69a640a-5526-42dd-b121-d8102b1ad4be.png")
                    .setAction(sma);

            return loib.build();
    }
}



