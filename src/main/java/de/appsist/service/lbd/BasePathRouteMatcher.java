package de.appsist.service.lbd;

import org.vertx.java.core.Handler;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.http.RouteMatcher;

/**
 * Extension for the route matcher prepending a base path to all HTTP end points.
 * @author simon.schwantzer(at)im-c.de
 */
public class BasePathRouteMatcher extends RouteMatcher {
	private final String basePath;  
	
	/**
	 * Creates a route matcher with the given base path.
	 * @param basePath Base path to prepend to all endpoints.
	 */
	public BasePathRouteMatcher(String basePath) {
		this.basePath = basePath;
	}
	
	/**
     * Return the base path for this route matcher.
     * 
     * @return Base path for all requests, e.g., "/services/myservices"
     */
	public String getBasePath() {
        return basePath;
    }

    @Override
	public void handle(HttpServerRequest request) {
		super.handle(request);
	}

	@Override
	public RouteMatcher get(String pattern, Handler<HttpServerRequest> handler) {
		return super.get(basePath + pattern, handler);
	}

	@Override
	public RouteMatcher put(String pattern, Handler<HttpServerRequest> handler) {
		return super.put(basePath + pattern, handler);
	}

	@Override
	public RouteMatcher post(String pattern, Handler<HttpServerRequest> handler) {
		return super.post(basePath + pattern, handler);
	}

	@Override
	public RouteMatcher delete(String pattern, Handler<HttpServerRequest> handler) {
		return super.delete(basePath + pattern, handler);
	}

	@Override
	public RouteMatcher options(String pattern, Handler<HttpServerRequest> handler) {
		return super.options(basePath + pattern, handler);
	}

	@Override
	public RouteMatcher head(String pattern, Handler<HttpServerRequest> handler) {
		return super.head(basePath + pattern, handler);
	}

	@Override
	public RouteMatcher trace(String pattern, Handler<HttpServerRequest> handler) {
		return super.trace(basePath + pattern, handler);
	}

	@Override
	public RouteMatcher connect(String pattern, Handler<HttpServerRequest> handler) {
		return super.connect(basePath + pattern, handler);
	}

	@Override
	public RouteMatcher patch(String pattern, Handler<HttpServerRequest> handler) {
		return super.patch(basePath + pattern, handler);
	}

	@Override
	public RouteMatcher all(String pattern, Handler<HttpServerRequest> handler) {
		return super.all(basePath + pattern, handler);
	}

	@Override
	public RouteMatcher getWithRegEx(String regex, Handler<HttpServerRequest> handler) {
		return super.getWithRegEx(basePath + regex, handler);
	}

	@Override
	public RouteMatcher putWithRegEx(String regex, Handler<HttpServerRequest> handler) {
		return super.putWithRegEx(basePath + regex, handler);
	}

	@Override
	public RouteMatcher postWithRegEx(String regex, Handler<HttpServerRequest> handler) {
		return super.postWithRegEx(basePath + regex, handler);
	}

	@Override
	public RouteMatcher deleteWithRegEx(String regex, Handler<HttpServerRequest> handler) {
		return super.deleteWithRegEx(basePath + regex, handler);
	}

	@Override
	public RouteMatcher optionsWithRegEx(String regex, Handler<HttpServerRequest> handler) {
		return super.optionsWithRegEx(basePath + regex, handler);
	}

	@Override
	public RouteMatcher headWithRegEx(String regex, Handler<HttpServerRequest> handler) {
		return super.headWithRegEx(basePath + regex, handler);
	}

	@Override
	public RouteMatcher traceWithRegEx(String regex, Handler<HttpServerRequest> handler) {
		return super.traceWithRegEx(basePath + regex, handler);
	}

	@Override
	public RouteMatcher connectWithRegEx(String regex, Handler<HttpServerRequest> handler) {
		return super.connectWithRegEx(basePath + regex, handler);
	}

	@Override
	public RouteMatcher patchWithRegEx(String regex, Handler<HttpServerRequest> handler) {
		return super.patchWithRegEx(basePath + regex, handler);
	}

	@Override
	public RouteMatcher allWithRegEx(String regex,Handler<HttpServerRequest> handler) {
		return super.allWithRegEx(basePath + regex, handler);
	}
}
