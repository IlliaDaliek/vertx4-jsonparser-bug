import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

import io.vertx.core.Vertx;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.parsetools.JsonParser;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.codec.BodyCodec;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;

import static io.netty.handler.codec.http.HttpHeaderValues.APPLICATION_JSON;
import static io.vertx.core.http.HttpHeaders.ACCEPT;
import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(VertxExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class JsonParserReproducer {

    private static final int mockServerPort = 7575;
    private static final int REQUEST_TIMEOUT = 904800000;
    private static final int ROWS_NUMBER = 50000;
    private static final String STREAM_PRODUCER_URI = "/producer";
    private static final String responseBodyTemplate = "{ \"instanceId\":\"1ed91465-7a75-4d96-bf34-4dfbd89790d5\", \"source\":\"GLOBAL\", \"updatedDate\":\"2020-06-15T11:07:48.563Z\", \"deleted\":\"false\", \"suppressFromDiscovery\":\"false\"}";

    @BeforeAll
    void setUp(Vertx vertx, VertxTestContext testContext) {
        startMockServer(vertx, testContext);
    }

    @AfterAll
    void tearDown(Vertx vertx, VertxTestContext testContext) {
        vertx.close().onSuccess(v -> testContext.completeNow())
                .onFailure(testContext::failNow);
    }

    @Test
    void shouldNotThrowJsonParseException_whenProcessingStreamInAsyncMode(Vertx vertx, VertxTestContext testContext) {
        WebClient client = WebClient.create(vertx);
        JsonParser parser = JsonParser.newParser().objectValueMode();
        AtomicInteger recordsCount = new AtomicInteger();
        parser.handler(event -> {
            if (recordsCount.incrementAndGet() % 1000 == 0) {
                System.out.println("Records returned so far: " + recordsCount.get());
            }
        });
        parser.exceptionHandler(testContext::failNow);

        AtomicBoolean isPaused = new AtomicBoolean();


        // Creates a tasks that imitates the work with JsonParser in async mode.
        for (int i = 0; i < 5; i++) {
            new Thread(() -> {
                while (true) {
                    if (isPaused.get()) {
                        parser.fetch(Long.MAX_VALUE);
                    } else {
                        parser.pause();
                        isPaused.set(true);
                    }
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }).start();
        }

        client.getAbs("http://localhost:" + mockServerPort + STREAM_PRODUCER_URI)
                .putHeader(ACCEPT.toString(), APPLICATION_JSON.toString())
                .timeout(REQUEST_TIMEOUT)
                .as(BodyCodec.jsonStream(parser))
                .send()
                .onSuccess(res -> {
                    assertEquals(ROWS_NUMBER, recordsCount.get());
                    testContext.completeNow();
                })
                .onFailure(testContext::failNow);
    }

    /**
     * Creates and starts the mock server with defined router for producing json entity stream.
     */
    private void startMockServer(Vertx vertx, final VertxTestContext testContext) {
        HttpServer mockServer = vertx.createHttpServer();
        mockServer.requestHandler(setupStreamProducer(vertx))
                .listen(mockServerPort, asyncResult -> {
                    if (asyncResult.succeeded()) {
                        testContext.completeNow();
                    } else {
                        testContext.failNow(asyncResult.cause());
                    }
                });
    }

    private Router setupStreamProducer(Vertx vertx) {
        Router router = Router.router(vertx);
        router.get(STREAM_PRODUCER_URI)
                .handler(this::handleStreamProducerGetResponse);
        return router;
    }

    private void handleStreamProducerGetResponse(RoutingContext routingContext) {
        HttpServerResponse response = routingContext.response();
        response.setStatusCode(200);
        response.putHeader(HttpHeaders.CONTENT_TYPE, APPLICATION_JSON);
        response.setChunked(true);
        int rows = ROWS_NUMBER;
        while (rows > 0) {
            response.write(responseBodyTemplate);
            rows--;
        }
        response.end();
    }

}
