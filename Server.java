import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import io.rsocket.AbstractRSocket;
import io.rsocket.Payload;
import io.rsocket.RSocketFactory;
import io.rsocket.transport.netty.server.WebsocketServerTransport;
import io.rsocket.util.DefaultPayload;
import org.reactivestreams.Publisher;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import java.sql.*;


public class Server {

    private static Logger log = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);

    static {
        log.setLevel(Level.INFO);
    }

    private static String url = "jdbc:postgresql://localhost:5432/postgresdb?user=egor&password=test";

    public static void main(String[] args) {
        RSocketFactory.receive()
                .acceptor((setup, sendingSocket) -> Mono.just(new DefaultSimpleService()))
                .transport(WebsocketServerTransport.create(8801))
                .start()
                .block()
                .onClose()
                .block();
    }

    public static final class DefaultSimpleService extends AbstractRSocket {

        private Connection getConnection() throws SQLException {
            return DriverManager.getConnection(url);
        }

        @Override
        public Mono<Void> fireAndForget(Payload payload) {
            System.out.println(payload);
            log.info("Got FireAndForget in Server");
            log.info(payload.getDataUtf8());

            try (Connection conn = getConnection();
                 Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery("SELECT * FROM users WHERE user_id = 1")) {

                while (rs.next()) {
                    System.out.print("Column 1 returned ");
                    System.out.println(rs.getString(1));
                }

            } catch (SQLException e) {
                log.error("Exception occurred while executing SQL query", e);
            }
            return Mono.empty();
        }

        @Override
        public Mono<Payload> requestResponse(Payload payload) {
            System.out.println(payload);
            log.info("Got requestResponse in Server");
            log.info(payload.getDataUtf8());

            try (Connection conn = getConnection();
                 Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery("SELECT * FROM users WHERE user_id = 1")) {

                while (rs.next()) {
                    System.out.print("Column 1 returned ");
                    System.out.println(rs.getString(1));
                }

            } catch (SQLException e) {
                log.error("Exception occurred while executing SQL query", e);
                return Mono.error(new RuntimeException("Failed to execute query: " + e.getMessage()));
            }

            return Mono.just(payload.getDataUtf8())
                    .map(payloadString -> MessageMapper.jsonToMessage(payloadString))
                    .map(message -> message.message + " | RequestResponse from Server")
                    .map(responseText -> new Message(responseText))
                    .map(responseMessage -> MessageMapper.messageToJson(responseMessage))
                    .map(responseJson -> DefaultPayload.create(responseJson));
        }

        @Override
        public Flux<Payload> requestStream(Payload payload) {
            System.out.println(payload);
            log.info("got requestStream in Server");
            log.info(payload.getDataUtf8());

            try (Connection conn = getConnection();
                 Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery("SELECT * FROM users WHERE user_id = 1")) {

                while (rs.next()) {
                    System.out.print("Column 1 returned ");
                    System.out.println(rs.getString(1));
                }

            } catch (SQLException e) {
                log.error("Exception occurred while executing SQL query ", e);
                return Flux.error(new RuntimeException("Failed to execute query: " + e.getMessage()));
            }

            return Mono.just(payload.getDataUtf8())
                    .map(payloadString -> MessageMapper.jsonToMessage(payloadString))
                    .flatMapMany(msg -> Flux.range(0, 5)
                            .map(count -> msg.message + " | requestStream from Server" + count)
                            .map(responseText -> new Message(responseText))
                            .map(responseMessage -> MessageMapper.messageToJson(responseMessage)))
                    .map(message -> DefaultPayload.create(message));
        }

        @Override
        public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
            System.out.println(payloads);
            log.info("Got requestChannel in Server");

            try (Connection conn = getConnection();
                 Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery("SELECT * FROM users WHERE user_id = 1")) {

                while (rs.next()) {
                    System.out.print("Column 1 returned ");
                    System.out.println(rs.getString(1));
                }

            } catch (SQLException e) {
                log.error("Exception occurred while executing SQL query", e);
                return Flux.error(new RuntimeException("Failed to execute query: " + e.getMessage()));
            }

            return Flux.from(payloads)
                    .map(payload -> payload.getDataUtf8())
                    .map(payloadString -> {
                        log.info(payloadString);
                        return MessageMapper.jsonToMessage(payloadString);
                    })
                    .flatMap(msg -> Flux.range(0, 2)
                            .map(count -> msg.message + " | requestChannel from Server" + count)
                            .map(responseText -> new Message(responseText))
                            .map(responseMessage -> MessageMapper.messageToJson(responseMessage)))
                    .map(message -> DefaultPayload.create(message));
        }
    }
}
