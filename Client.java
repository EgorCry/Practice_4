import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.transport.netty.client.WebsocketClientTransport;
import io.rsocket.util.DefaultPayload;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;


public class Client {
    private static Logger log = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);

    static {
        log.setLevel(Level.INFO);
    }

    public static void main(String[] args) {
        final Client client = new Client();
        final RSocket rSocket = client.connect();

        final JFrame frame = new JFrame();
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.setSize(500, 300);
        frame.setLayout(new FlowLayout(FlowLayout.CENTER, 10, 10));

        final JPanel panel = new JPanel();
        panel.setPreferredSize(new Dimension(300, 150));
        panel.setBackground(Color.lightGray);
        panel.setLayout(new GridLayout(2, 2, 10, 10));

        final JButton fireAndForgetBtn = new JButton("Fire and Forget");
        final JButton requestResponseBtn = new JButton("Request-Response");
        final JButton requestStreamBtn = new JButton("Request-Stream");
        final JButton requestChannelBtn = new JButton("Request-Channel");

        final ActionListener AListener = new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                switch (e.getActionCommand()) {
                    case "Fire and Forget":
                        client.fireAndForget(rSocket);
                        break;
                    case "Request-Response":
                        client.requestResponse(rSocket);
                        break;
                    case "Request-Stream":
                        client.requestStream(rSocket);
                        break;
                    case "Request-Channel":
                        client.requestChannel(rSocket);
                        break;
                }
            }
        };

        fireAndForgetBtn.addActionListener(AListener);
        requestResponseBtn.addActionListener(AListener);
        requestStreamBtn.addActionListener(AListener);
        requestChannelBtn.addActionListener(AListener);

        panel.add(fireAndForgetBtn);
        panel.add(requestResponseBtn);
        panel.add(requestStreamBtn);
        panel.add(requestChannelBtn);

        frame.add(panel);
        frame.setVisible(true);
    }

    private RSocket connect() {
        return RSocketFactory.connect()
                .transport(WebsocketClientTransport.create(8801))
                .start()
                .block();
    }

    private void fireAndForget(RSocket rSocket) {
        log.info("Sending fire and forget from client");
        try {
            Flux.just(new Message("fire and forget JAVA client!"))
                    .map(msg -> MessageMapper.messageToJson(msg))
                    .map(json -> DefaultPayload.create(json))
                    .flatMap(message -> rSocket.fireAndForget(message))
                    .blockLast();
        } catch (Exception e) {
            log.error("Error occurred in fireAndForget", e);
        }
    }

    private void requestResponse(RSocket rSocket) {
        log.info("sending request-response from client");
        try {
            Flux.just(new Message("RequestResponse from JAVA client!"))
                    .map(msg -> MessageMapper.messageToJson(msg))
                    .map(json -> DefaultPayload.create(json))
                    .flatMap(message -> rSocket.requestResponse(message))
                    .map(payload -> payload.getDataUtf8())
                    .doOnNext(payloadString -> {
                        log.info("Got response in JAVA client");
                        log.info(payloadString);
                    })
                    .blockLast();
        } catch (Exception e) {
            log.error("Error occurred in requestResponse", e);
        }
    }

    private void requestStream(RSocket rSocket) {
        log.info("Sending request-stream from client");
        try {
            Flux.just(new Message("RequestStream from JAVA client!"))
                    .map(msg -> MessageMapper.messageToJson(msg))
                    .map(json -> DefaultPayload.create(json))
                    .flatMap(message -> rSocket.requestStream(message))
                    .map(payload -> payload.getDataUtf8())
                    .doOnNext(payloadString -> log.info(payloadString))
                    .blockLast();
        } catch (Exception e) {
            log.error("Error occurred in requestStream", e);
        }
    }

    private void requestChannel(RSocket rSocket) {
        log.info("Sending request-channel from client");
        try {
            final Flux<Payload> requestPayload = Flux.range(0, 5)
                    .map(count -> new Message("RequestChannel from JAVA client! " + count))
                    .map(msg -> {
                        log.info("Sending message: {}", msg.message);
                        return MessageMapper.messageToJson(msg);
                    })
                    .map(json -> DefaultPayload.create(json));

            rSocket
                    .requestChannel(requestPayload)
                    .map(payload -> payload.getDataUtf8())
                    .doOnNext(payloadString -> log.info(payloadString))
                    .blockLast();
        } catch (Exception e) {
            log.error("Error occurred in requestChannel", e);
        }
    }
}
