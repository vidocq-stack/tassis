/*
 * Copyright (c) 2026 Yann Blazart, Antoine Sabot-Durand and the Vidocq contributors
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License 2.0 which is available at
 * https://www.eclipse.org/legal/epl-2.0/
 *
 * This Source Code may also be made available under the following Secondary
 * Licenses when the conditions for such availability set forth in the Eclipse
 * Public License, v. 2.0 are satisfied: GNU General Public License, version 2
 * or any later version, which is available at
 * https://www.gnu.org/licenses/old-licenses/gpl-2.0.html
 *
 * It is also made available under the European Union Public Licence v. 1.2,
 * which is available at
 * https://joinup.ec.europa.eu/collection/eupl/eupl-text-eupl-12
 *
 * SPDX-License-Identifier: EPL-2.0 OR EUPL-1.2 OR GPL-2.0-or-later
 */
package io.vidocq.tassis.amqp.internal.jms;

import io.vidocq.tassis.amqp.internal.messaging.AmqpMessage;
import io.vidocq.tassis.amqp.internal.messaging.AmqpSequence;
import io.vidocq.tassis.amqp.internal.messaging.AmqpValue;
import io.vidocq.tassis.amqp.internal.messaging.ApplicationProperties;
import io.vidocq.tassis.amqp.internal.messaging.Data;
import io.vidocq.tassis.amqp.internal.messaging.Header;
import io.vidocq.tassis.amqp.internal.messaging.MessageAnnotations;
import io.vidocq.tassis.amqp.internal.messaging.Properties;
import io.vidocq.tassis.amqp.internal.messaging.Section;
import io.vidocq.tassis.amqp.internal.types.AmqpChar;
import io.vidocq.tassis.amqp.internal.types.AmqpDecodeException;
import io.vidocq.tassis.amqp.internal.types.Binary;
import io.vidocq.tassis.amqp.internal.types.Symbol;
import io.vidocq.tassis.amqp.internal.types.Timestamp;
import io.vidocq.tassis.amqp.internal.types.UByte;
import io.vidocq.tassis.amqp.internal.types.UInt;
import io.vidocq.tassis.amqp.internal.types.ULong;
import io.vidocq.tassis.amqp.internal.types.UShort;
import io.vidocq.tassis.core.TassisQueue;
import io.vidocq.tassis.core.TassisTopic;
import io.vidocq.tassis.core.internal.message.TassisBytesMessage;
import io.vidocq.tassis.core.internal.message.TassisMapMessage;
import io.vidocq.tassis.core.internal.message.TassisMessage;
import io.vidocq.tassis.core.internal.message.TassisObjectMessage;
import io.vidocq.tassis.core.internal.message.TassisStreamMessage;
import io.vidocq.tassis.core.internal.message.TassisTextMessage;
import jakarta.jms.DeliveryMode;
import jakarta.jms.Destination;
import jakarta.jms.JMSException;
import jakarta.jms.MessageEOFException;
import jakarta.jms.Queue;
import jakarta.jms.Topic;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * How a Jakarta Messaging message travels as an AMQP 1.0 message, and back.
 *
 * <p>AMQP defines no such mapping. These are the conventions of Apache Qpid JMS, described by
 * the OASIS "AMQP JMS Mapping" working drafts, which the brokers that serve JMS clients over
 * AMQP follow: a message annotation says which of the JMS message types a message is, two more
 * say what kind of destination its addresses name, and the header fields of JMS are spread over
 * the header and the properties sections.
 */
public final class JmsMessageCodec {

    static final Symbol MSG_TYPE = Symbol.of("x-opt-jms-msg-type");
    static final Symbol DESTINATION_TYPE = Symbol.of("x-opt-jms-dest");
    static final Symbol REPLY_TO_TYPE = Symbol.of("x-opt-jms-reply-to");
    static final Symbol DELIVERY_TIME = Symbol.of("x-opt-delivery-time");

    // The values of x-opt-jms-msg-type.
    static final byte TYPE_MESSAGE = 0;
    static final byte TYPE_OBJECT = 1;
    static final byte TYPE_MAP = 2;
    static final byte TYPE_BYTES = 3;
    static final byte TYPE_STREAM = 4;
    static final byte TYPE_TEXT = 5;

    // The values of x-opt-jms-dest and x-opt-jms-reply-to.
    static final byte KIND_QUEUE = 0;
    static final byte KIND_TOPIC = 1;
    static final byte KIND_TEMPORARY_QUEUE = 2;
    static final byte KIND_TEMPORARY_TOPIC = 3;

    static final Symbol SERIALIZED_OBJECT = Symbol.of("application/x-java-serialized-object");
    static final Symbol OCTET_STREAM = Symbol.of("application/octet-stream");

    private static final String DELIVERY_COUNT = "JMSXDeliveryCount";
    private static final String GROUP_ID = "JMSXGroupID";
    private static final String GROUP_SEQUENCE = "JMSXGroupSeq";

    private static final int DEFAULT_PRIORITY = 4;

    /** What a value of AMQP becomes when Jakarta Messaging has no type that can hold it. */
    private static final Object UNSUPPORTED = new Object();

    private JmsMessageCodec() {
    }

    // ---- to AMQP -------------------------------------------------------------

    /**
     * Returns the AMQP form of a message.
     *
     * @param message a message nobody else uses: its body is read, which for a bytes or a stream
     *        message moves its read position
     */
    public static AmqpMessage toAmqp(TassisMessage message) throws JMSException {
        Map<Object, Object> annotations = new LinkedHashMap<>();
        Symbol contentType = null;
        List<Section> body;
        switch (message) {
            case TassisTextMessage text -> {
                annotations.put(MSG_TYPE, TYPE_TEXT);
                body = List.of(new AmqpValue(text.getText()));
            }
            case TassisBytesMessage bytes -> {
                annotations.put(MSG_TYPE, TYPE_BYTES);
                contentType = OCTET_STREAM;
                // The body as the message holds it, which nothing writes to: no copy on the way.
                body = List.of(new Data(Binary.wrap(bytes.content())));
            }
            case TassisMapMessage map -> {
                annotations.put(MSG_TYPE, TYPE_MAP);
                Map<String, Object> entries = new LinkedHashMap<>();
                for (String name : Collections.list(map.getMapNames())) {
                    entries.put(name, toAmqpValue(map.getObject(name)));
                }
                body = List.of(new AmqpValue(entries));
            }
            case TassisStreamMessage stream -> {
                annotations.put(MSG_TYPE, TYPE_STREAM);
                List<Object> fields = new ArrayList<>();
                stream.reset();
                try {
                    while (true) {
                        fields.add(toAmqpValue(stream.readObject()));
                    }
                } catch (MessageEOFException end) {
                    // the way a stream message says it has no more fields
                }
                body = List.of(new AmqpSequence(fields));
            }
            case TassisObjectMessage object -> {
                annotations.put(MSG_TYPE, TYPE_OBJECT);
                contentType = SERIALIZED_OBJECT;
                byte[] serialized = object.serialized();
                body = List.of(new Data(new Binary(serialized == null ? new byte[0] : serialized)));
            }
            default -> {
                annotations.put(MSG_TYPE, TYPE_MESSAGE);
                body = List.of();
            }
        }

        long timestamp = message.getJMSTimestamp();
        long expiration = message.getJMSExpiration();
        Long ttl = expiration != 0 && timestamp != 0 && expiration > timestamp ? expiration - timestamp : null;
        Long deliveryCount = message.propertyExists(DELIVERY_COUNT)
                ? Long.valueOf(Math.max(0, message.getIntProperty(DELIVERY_COUNT) - 1)) : null;
        // Written even when they are the defaults of JMS: those of AMQP are not the same.
        Header header = new Header(message.getJMSDeliveryMode() == DeliveryMode.PERSISTENT, message.getJMSPriority(),
                ttl, null, deliveryCount);

        Destination destination = message.getJMSDestination();
        Destination replyTo = message.getJMSReplyTo();
        if (destination != null) {
            annotations.put(DESTINATION_TYPE, kindOf(destination));
        }
        if (replyTo != null) {
            annotations.put(REPLY_TO_TYPE, kindOf(replyTo));
        }
        if (message.getJMSDeliveryTime() > timestamp) {
            annotations.put(DELIVERY_TIME, message.getJMSDeliveryTime());
        }

        Map<Object, Object> application = new LinkedHashMap<>();
        String groupId = null;
        Long groupSequence = null;
        for (String name : Collections.list(message.getPropertyNames())) {
            Object value = message.getObjectProperty(name);
            switch (name) {
                case DELIVERY_COUNT -> {
                }
                case GROUP_ID -> groupId = value == null ? null : value.toString();
                case GROUP_SEQUENCE -> groupSequence =
                        value instanceof Number number ? Long.valueOf(number.longValue() & UInt.MAX) : null;
                default -> application.put(name, toAmqpValue(value));
            }
        }

        String messageId = message.getJMSMessageID();
        String correlationId = message.getJMSCorrelationID();
        Object correlation = correlationId != null && correlationId.startsWith(ID_PREFIX)
                ? toIdObject(correlationId) : correlationId;
        Properties properties = new Properties(messageId == null ? null : toIdObject(messageId), null,
                addressOf(destination), message.getJMSType(), addressOf(replyTo), correlation, contentType, null,
                expiration == 0 ? null : new Timestamp(expiration), timestamp == 0 ? null : new Timestamp(timestamp),
                groupId, groupSequence, null);

        return new AmqpMessage(header, null, new MessageAnnotations(annotations),
                properties.describedValue().isEmpty() ? null : properties,
                application.isEmpty() ? null : new ApplicationProperties(application), body, null);
    }

    private static byte kindOf(Destination destination) throws JMSException {
        // A temporary destination that crossed a broker comes back as a plain one of its name:
        // the names a Tassis broker gives them say what they are.
        String name = addressOf(destination);
        boolean temporary = destination instanceof jakarta.jms.TemporaryQueue
                || destination instanceof jakarta.jms.TemporaryTopic
                || name != null && (name.startsWith("temporary-queue:") || name.startsWith("temporary-topic:"));
        if (destination instanceof Topic) {
            return temporary ? KIND_TEMPORARY_TOPIC : KIND_TOPIC;
        }
        return temporary ? KIND_TEMPORARY_QUEUE : KIND_QUEUE;
    }

    private static String addressOf(Destination destination) throws JMSException {
        return switch (destination) {
            case null -> null;
            case Queue queue -> queue.getQueueName();
            case Topic topic -> topic.getTopicName();
            default -> destination.toString();
        };
    }

    /** A value of a map message, of a stream message or of a property, as AMQP carries it. */
    private static Object toAmqpValue(Object value) {
        return switch (value) {
            case Character character -> new AmqpChar(character);
            case byte[] bytes -> new Binary(bytes);
            case null, default -> value;
        };
    }

    // ---- to Jakarta Messaging --------------------------------------------------

    /**
     * Returns the Jakarta Messaging form of a message, a JMS client's or not: one that has no
     * {@code x-opt-jms-msg-type} annotation is classed by its body.
     *
     * <p>The message returned can still be written to, except the body of a bytes or a stream
     * message, which is ready to be read.
     *
     * @throws AmqpDecodeException if no Jakarta Messaging message can hold what this one carries
     */
    public static TassisMessage toJms(AmqpMessage amqp) throws AmqpDecodeException {
        try {
            return read(amqp);
        } catch (JMSException e) {
            throw new AmqpDecodeException("a message Jakarta Messaging cannot hold: " + e.getMessage());
        }
    }

    private static TassisMessage read(AmqpMessage amqp) throws JMSException, AmqpDecodeException {
        Map<Object, Object> annotations =
                amqp.messageAnnotations() == null ? Map.of() : amqp.messageAnnotations().value();
        Properties properties = amqp.properties();
        Symbol contentType = properties == null ? null : properties.contentType();
        TassisMessage message = body(typeOf(annotations.get(MSG_TYPE), amqp.body(), contentType), amqp.body());

        Header header = amqp.header();
        // The defaults of AMQP: not durable, priority 4 (part 3, §3.2.1).
        boolean durable = header != null && Boolean.TRUE.equals(header.durable());
        message.setJMSDeliveryMode(durable ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT);
        int priority = header == null || header.priority() == null ? DEFAULT_PRIORITY : header.priority();
        message.setJMSPriority(Math.min(priority, 9));
        if (header != null && header.deliveryCount() != null) {
            long redeliveries = header.deliveryCount();
            message.setJMSRedelivered(redeliveries > 0);
            message.setIntProperty(DELIVERY_COUNT, (int) Math.min(redeliveries + 1, Integer.MAX_VALUE));
        }

        long timestamp = 0;
        if (properties != null) {
            if (properties.messageId() != null) {
                message.setJMSMessageID(toIdString(properties.messageId()));
            }
            if (properties.correlationId() != null) {
                message.setJMSCorrelationID(toCorrelationString(properties.correlationId()));
            }
            if (properties.creationTime() != null) {
                timestamp = properties.creationTime().millis();
                message.setJMSTimestamp(timestamp);
            }
            if (properties.absoluteExpiryTime() != null) {
                message.setJMSExpiration(properties.absoluteExpiryTime().millis());
            }
            message.setJMSType(properties.subject());
            message.setJMSDestination(destination(properties.to(), annotations.get(DESTINATION_TYPE)));
            message.setJMSReplyTo(destination(properties.replyTo(), annotations.get(REPLY_TO_TYPE)));
            if (properties.groupId() != null) {
                message.setStringProperty(GROUP_ID, properties.groupId());
            }
            if (properties.groupSequence() != null) {
                message.setIntProperty(GROUP_SEQUENCE, (int) (long) properties.groupSequence());
            }
        }
        if (message.getJMSExpiration() == 0 && header != null && header.ttl() != null && header.ttl() > 0) {
            // A time to live alone: counted from when the message was created, or else from now.
            message.setJMSExpiration((timestamp != 0 ? timestamp : System.currentTimeMillis()) + header.ttl());
        }
        if (annotations.get(DELIVERY_TIME) instanceof Number deliveryTime) {
            message.setJMSDeliveryTime(deliveryTime.longValue());
        } else if (annotations.get(DELIVERY_TIME) instanceof Timestamp deliveryTime) {
            message.setJMSDeliveryTime(deliveryTime.millis());
        }

        if (amqp.applicationProperties() != null) {
            for (Map.Entry<Object, Object> property : amqp.applicationProperties().value().entrySet()) {
                Object value = toJmsValue(property.getValue());
                if (!(property.getKey() instanceof String name) || value == UNSUPPORTED) {
                    continue;
                }
                try {
                    message.setObjectProperty(name, value);
                } catch (JMSException | IllegalArgumentException notAProperty) {
                    // A name no selector could use, or a value no property can hold: the rest
                    // of the message is still worth delivering.
                }
            }
        }
        return message;
    }

    /** Which JMS message type a message is: what its annotation says, or else what its body looks like. */
    private static byte typeOf(Object annotation, List<Section> body, Symbol contentType) throws AmqpDecodeException {
        switch (annotation) {
            case null -> {
            }
            case Byte type -> {
                return type;
            }
            case UByte type -> {
                return (byte) type.value();
            }
            case Number type -> {
                return type.byteValue();
            }
            default -> throw new AmqpDecodeException("the annotation " + MSG_TYPE + " is not a number");
        }
        if (body.isEmpty()) {
            return TYPE_MESSAGE;
        }
        return switch (body.get(0)) {
            case Data data -> {
                String type = contentType == null ? "" : contentType.value();
                yield SERIALIZED_OBJECT.equals(contentType) ? TYPE_OBJECT
                        : type.startsWith("text/") ? TYPE_TEXT : TYPE_BYTES;
            }
            case AmqpSequence sequence -> TYPE_STREAM;
            case AmqpValue value -> switch (value.value()) {
                case null -> TYPE_TEXT;
                case String text -> TYPE_TEXT;
                case Binary binary -> TYPE_BYTES;
                case Map<?, ?> map -> TYPE_MAP;
                case List<?> list -> TYPE_STREAM;
                default -> throw new AmqpDecodeException(
                        "a body of type " + value.value().getClass().getSimpleName() + " is no JMS message");
            };
            default -> throw new AmqpDecodeException("not a body section");
        };
    }

    private static TassisMessage body(byte type, List<Section> body) throws JMSException, AmqpDecodeException {
        Object value = body.size() == 1 && body.get(0) instanceof AmqpValue amqpValue ? amqpValue.value() : null;
        switch (type) {
            case TYPE_MESSAGE:
                return new TassisMessage();
            case TYPE_TEXT:
                TassisTextMessage text = new TassisTextMessage();
                if (value instanceof String string) {
                    text.setText(string);
                } else if (!body.isEmpty() && body.get(0) instanceof Data) {
                    text.setText(utf8(octets(body)));
                } else if (value != null) {
                    throw new AmqpDecodeException("a text message whose body is no text");
                }
                return text;
            case TYPE_BYTES:
                // The octets were read for this message alone: it holds them as they are.
                return TassisBytesMessage.holding(value instanceof Binary binary ? binary.array() : octets(body));
            case TYPE_OBJECT:
                TassisObjectMessage object = new TassisObjectMessage();
                byte[] serialized = value instanceof Binary binary ? binary.array() : octets(body);
                // Never deserialized here: what the octets are is the business of whoever reads them.
                object.setSerialized(serialized.length == 0 ? null : serialized);
                return object;
            case TYPE_MAP:
                TassisMapMessage map = new TassisMapMessage();
                if (value instanceof Map<?, ?> entries) {
                    for (Map.Entry<?, ?> entry : entries.entrySet()) {
                        Object item = toJmsValue(entry.getValue());
                        if (item == UNSUPPORTED) {
                            throw new AmqpDecodeException("a map message cannot hold the value of " + entry.getKey());
                        }
                        map.setObject(String.valueOf(entry.getKey()), item);
                    }
                } else if (value != null || !body.isEmpty()) {
                    throw new AmqpDecodeException("a map message whose body is no map");
                }
                return map;
            case TYPE_STREAM:
                TassisStreamMessage stream = new TassisStreamMessage();
                List<Object> fields = new ArrayList<>();
                if (value instanceof List<?> list) {
                    fields.addAll(list);
                } else {
                    for (Section section : body) {
                        if (!(section instanceof AmqpSequence sequence)) {
                            throw new AmqpDecodeException("a stream message whose body is no sequence");
                        }
                        fields.addAll(sequence.value());
                    }
                }
                for (Object field : fields) {
                    Object item = toJmsValue(field);
                    if (item == UNSUPPORTED) {
                        throw new AmqpDecodeException("a stream message cannot hold " + field);
                    }
                    stream.writeObject(item);
                }
                stream.reset();
                return stream;
            default:
                throw new AmqpDecodeException("unknown JMS message type " + type);
        }
    }

    /** The octets of a body made of data sections, which are one body whatever their number. */
    private static byte[] octets(List<Section> body) throws AmqpDecodeException {
        int length = 0;
        for (Section section : body) {
            if (!(section instanceof Data data)) {
                throw new AmqpDecodeException("a body of octets is made of data sections");
            }
            length += data.value().length();
        }
        if (body.size() == 1) {
            // The usual case: the octets of the one section, read for this message alone.
            return ((Data) body.get(0)).value().array();
        }
        byte[] octets = new byte[length];
        int at = 0;
        for (Section section : body) {
            byte[] part = ((Data) section).value().array();
            System.arraycopy(part, 0, octets, at, part.length);
            at += part.length;
        }
        return octets;
    }

    private static String utf8(byte[] octets) throws AmqpDecodeException {
        try {
            return StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(octets)).toString();
        } catch (CharacterCodingException e) {
            throw new AmqpDecodeException("a text body that is not UTF-8");
        }
    }

    private static Destination destination(Object address, Object kind) {
        if (!(address instanceof String name) || name.isEmpty()) {
            return null;
        }
        boolean topic = kind instanceof Number number
                && (number.byteValue() == KIND_TOPIC || number.byteValue() == KIND_TEMPORARY_TOPIC);
        return topic ? new TassisTopic(name) : new TassisQueue(name);
    }

    /** A value AMQP carries, as the nearest type Jakarta Messaging has, or {@link #UNSUPPORTED}. */
    private static Object toJmsValue(Object value) {
        return switch (value) {
            case null -> null;
            case Boolean b -> b;
            case Byte b -> b;
            case Short s -> s;
            case Integer i -> i;
            case Long l -> l;
            case Float f -> f;
            case Double d -> d;
            case String s -> s;
            case AmqpChar c -> Character.isBmpCodePoint(c.codePoint()) ? (Object) (char) c.codePoint()
                    : Character.toString(c.codePoint());
            case Binary b -> b.bytes();
            case UByte u -> (short) u.value();
            case UShort u -> u.value();
            case UInt u -> u.value();
            case ULong u -> u.bits() >= 0 ? (Object) u.bits() : u.toString();
            case Symbol s -> s.value();
            case Timestamp t -> t.millis();
            case UUID u -> u.toString();
            default -> UNSUPPORTED;
        };
    }

    // ---- identifiers -----------------------------------------------------------
    //
    // A JMSMessageID is a string that starts with "ID:". An AMQP message-id is a string, a
    // ulong, a uuid or a binary. So that an identifier comes back as the value it arrived as,
    // its type is kept in the text, after the "ID:" prefix.

    private static final String ID_PREFIX = "ID:";
    private static final String UUID_PREFIX = "AMQP_UUID:";
    private static final String ULONG_PREFIX = "AMQP_ULONG:";
    private static final String BINARY_PREFIX = "AMQP_BINARY:";
    private static final String STRING_PREFIX = "AMQP_STRING:";
    private static final String NO_PREFIX = "AMQP_NO_PREFIX:";

    private static boolean saysItsType(String afterIdPrefix) {
        return afterIdPrefix.startsWith(UUID_PREFIX) || afterIdPrefix.startsWith(ULONG_PREFIX)
                || afterIdPrefix.startsWith(BINARY_PREFIX) || afterIdPrefix.startsWith(STRING_PREFIX)
                || afterIdPrefix.startsWith(NO_PREFIX);
    }

    static String toIdString(Object id) {
        return switch (id) {
            case String text when !text.startsWith(ID_PREFIX) -> ID_PREFIX + NO_PREFIX + text;
            case String text when saysItsType(text.substring(ID_PREFIX.length())) -> ID_PREFIX + STRING_PREFIX + text;
            case String text -> text;
            case UUID uuid -> ID_PREFIX + UUID_PREFIX + uuid;
            case ULong number -> ID_PREFIX + ULONG_PREFIX + number;
            case Binary binary -> ID_PREFIX + BINARY_PREFIX + HexFormat.of().withUpperCase().formatHex(binary.bytes());
            default -> ID_PREFIX + id;
        };
    }

    /** A correlation identifier is the application's: a string that says nothing of a type is left alone. */
    private static String toCorrelationString(Object id) {
        if (id instanceof String text
                && !(text.startsWith(ID_PREFIX) && saysItsType(text.substring(ID_PREFIX.length())))) {
            return text;
        }
        return toIdString(id);
    }

    static Object toIdObject(String id) {
        if (!id.startsWith(ID_PREFIX)) {
            return id;
        }
        String rest = id.substring(ID_PREFIX.length());
        try {
            if (rest.startsWith(UUID_PREFIX)) {
                return UUID.fromString(rest.substring(UUID_PREFIX.length()));
            }
            if (rest.startsWith(ULONG_PREFIX)) {
                return ULong.of(Long.parseUnsignedLong(rest.substring(ULONG_PREFIX.length())));
            }
            if (rest.startsWith(BINARY_PREFIX)) {
                return new Binary(HexFormat.of().parseHex(rest.substring(BINARY_PREFIX.length())));
            }
        } catch (IllegalArgumentException notWhatItSays) {
            // An application may well write such a text itself: it then travels as the string it is.
            return id;
        }
        if (rest.startsWith(STRING_PREFIX)) {
            return rest.substring(STRING_PREFIX.length());
        }
        if (rest.startsWith(NO_PREFIX)) {
            return rest.substring(NO_PREFIX.length());
        }
        return id;
    }
}
