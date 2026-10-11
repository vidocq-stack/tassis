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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.vidocq.tassis.amqp.internal.messaging.AmqpMessage;
import io.vidocq.tassis.amqp.internal.messaging.AmqpSequence;
import io.vidocq.tassis.amqp.internal.messaging.AmqpValue;
import io.vidocq.tassis.amqp.internal.messaging.ApplicationProperties;
import io.vidocq.tassis.amqp.internal.messaging.Data;
import io.vidocq.tassis.amqp.internal.messaging.Header;
import io.vidocq.tassis.amqp.internal.messaging.MessageAnnotations;
import io.vidocq.tassis.amqp.internal.messaging.Properties;
import io.vidocq.tassis.amqp.internal.types.AmqpChar;
import io.vidocq.tassis.amqp.internal.types.AmqpDecodeException;
import io.vidocq.tassis.amqp.internal.types.Binary;
import io.vidocq.tassis.amqp.internal.types.Symbol;
import io.vidocq.tassis.amqp.internal.types.Timestamp;
import io.vidocq.tassis.amqp.internal.types.UByte;
import io.vidocq.tassis.amqp.internal.types.UInt;
import io.vidocq.tassis.amqp.internal.types.ULong;
import io.vidocq.tassis.core.TassisQueue;
import io.vidocq.tassis.core.TassisTopic;
import io.vidocq.tassis.core.internal.message.TassisBytesMessage;
import io.vidocq.tassis.core.internal.message.TassisMapMessage;
import io.vidocq.tassis.core.internal.message.TassisMessage;
import io.vidocq.tassis.core.internal.message.TassisObjectMessage;
import io.vidocq.tassis.core.internal.message.TassisStreamMessage;
import io.vidocq.tassis.core.internal.message.TassisTextMessage;
import jakarta.jms.DeliveryMode;
import jakarta.jms.JMSException;
import jakarta.jms.MessageEOFException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * How a Jakarta Messaging message travels as an AMQP 1.0 message. AMQP defines no such mapping;
 * the conventions are those of Apache Qpid JMS, which the OASIS "AMQP JMS Mapping" working
 * drafts describe (Jakarta Messaging 3.1 §3.4 for the header fields, §3.5 for the properties,
 * §3.11 for the bodies).
 */
class JmsMessageCodecTest {

    private static final Symbol MSG_TYPE = Symbol.of("x-opt-jms-msg-type");

    /** Through the wire and back: what a peer would read of what Tassis wrote. */
    private static TassisMessage roundTrip(TassisMessage message) throws JMSException, AmqpDecodeException {
        return JmsMessageCodec.toJms(AmqpMessage.decode(JmsMessageCodec.toAmqp(message).encode()));
    }

    private static Map<Object, Object> annotations(Object... keysAndValues) {
        Map<Object, Object> map = new LinkedHashMap<>();
        for (int i = 0; i < keysAndValues.length; i += 2) {
            map.put(keysAndValues[i], keysAndValues[i + 1]);
        }
        return map;
    }

    private static AmqpMessage foreign(Properties properties, ApplicationProperties application,
            io.vidocq.tassis.amqp.internal.messaging.Section... body) {
        return new AmqpMessage(null, null, null, properties, application, List.of(body), null);
    }

    // ---- §3.11: the five bodies, and none ------------------------------------

    @Test
    void aTextMessageIsAnAmqpValueHoldingAString() throws Exception {
        TassisTextMessage message = new TassisTextMessage();
        message.setText("hello");

        AmqpMessage amqp = JmsMessageCodec.toAmqp(message);

        assertEquals(List.of(new AmqpValue("hello")), amqp.body());
        assertEquals((byte) 5, amqp.messageAnnotations().value().get(MSG_TYPE));
        assertEquals("hello", ((TassisTextMessage) roundTrip(message)).getText());

        assertNull(((TassisTextMessage) roundTrip(new TassisTextMessage())).getText());
    }

    @Test
    void aBytesMessageIsADataSection() throws Exception {
        TassisBytesMessage message = new TassisBytesMessage();
        message.writeBytes(new byte[] {1, 2, 3});
        message.writeInt(7);

        AmqpMessage amqp = JmsMessageCodec.toAmqp(message);

        assertEquals(List.of(new Data(Binary.of(1, 2, 3, 0, 0, 0, 7))), amqp.body());
        assertEquals((byte) 3, amqp.messageAnnotations().value().get(MSG_TYPE));
        assertEquals(Symbol.of("application/octet-stream"), amqp.properties().contentType());
        TassisBytesMessage read = (TassisBytesMessage) roundTrip(message);
        assertEquals(7, read.getBodyLength());
        byte[] three = new byte[3];
        read.readBytes(three);
        assertArrayEquals(new byte[] {1, 2, 3}, three);
        assertEquals(7, read.readInt());
    }

    @Test
    void aMapMessageIsAnAmqpValueHoldingAMap() throws Exception {
        TassisMapMessage message = new TassisMapMessage();
        message.setString("s", "text");
        message.setInt("i", 7);
        message.setBoolean("b", true);
        message.setChar("c", 'x');
        message.setBytes("bytes", new byte[] {9});
        message.setDouble("d", 1.5);
        message.setString("none", null);

        AmqpMessage amqp = JmsMessageCodec.toAmqp(message);

        Map<?, ?> body = (Map<?, ?>) ((AmqpValue) amqp.body().get(0)).value();
        assertEquals(new AmqpChar('x'), body.get("c"));
        assertEquals(Binary.of(9), body.get("bytes"));
        assertEquals((byte) 2, amqp.messageAnnotations().value().get(MSG_TYPE));
        TassisMapMessage read = (TassisMapMessage) roundTrip(message);
        assertEquals(List.of("s", "i", "b", "c", "bytes", "d", "none"), Collections.list(read.getMapNames()));
        assertEquals("text", read.getString("s"));
        assertEquals(7, read.getObject("i"));
        assertEquals(true, read.getObject("b"));
        assertEquals('x', read.getObject("c"));
        assertArrayEquals(new byte[] {9}, read.getBytes("bytes"));
        assertEquals(1.5, read.getObject("d"));
        assertNull(read.getObject("none"));
    }

    @Test
    void aStreamMessageIsAnAmqpSequence() throws Exception {
        TassisStreamMessage message = new TassisStreamMessage();
        message.writeString("a");
        message.writeLong(5L);
        message.writeBytes(new byte[] {1, 2});
        message.writeChar('z');
        message.writeObject(null);

        AmqpMessage amqp = JmsMessageCodec.toAmqp(message);

        List<Object> expected = new ArrayList<>(List.of("a", 5L, Binary.of(1, 2), new AmqpChar('z')));
        expected.add(null);
        assertEquals(List.of(new AmqpSequence(expected)), amqp.body());
        assertEquals((byte) 4, amqp.messageAnnotations().value().get(MSG_TYPE));
        TassisStreamMessage read = (TassisStreamMessage) roundTrip(message);
        assertEquals("a", read.readObject());
        assertEquals(5L, read.readObject());
        assertArrayEquals(new byte[] {1, 2}, (byte[]) read.readObject());
        assertEquals('z', read.readObject());
        assertNull(read.readObject());
        assertThrows(MessageEOFException.class, read::readObject);
    }

    @Test
    void anObjectMessageCarriesTheSerializedObjectAndIsNeverDeserializedOnTheWay() throws Exception {
        TassisObjectMessage message = new TassisObjectMessage();
        message.setObject(new ArrayList<>(List.of("a", 1)));
        byte[] serialized = message.serialized();

        AmqpMessage amqp = JmsMessageCodec.toAmqp(message);

        assertEquals(List.of(new Data(new Binary(serialized))), amqp.body());
        assertEquals(Symbol.of("application/x-java-serialized-object"), amqp.properties().contentType());
        assertEquals((byte) 1, amqp.messageAnnotations().value().get(MSG_TYPE));
        assertEquals(List.of("a", 1), ((TassisObjectMessage) roundTrip(message)).getObject());

        // Octets that are no serialized object travel as they are: nobody looks into them.
        TassisObjectMessage opaque = new TassisObjectMessage();
        opaque.setSerialized(new byte[] {1, 2, 3});
        assertArrayEquals(new byte[] {1, 2, 3}, ((TassisObjectMessage) roundTrip(opaque)).serialized());
        assertNull(((TassisObjectMessage) roundTrip(new TassisObjectMessage())).getObject());
    }

    @Test
    void aMessageWithoutABodyHasNoBodySection() throws Exception {
        AmqpMessage amqp = JmsMessageCodec.toAmqp(new TassisMessage());

        assertEquals(List.of(), amqp.body());
        assertEquals((byte) 0, amqp.messageAnnotations().value().get(MSG_TYPE));
        assertEquals(TassisMessage.class, roundTrip(new TassisMessage()).getClass());
    }

    // ---- §3.4: header fields --------------------------------------------------

    @Test
    void theHeaderFieldsTravelInTheHeaderAndThePropertiesSections() throws Exception {
        TassisTextMessage message = new TassisTextMessage();
        message.setJMSMessageID("ID:broker-42");
        message.setJMSTimestamp(1_000_000L);
        message.setJMSExpiration(1_060_000L);
        message.setJMSDeliveryMode(DeliveryMode.NON_PERSISTENT);
        message.setJMSPriority(7);
        message.setJMSType("order");
        message.setJMSCorrelationID("request-9");
        message.setJMSDestination(new TassisQueue("orders"));
        message.setJMSReplyTo(new TassisQueue("replies"));

        AmqpMessage amqp = JmsMessageCodec.toAmqp(message);

        assertEquals(new Header(false, 7, 60_000L, null, null), amqp.header());
        Properties properties = amqp.properties();
        assertEquals("ID:broker-42", properties.messageId());
        assertEquals(new Timestamp(1_000_000L), properties.creationTime());
        assertEquals(new Timestamp(1_060_000L), properties.absoluteExpiryTime());
        assertEquals("order", properties.subject());
        assertEquals("request-9", properties.correlationId());
        assertEquals("orders", properties.to());
        assertEquals("replies", properties.replyTo());
        assertEquals((byte) 0, amqp.messageAnnotations().value().get(Symbol.of("x-opt-jms-dest")));
        assertEquals((byte) 0, amqp.messageAnnotations().value().get(Symbol.of("x-opt-jms-reply-to")));

        TassisMessage read = roundTrip(message);
        assertEquals("ID:broker-42", read.getJMSMessageID());
        assertEquals(1_000_000L, read.getJMSTimestamp());
        assertEquals(1_060_000L, read.getJMSExpiration());
        assertEquals(DeliveryMode.NON_PERSISTENT, read.getJMSDeliveryMode());
        assertEquals(7, read.getJMSPriority());
        assertEquals("order", read.getJMSType());
        assertEquals("request-9", read.getJMSCorrelationID());
        assertEquals(new TassisQueue("orders"), read.getJMSDestination());
        assertEquals(new TassisQueue("replies"), read.getJMSReplyTo());
    }

    @Test
    void aTopicIsToldFromAQueueByAnAnnotation() throws Exception {
        TassisMessage message = new TassisMessage();
        message.setJMSDestination(new TassisTopic("news"));
        message.setJMSReplyTo(new TassisTopic("answers"));

        AmqpMessage amqp = JmsMessageCodec.toAmqp(message);

        assertEquals((byte) 1, amqp.messageAnnotations().value().get(Symbol.of("x-opt-jms-dest")));
        assertEquals((byte) 1, amqp.messageAnnotations().value().get(Symbol.of("x-opt-jms-reply-to")));
        TassisMessage read = roundTrip(message);
        assertEquals(new TassisTopic("news"), read.getJMSDestination());
        assertEquals(new TassisTopic("answers"), read.getJMSReplyTo());
    }

    @Test
    void aMessageThatNeverExpiresAndHasNoTimestampSaysNothingOfThem() throws Exception {
        AmqpMessage amqp = JmsMessageCodec.toAmqp(new TassisMessage());

        // persistent, priority 4: the defaults of JMS are written, since those of AMQP differ
        assertEquals(new Header(true, 4, null, null, null), amqp.header());
        assertNull(amqp.properties());
        TassisMessage read = roundTrip(new TassisMessage());
        assertEquals(0, read.getJMSExpiration());
        assertEquals(0, read.getJMSTimestamp());
        assertEquals(DeliveryMode.PERSISTENT, read.getJMSDeliveryMode());
        assertEquals(4, read.getJMSPriority());
        assertNull(read.getJMSMessageID());
    }

    @Test
    void theDeliveryCountOfAmqpIsTheNumberOfRedeliveries() throws Exception {
        TassisTextMessage second = new TassisTextMessage();
        second.setText("again");
        second.markDelivered(2);

        AmqpMessage amqp = JmsMessageCodec.toAmqp(second);

        assertEquals(1L, amqp.header().deliveryCount());
        assertNull(amqp.applicationProperties(), "JMSXDeliveryCount is not sent as a property");
        TassisMessage read = roundTrip(second);
        assertTrue(read.getJMSRedelivered());
        assertEquals(2, read.getIntProperty("JMSXDeliveryCount"));

        TassisTextMessage first = new TassisTextMessage();
        first.markDelivered(1);
        assertEquals(0L, JmsMessageCodec.toAmqp(first).header().deliveryCount());
        assertFalse(roundTrip(first).getJMSRedelivered());
        assertEquals(1, roundTrip(first).getIntProperty("JMSXDeliveryCount"));
    }

    // ---- message and correlation identifiers ----------------------------------

    @Test
    void identifiersThatAreNotStringsKeepTheirTypeInTheText() throws Exception {
        UUID uuid = UUID.fromString("f81d4fae-7dec-11d0-a765-00a0c91e6bf6");

        assertEquals("ID:AMQP_UUID:" + uuid, idOf(uuid));
        assertEquals("ID:AMQP_ULONG:77", idOf(ULong.of(77)));
        assertEquals("ID:AMQP_BINARY:0102FF", idOf(Binary.of(1, 2, 255)));
        assertEquals("ID:plain", idOf("ID:plain"));
        assertEquals("ID:AMQP_NO_PREFIX:plain", idOf("plain"));
        assertEquals("ID:AMQP_STRING:ID:AMQP_UUID:not-one", idOf("ID:AMQP_UUID:not-one"));

        // and each is written back as the value it came as
        for (Object id : List.of(uuid, ULong.of(77), Binary.of(1, 2, 255), "ID:plain", "plain", "ID:AMQP_UUID:not-one")) {
            TassisMessage read = JmsMessageCodec.toJms(foreign(
                    new Properties(id, null, null, null, null, id, null, null, null, null, null, null, null), null));
            AmqpMessage back = JmsMessageCodec.toAmqp(read);
            assertEquals(id, back.properties().messageId(), "message-id " + id);
            assertEquals(id, back.properties().correlationId(), "correlation-id " + id);
        }
    }

    private static String idOf(Object id) throws AmqpDecodeException, JMSException {
        return JmsMessageCodec.toJms(foreign(
                new Properties(id, null, null, null, null, null, null, null, null, null, null, null, null), null))
                .getJMSMessageID();
    }

    @Test
    void aCorrelationIdentifierOfTheApplicationTravelsAsItIs() throws Exception {
        TassisMessage message = new TassisMessage();
        message.setJMSCorrelationID("order 42");

        assertEquals("order 42", JmsMessageCodec.toAmqp(message).properties().correlationId());
        assertEquals("order 42", roundTrip(message).getJMSCorrelationID());
    }

    // ---- §3.5: properties -----------------------------------------------------

    @Test
    void propertiesAreApplicationProperties() throws Exception {
        TassisMessage message = new TassisMessage();
        message.setStringProperty("color", "red");
        message.setIntProperty("weight", 12);
        message.setBooleanProperty("fragile", true);
        message.setByteProperty("b", (byte) 1);
        message.setShortProperty("s", (short) 2);
        message.setLongProperty("l", 3L);
        message.setFloatProperty("f", 1.5f);
        message.setDoubleProperty("d", 2.5);
        message.setStringProperty("none", null);

        AmqpMessage amqp = JmsMessageCodec.toAmqp(message);

        assertEquals(12, amqp.applicationProperties().value().get("weight"));
        TassisMessage read = roundTrip(message);
        assertEquals(List.of("color", "weight", "fragile", "b", "s", "l", "f", "d", "none"),
                Collections.list(read.getPropertyNames()));
        assertEquals("red", read.getObjectProperty("color"));
        assertEquals(12, read.getObjectProperty("weight"));
        assertEquals(true, read.getObjectProperty("fragile"));
        assertEquals((byte) 1, read.getObjectProperty("b"));
        assertEquals((short) 2, read.getObjectProperty("s"));
        assertEquals(3L, read.getObjectProperty("l"));
        assertEquals(1.5f, read.getObjectProperty("f"));
        assertEquals(2.5, read.getObjectProperty("d"));
        assertNull(read.getObjectProperty("none"));
        assertTrue(read.propertyExists("none"));
    }

    @Test
    void theGroupPropertiesTravelInThePropertiesSection() throws Exception {
        TassisMessage message = new TassisMessage();
        message.setStringProperty("JMSXGroupID", "batch-1");
        message.setIntProperty("JMSXGroupSeq", 3);

        AmqpMessage amqp = JmsMessageCodec.toAmqp(message);

        assertEquals("batch-1", amqp.properties().groupId());
        assertEquals(3L, amqp.properties().groupSequence());
        assertNull(amqp.applicationProperties());
        TassisMessage read = roundTrip(message);
        assertEquals("batch-1", read.getStringProperty("JMSXGroupID"));
        assertEquals(3, read.getIntProperty("JMSXGroupSeq"));
    }

    // ---- messages of a client that is not a JMS one ---------------------------

    @Test
    void aMessageWithoutTheAnnotationIsClassedByItsBody() throws Exception {
        assertEquals("text", ((TassisTextMessage) JmsMessageCodec.toJms(foreign(null, null, new AmqpValue("text")))).getText());
        assertInstanceOf(TassisTextMessage.class, JmsMessageCodec.toJms(foreign(null, null, new AmqpValue(null))));
        assertInstanceOf(TassisBytesMessage.class, JmsMessageCodec.toJms(foreign(null, null, new Data(Binary.of(1)))));
        assertInstanceOf(TassisBytesMessage.class, JmsMessageCodec.toJms(foreign(null, null, new AmqpValue(Binary.of(1)))));
        assertInstanceOf(TassisMapMessage.class,
                JmsMessageCodec.toJms(foreign(null, null, new AmqpValue(annotations("k", "v")))));
        assertInstanceOf(TassisStreamMessage.class, JmsMessageCodec.toJms(foreign(null, null, new AmqpValue(List.of(1)))));
        assertInstanceOf(TassisStreamMessage.class,
                JmsMessageCodec.toJms(foreign(null, null, new AmqpSequence(List.of(1)))));
        assertEquals(TassisMessage.class, JmsMessageCodec.toJms(foreign(null, null)).getClass());

        Properties serialized = new Properties(null, null, null, null, null, null,
                Symbol.of("application/x-java-serialized-object"), null, null, null, null, null, null);
        assertInstanceOf(TassisObjectMessage.class, JmsMessageCodec.toJms(foreign(serialized, null, new Data(Binary.of(1)))));
        Properties text = new Properties(null, null, null, null, null, null, Symbol.of("text/plain"), null, null, null,
                null, null, null);
        assertEquals("hé", ((TassisTextMessage) JmsMessageCodec.toJms(foreign(text, null,
                new Data(new Binary("hé".getBytes(java.nio.charset.StandardCharsets.UTF_8)))))).getText());
    }

    @Test
    void severalDataSectionsAreOneBody() throws Exception {
        TassisBytesMessage read = (TassisBytesMessage) JmsMessageCodec
                .toJms(foreign(null, null, new Data(Binary.of(1, 2)), new Data(Binary.of(3))));

        assertEquals(3, read.getBodyLength());
    }

    @Test
    void aTimeToLiveAloneGivesAnExpiration() throws Exception {
        AmqpMessage amqp = new AmqpMessage(new Header(null, null, 5_000L, null, null), null, null,
                new Properties(null, null, null, null, null, null, null, null, null, new Timestamp(10_000L), null, null,
                        null),
                null, List.of(), null);

        assertEquals(15_000L, JmsMessageCodec.toJms(amqp).getJMSExpiration());
    }

    @Test
    void theDefaultsOfAmqpApplyToAMessageThatSaysNothing() throws Exception {
        // AMQP: not durable, priority 4 (part 3, §3.2.1)
        TassisMessage read = JmsMessageCodec.toJms(foreign(null, null, new AmqpValue("x")));

        assertEquals(DeliveryMode.NON_PERSISTENT, read.getJMSDeliveryMode());
        assertEquals(4, read.getJMSPriority());
        assertFalse(read.getJMSRedelivered());
        assertFalse(read.propertyExists("JMSXDeliveryCount"));
    }

    @Test
    void valuesJmsHasNoTypeForAreBroughtToTheNearestOne() throws Exception {
        Map<Object, Object> application = annotations("u8", UByte.of(200), "u32", UInt.of(4_000_000_000L), "u64",
                ULong.of(9), "sym", Symbol.of("s"), "at", new Timestamp(5), "id", new UUID(0, 1), "not-an-identifier", 1,
                "nested", List.of(1));

        TassisMessage read = JmsMessageCodec.toJms(foreign(null, new ApplicationProperties(application), new AmqpValue("x")));

        assertEquals((short) 200, read.getObjectProperty("u8"));
        assertEquals(4_000_000_000L, read.getObjectProperty("u32"));
        assertEquals(9L, read.getObjectProperty("u64"));
        assertEquals("s", read.getObjectProperty("sym"));
        assertEquals(5L, read.getObjectProperty("at"));
        assertEquals(new UUID(0, 1).toString(), read.getObjectProperty("id"));
        // a name that no selector could use, a value with no JMS type: left out, not fatal
        assertFalse(read.propertyExists("not-an-identifier"));
        assertFalse(read.propertyExists("nested"));
    }

    @Test
    void aBodyThatFitsNoJmsMessageIsRefused() {
        assertThrows(AmqpDecodeException.class, () -> JmsMessageCodec.toJms(foreign(null, null, new AmqpValue(1.5))));
        MessageAnnotations unknownType = new MessageAnnotations(annotations(MSG_TYPE, (byte) 99));
        assertThrows(AmqpDecodeException.class, () -> JmsMessageCodec.toJms(
                new AmqpMessage(null, null, unknownType, null, null, List.of(new AmqpValue("x")), null)));
    }
}
