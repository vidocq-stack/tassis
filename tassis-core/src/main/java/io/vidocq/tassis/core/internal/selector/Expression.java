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
package io.vidocq.tassis.core.internal.selector;

import io.vidocq.tassis.core.internal.message.TassisMessage;
import jakarta.jms.DeliveryMode;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * A node of a parsed selector. Evaluating it against a message yields a
 * {@code Long} (exact numeric), a {@code Double} (approximate numeric), a
 * {@code String}, a {@code Boolean}, or {@code null} for SQL's NULL, which as a
 * condition is "unknown" (Jakarta Messaging 3.1 §3.8.1.2).
 */
sealed interface Expression {

    /** What an expression is known to yield before any message is seen. */
    enum Kind {
        BOOLEAN,
        NUMBER,
        STRING,
        /** An identifier: its type depends on the message. */
        ANY
    }

    Object evaluate(TassisMessage message);

    Kind kind();

    record Literal(Object value, Kind kind) implements Expression {

        @Override
        public Object evaluate(TassisMessage message) {
            return value;
        }
    }

    /** A header field or property reference; a property that is not set is NULL. */
    record Identifier(String name) implements Expression {

        @Override
        public Object evaluate(TassisMessage message) {
            Object value = switch (name) {
                // §3.8.1.3: the delivery mode reads as one of these two strings.
                case "JMSDeliveryMode" ->
                    message.getJMSDeliveryMode() == DeliveryMode.PERSISTENT ? "PERSISTENT" : "NON_PERSISTENT";
                case "JMSPriority" -> message.getJMSPriority();
                case "JMSTimestamp" -> message.getJMSTimestamp();
                case "JMSMessageID" -> message.getJMSMessageID();
                case "JMSCorrelationID" -> message.getJMSCorrelationID();
                case "JMSType" -> message.getJMSType();
                default -> message.getObjectProperty(name);
            };
            // Java numeric promotion: every integral type is exact, every floating type approximate.
            return switch (value) {
                case Byte b -> b.longValue();
                case Short s -> s.longValue();
                case Integer i -> i.longValue();
                case Float f -> f.doubleValue();
                case null, default -> value;
            };
        }

        @Override
        public Kind kind() {
            return Kind.ANY;
        }
    }

    record Not(Expression operand) implements Expression {

        @Override
        public Object evaluate(TassisMessage message) {
            Boolean value = condition(operand.evaluate(message));
            return value == null ? null : !value;
        }

        @Override
        public Kind kind() {
            return Kind.BOOLEAN;
        }
    }

    record And(Expression left, Expression right) implements Expression {

        @Override
        public Object evaluate(TassisMessage message) {
            Boolean l = condition(left.evaluate(message));
            Boolean r = condition(right.evaluate(message));
            if (Boolean.FALSE.equals(l) || Boolean.FALSE.equals(r)) {
                return false;
            }
            return l == null || r == null ? null : Boolean.TRUE;
        }

        @Override
        public Kind kind() {
            return Kind.BOOLEAN;
        }
    }

    record Or(Expression left, Expression right) implements Expression {

        @Override
        public Object evaluate(TassisMessage message) {
            Boolean l = condition(left.evaluate(message));
            Boolean r = condition(right.evaluate(message));
            if (Boolean.TRUE.equals(l) || Boolean.TRUE.equals(r)) {
                return true;
            }
            return l == null || r == null ? null : Boolean.FALSE;
        }

        @Override
        public Kind kind() {
            return Kind.BOOLEAN;
        }
    }

    enum Comparator {
        EQUAL,
        NOT_EQUAL,
        LESS,
        LESS_OR_EQUAL,
        GREATER,
        GREATER_OR_EQUAL
    }

    /**
     * Only values of like types compare, exact and approximate numbers being alike; any other
     * pairing is false, and a NULL on either side makes the result unknown (§3.8.1.1).
     */
    record Comparison(Comparator comparator, Expression left, Expression right) implements Expression {

        @Override
        public Object evaluate(TassisMessage message) {
            Object l = left.evaluate(message);
            Object r = right.evaluate(message);
            if (l == null || r == null) {
                return null;
            }
            if (l instanceof Number a && r instanceof Number b) {
                int order = a instanceof Long && b instanceof Long
                        ? Long.compare(a.longValue(), b.longValue())
                        : Double.compare(a.doubleValue(), b.doubleValue());
                return switch (comparator) {
                    case EQUAL -> order == 0;
                    case NOT_EQUAL -> order != 0;
                    case LESS -> order < 0;
                    case LESS_OR_EQUAL -> order <= 0;
                    case GREATER -> order > 0;
                    case GREATER_OR_EQUAL -> order >= 0;
                };
            }
            // Strings and booleans only know equality.
            boolean alike = l instanceof String && r instanceof String || l instanceof Boolean && r instanceof Boolean;
            return switch (comparator) {
                case EQUAL -> alike && l.equals(r);
                case NOT_EQUAL -> alike && !l.equals(r);
                default -> false;
            };
        }

        @Override
        public Kind kind() {
            return Kind.BOOLEAN;
        }
    }

    enum Operator {
        ADD,
        SUBTRACT,
        MULTIPLY,
        DIVIDE
    }

    /** Arithmetic with Java numeric promotion; anything but two numbers yields NULL. */
    record Arithmetic(Operator operator, Expression left, Expression right) implements Expression {

        @Override
        public Object evaluate(TassisMessage message) {
            if (!(left.evaluate(message) instanceof Number l) || !(right.evaluate(message) instanceof Number r)) {
                return null;
            }
            if (l instanceof Long a && r instanceof Long b) {
                return switch (operator) {
                    case ADD -> a + b;
                    case SUBTRACT -> a - b;
                    case MULTIPLY -> a * b;
                    // An exact division by zero has no value.
                    case DIVIDE -> b == 0 ? null : (Object) (a / b);
                };
            }
            double a = l.doubleValue();
            double b = r.doubleValue();
            return switch (operator) {
                case ADD -> a + b;
                case SUBTRACT -> a - b;
                case MULTIPLY -> a * b;
                case DIVIDE -> a / b;
            };
        }

        @Override
        public Kind kind() {
            return Kind.NUMBER;
        }
    }

    record Negate(Expression operand) implements Expression {

        @Override
        public Object evaluate(TassisMessage message) {
            return switch (operand.evaluate(message)) {
                case Long l -> -l;
                case Double d -> -d;
                case null, default -> null;
            };
        }

        @Override
        public Kind kind() {
            return Kind.NUMBER;
        }
    }

    /** {@code identifier [NOT] IN (...)}: unknown unless the identifier has a string value. */
    record In(Identifier identifier, Set<String> values, boolean negated) implements Expression {

        @Override
        public Object evaluate(TassisMessage message) {
            return identifier.evaluate(message) instanceof String value ? values.contains(value) != negated : null;
        }

        @Override
        public Kind kind() {
            return Kind.BOOLEAN;
        }
    }

    /** {@code identifier [NOT] LIKE pattern}: unknown unless the identifier has a string value. */
    record Like(Identifier identifier, Pattern pattern, boolean negated) implements Expression {

        @Override
        public Object evaluate(TassisMessage message) {
            return identifier.evaluate(message) instanceof String value
                    ? pattern.matcher(value).matches() != negated
                    : null;
        }

        @Override
        public Kind kind() {
            return Kind.BOOLEAN;
        }
    }

    /** {@code identifier IS [NOT] NULL}: the one operator that turns unknown into a truth value. */
    record IsNull(Identifier identifier, boolean negated) implements Expression {

        @Override
        public Object evaluate(TassisMessage message) {
            return (identifier.evaluate(message) == null) != negated;
        }

        @Override
        public Kind kind() {
            return Kind.BOOLEAN;
        }
    }

    /** Reads a value as a condition: anything that is not a boolean is unknown. */
    private static Boolean condition(Object value) {
        return value instanceof Boolean condition ? condition : null;
    }
}
