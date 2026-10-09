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

import io.vidocq.tassis.core.internal.selector.Expression.And;
import io.vidocq.tassis.core.internal.selector.Expression.Arithmetic;
import io.vidocq.tassis.core.internal.selector.Expression.Comparator;
import io.vidocq.tassis.core.internal.selector.Expression.Comparison;
import io.vidocq.tassis.core.internal.selector.Expression.Identifier;
import io.vidocq.tassis.core.internal.selector.Expression.In;
import io.vidocq.tassis.core.internal.selector.Expression.IsNull;
import io.vidocq.tassis.core.internal.selector.Expression.Kind;
import io.vidocq.tassis.core.internal.selector.Expression.Like;
import io.vidocq.tassis.core.internal.selector.Expression.Literal;
import io.vidocq.tassis.core.internal.selector.Expression.Negate;
import io.vidocq.tassis.core.internal.selector.Expression.Not;
import io.vidocq.tassis.core.internal.selector.Expression.Operator;
import io.vidocq.tassis.core.internal.selector.Expression.Or;
import jakarta.jms.InvalidSelectorException;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Hand-written lexer and recursive-descent parser for the selector syntax of
 * Jakarta Messaging 3.1 §3.8.1.1, a subset of SQL-92 conditional expressions.
 *
 * <pre>
 * condition  := conjunction (OR conjunction)*
 * conjunction:= negation (AND negation)*
 * negation   := NOT negation | predicate
 * predicate  := sum [ comparator sum
 *                   | [NOT] BETWEEN sum AND sum
 *                   | [NOT] IN '(' string (',' string)* ')'
 *                   | [NOT] LIKE string [ESCAPE string]
 *                   | IS [NOT] NULL ]
 * sum        := product (('+' | '-') product)*
 * product    := unary (('*' | '/') unary)*
 * unary      := ('+' | '-') unary | primary
 * primary    := literal | identifier | '(' condition ')'
 * </pre>
 */
final class SelectorParser {

    private enum Type {
        IDENTIFIER,
        KEYWORD,
        STRING,
        NUMBER,
        SYMBOL,
        END
    }

    private record Token(Type type, String text, int position) {

        boolean is(Type expected, String expectedText) {
            return type == expected && text.equals(expectedText);
        }
    }

    /** Reserved words, which are case insensitive and cannot be identifiers. */
    private static final Set<String> KEYWORDS =
            Set.of("NOT", "AND", "OR", "BETWEEN", "LIKE", "IN", "IS", "ESCAPE", "NULL", "TRUE", "FALSE");

    /** The only header fields a selector may reference. */
    private static final Set<String> HEADER_FIELDS = Set.of("JMSDeliveryMode", "JMSPriority", "JMSMessageID",
            "JMSTimestamp", "JMSCorrelationID", "JMSType");

    private final String selector;
    private int position;
    private Token token;

    SelectorParser(String selector) {
        this.selector = selector;
    }

    Expression parse() throws InvalidSelectorException {
        advance();
        Expression condition = condition();
        if (token.type != Type.END) {
            throw invalid("unexpected '" + token.text + "'", token.position);
        }
        return conditional(condition);
    }

    // ---- parser ----------------------------------------------------------

    /** Also what parentheses enclose, which is why it may turn out to be an arithmetic expression. */
    private Expression condition() throws InvalidSelectorException {
        Expression left = conjunction();
        while (accept(Type.KEYWORD, "OR")) {
            left = new Or(conditional(left), conditional(conjunction()));
        }
        return left;
    }

    private Expression conjunction() throws InvalidSelectorException {
        Expression left = negation();
        while (accept(Type.KEYWORD, "AND")) {
            left = new And(conditional(left), conditional(negation()));
        }
        return left;
    }

    private Expression negation() throws InvalidSelectorException {
        if (accept(Type.KEYWORD, "NOT")) {
            return new Not(conditional(negation()));
        }
        return predicate();
    }

    private Expression predicate() throws InvalidSelectorException {
        Expression left = sum();
        if (token.type == Type.SYMBOL) {
            Comparator comparator = switch (token.text) {
                case "=" -> Comparator.EQUAL;
                case "<>" -> Comparator.NOT_EQUAL;
                case "<" -> Comparator.LESS;
                case "<=" -> Comparator.LESS_OR_EQUAL;
                case ">" -> Comparator.GREATER;
                case ">=" -> Comparator.GREATER_OR_EQUAL;
                default -> null;
            };
            if (comparator != null) {
                advance();
                return comparison(comparator, left, sum());
            }
        }
        if (accept(Type.KEYWORD, "IS")) {
            boolean negated = accept(Type.KEYWORD, "NOT");
            expect(Type.KEYWORD, "NULL");
            return new IsNull(identifier(left, "IS NULL"), negated);
        }
        int start = token.position;
        boolean negated = accept(Type.KEYWORD, "NOT");
        if (accept(Type.KEYWORD, "BETWEEN")) {
            Expression low = arithmetic(sum());
            expect(Type.KEYWORD, "AND");
            Expression high = arithmetic(sum());
            arithmetic(left);
            // "a BETWEEN x AND y" is "a >= x AND a <= y", and its negation "a < x OR a > y".
            return negated
                    ? new Or(comparison(Comparator.LESS, left, low), comparison(Comparator.GREATER, left, high))
                    : new And(comparison(Comparator.GREATER_OR_EQUAL, left, low),
                            comparison(Comparator.LESS_OR_EQUAL, left, high));
        }
        if (accept(Type.KEYWORD, "IN")) {
            Identifier identifier = identifier(left, "IN");
            expect(Type.SYMBOL, "(");
            Set<String> values = new HashSet<>();
            do {
                values.add(string());
            } while (accept(Type.SYMBOL, ","));
            expect(Type.SYMBOL, ")");
            return new In(identifier, values, negated);
        }
        if (accept(Type.KEYWORD, "LIKE")) {
            Identifier identifier = identifier(left, "LIKE");
            int patternPosition = token.position;
            String pattern = string();
            Character escape = null;
            if (accept(Type.KEYWORD, "ESCAPE")) {
                int escapePosition = token.position;
                String escapeText = string();
                if (escapeText.length() != 1) {
                    throw invalid("the escape character must be a single character", escapePosition);
                }
                escape = escapeText.charAt(0);
            }
            return new Like(identifier, likePattern(pattern, escape, patternPosition), negated);
        }
        if (negated) {
            throw invalid("NOT must be followed by BETWEEN, IN or LIKE here", start);
        }
        return left;
    }

    /**
     * Builds a comparison after the semantic checks that need no message (§3.8.1.1): only
     * numbers have an order, and two operands whose types are both known must be of like types.
     * An identifier's type is only known at evaluation, where a mismatch simply yields false.
     */
    private Comparison comparison(Comparator comparator, Expression left, Expression right)
            throws InvalidSelectorException {
        boolean ordering = comparator != Comparator.EQUAL && comparator != Comparator.NOT_EQUAL;
        if (ordering) {
            arithmetic(left);
            arithmetic(right);
        } else if (left.kind() != Kind.ANY && right.kind() != Kind.ANY && left.kind() != right.kind()) {
            throw invalid("a " + name(left.kind()) + " cannot be compared with a " + name(right.kind()), 0);
        }
        return new Comparison(comparator, left, right);
    }

    private static String name(Kind kind) {
        return kind.name().toLowerCase(Locale.ROOT);
    }

    private Expression sum() throws InvalidSelectorException {
        Expression left = product();
        while (token.is(Type.SYMBOL, "+") || token.is(Type.SYMBOL, "-")) {
            Operator operator = token.text.equals("+") ? Operator.ADD : Operator.SUBTRACT;
            advance();
            left = new Arithmetic(operator, arithmetic(left), arithmetic(product()));
        }
        return left;
    }

    private Expression product() throws InvalidSelectorException {
        Expression left = unary();
        while (token.is(Type.SYMBOL, "*") || token.is(Type.SYMBOL, "/")) {
            Operator operator = token.text.equals("*") ? Operator.MULTIPLY : Operator.DIVIDE;
            advance();
            left = new Arithmetic(operator, arithmetic(left), arithmetic(unary()));
        }
        return left;
    }

    private Expression unary() throws InvalidSelectorException {
        if (accept(Type.SYMBOL, "+")) {
            return arithmetic(unary());
        }
        if (accept(Type.SYMBOL, "-")) {
            // The sign belongs to a literal that follows it: -9223372036854775808 has no positive half.
            if (token.type == Type.NUMBER) {
                Literal literal = number("-" + token.text, token.position);
                advance();
                return literal;
            }
            return new Negate(arithmetic(unary()));
        }
        return primary();
    }

    private Expression primary() throws InvalidSelectorException {
        Token current = token;
        switch (current.type) {
            case NUMBER -> {
                advance();
                return number(current.text, current.position);
            }
            case STRING -> {
                advance();
                return new Literal(current.text, Kind.STRING);
            }
            case IDENTIFIER -> {
                advance();
                // A JMS-prefixed name is a header field unless it is a JMSX or JMS_ property.
                if (current.text.startsWith("JMS") && !current.text.startsWith("JMSX")
                        && !current.text.startsWith("JMS_") && !HEADER_FIELDS.contains(current.text)) {
                    throw invalid("header field " + current.text + " cannot be used in a selector",
                            current.position);
                }
                return new Identifier(current.text);
            }
            case KEYWORD -> {
                if (current.text.equals("TRUE") || current.text.equals("FALSE")) {
                    advance();
                    return new Literal(current.text.equals("TRUE"), Kind.BOOLEAN);
                }
            }
            case SYMBOL -> {
                if (accept(Type.SYMBOL, "(")) {
                    Expression inner = condition();
                    expect(Type.SYMBOL, ")");
                    return inner;
                }
            }
            default -> {
            }
        }
        throw invalid(current.type == Type.END ? "the selector ends too soon" : "unexpected '" + current.text + "'",
                current.position);
    }

    /** Checks that an operand of a logical operator, or the selector itself, can be a condition. */
    private Expression conditional(Expression expression) throws InvalidSelectorException {
        if (expression.kind() == Kind.NUMBER || expression.kind() == Kind.STRING) {
            throw invalid("a condition is expected, not a " + name(expression.kind()), 0);
        }
        return expression;
    }

    /** Checks that an operand of an arithmetic operator can be a number. */
    private Expression arithmetic(Expression expression) throws InvalidSelectorException {
        if (expression.kind() == Kind.BOOLEAN || expression.kind() == Kind.STRING) {
            throw invalid("a number is expected, not a " + name(expression.kind()), 0);
        }
        return expression;
    }

    private Identifier identifier(Expression expression, String operator) throws InvalidSelectorException {
        if (expression instanceof Identifier identifier) {
            return identifier;
        }
        throw invalid(operator + " applies to an identifier", 0);
    }

    private String string() throws InvalidSelectorException {
        if (token.type != Type.STRING) {
            throw invalid("a string literal is expected", token.position);
        }
        String value = token.text;
        advance();
        return value;
    }

    /** Exact literals follow the Java integer syntax, approximate ones the Java floating-point syntax. */
    private Literal number(String text, int at) throws InvalidSelectorException {
        try {
            String digits = text.startsWith("-") ? text.substring(1) : text;
            String sign = text.startsWith("-") ? "-" : "";
            if (digits.startsWith("0x") || digits.startsWith("0X")) {
                return new Literal(Long.parseLong(sign + withoutLongSuffix(digits.substring(2)), 16), Kind.NUMBER);
            }
            if (digits.chars().anyMatch(c -> ".eEfFdD".indexOf(c) >= 0)) {
                return new Literal(Double.parseDouble(text), Kind.NUMBER);
            }
            digits = withoutLongSuffix(digits);
            int radix = digits.length() > 1 && digits.startsWith("0") ? 8 : 10;
            return new Literal(Long.parseLong(sign + digits, radix), Kind.NUMBER);
        } catch (NumberFormatException e) {
            throw invalid("'" + text + "' is not a number in the range of long or double", at);
        }
    }

    private static String withoutLongSuffix(String digits) {
        return digits.endsWith("L") || digits.endsWith("l") ? digits.substring(0, digits.length() - 1) : digits;
    }

    /** '%' is any sequence, '_' any one character, and the escape character quotes either. */
    private Pattern likePattern(String pattern, Character escape, int at) throws InvalidSelectorException {
        StringBuilder regex = new StringBuilder();
        for (int i = 0; i < pattern.length(); i++) {
            char c = pattern.charAt(i);
            if (escape != null && c == escape) {
                if (++i == pattern.length()) {
                    throw invalid("the pattern ends with its escape character", at);
                }
                regex.append(Pattern.quote(String.valueOf(pattern.charAt(i))));
            } else if (c == '%') {
                regex.append(".*");
            } else if (c == '_') {
                regex.append('.');
            } else {
                regex.append(Pattern.quote(String.valueOf(c)));
            }
        }
        return Pattern.compile(regex.toString(), Pattern.DOTALL);
    }

    private boolean accept(Type type, String text) throws InvalidSelectorException {
        if (token.is(type, text)) {
            advance();
            return true;
        }
        return false;
    }

    private void expect(Type type, String text) throws InvalidSelectorException {
        if (!accept(type, text)) {
            throw invalid("'" + text + "' is expected", token.position);
        }
    }

    private InvalidSelectorException invalid(String reason, int at) {
        return new InvalidSelectorException("invalid selector \"" + selector + "\": " + reason
                + (at > 0 ? " at position " + at : ""));
    }

    // ---- lexer -----------------------------------------------------------

    private void advance() throws InvalidSelectorException {
        while (position < selector.length() && Character.isWhitespace(selector.charAt(position))) {
            position++;
        }
        int start = position;
        if (position == selector.length()) {
            token = new Token(Type.END, "", start);
            return;
        }
        char c = selector.charAt(position);
        if (c == '\'') {
            token = new Token(Type.STRING, quoted(start), start);
        } else if (Character.isDigit(c) || c == '.' && position + 1 < selector.length()
                && Character.isDigit(selector.charAt(position + 1))) {
            token = new Token(Type.NUMBER, numberText(), start);
        } else if (Character.isJavaIdentifierStart(c)) {
            position++;
            while (position < selector.length() && Character.isJavaIdentifierPart(selector.charAt(position))) {
                position++;
            }
            String word = selector.substring(start, position);
            String keyword = word.toUpperCase(Locale.ROOT);
            token = KEYWORDS.contains(keyword)
                    ? new Token(Type.KEYWORD, keyword, start)
                    : new Token(Type.IDENTIFIER, word, start);
        } else {
            String two = selector.substring(position, Math.min(position + 2, selector.length()));
            String symbol = two.equals("<>") || two.equals("<=") || two.equals(">=") ? two : String.valueOf(c);
            if ("=<>()+-*/,".indexOf(c) < 0) {
                throw invalid("unexpected character '" + c + "'", start);
            }
            position += symbol.length();
            token = new Token(Type.SYMBOL, symbol, start);
        }
    }

    /** A quote inside a string literal is written twice. */
    private String quoted(int start) throws InvalidSelectorException {
        StringBuilder value = new StringBuilder();
        position++;
        while (position < selector.length()) {
            char c = selector.charAt(position++);
            if (c != '\'') {
                value.append(c);
            } else if (position < selector.length() && selector.charAt(position) == '\'') {
                value.append('\'');
                position++;
            } else {
                return value.toString();
            }
        }
        throw invalid("the string literal is not closed", start);
    }

    private String numberText() {
        int start = position;
        boolean hexadecimal = selector.startsWith("0x", position) || selector.startsWith("0X", position);
        while (position < selector.length()) {
            char c = selector.charAt(position);
            boolean exponentSign = !hexadecimal && (c == '+' || c == '-') && position > start
                    && "eE".indexOf(selector.charAt(position - 1)) >= 0;
            if (!Character.isLetterOrDigit(c) && c != '.' && !exponentSign) {
                break;
            }
            position++;
        }
        return selector.substring(start, position);
    }
}
