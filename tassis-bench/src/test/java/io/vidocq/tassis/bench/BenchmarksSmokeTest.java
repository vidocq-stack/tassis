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
package io.vidocq.tassis.bench;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collection;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;
import org.openjdk.jmh.runner.options.VerboseMode;

/**
 * Every benchmark runs, with every value of its parameters, and measures something. One short
 * iteration in this JVM: what it measures is published nowhere, the figures of {@code BENCH.md}
 * come from {@code benchmarks.jar}.
 */
@Timeout(600)
class BenchmarksSmokeTest {

    @ParameterizedTest
    @ValueSource(
            classes = {
                RoundTripBench.class,
                ContentionBench.class,
                IdleWaitersBench.class,
                BacklogBench.class,
                ListenerBench.class,
                DrainBench.class,
                SessionBench.class,
                PoolBench.class
            })
    void aBenchmarkRunsAndMeasures(Class<?> benchmark) throws Exception {
        Options options = new OptionsBuilder()
                .include("^" + benchmark.getName().replace(".", "\\.") + "\\.")
                .forks(0)
                .warmupIterations(0)
                .measurementIterations(1)
                .measurementTime(TimeValue.milliseconds(100))
                .shouldFailOnError(true)
                .verbosity(VerboseMode.SILENT)
                .build();

        Collection<RunResult> results = new Runner(options).run();

        assertFalse(results.isEmpty(), "no result for " + benchmark.getSimpleName());
        for (RunResult result : results) {
            double score = result.getPrimaryResult().getScore();
            assertTrue(score > 0 && Double.isFinite(score), result.getParams().getBenchmark() + " measured " + score);
        }
    }
}
