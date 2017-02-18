/*
 * Copyright 2016 Classmethod, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package org.janusgraph.graphdb.tupl;

import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.graphdb.JanusGraphPerformanceMemoryTest;

import org.janusgraph.TuplStorageSetup;
import org.junit.Rule;
import org.junit.rules.TestName;

/**
 * Performance and memory leak tests. Take a long time.
 * @author Alexander Patrikalakis
 *
 */
public class TuplGraphPerformanceMemoryTest extends JanusGraphPerformanceMemoryTest {

    @Rule
    public TestName testName = new TestName();

    @Override
    public WriteConfiguration getConfiguration() {
        return TuplStorageSetup.getTuplGraphBaseConfiguration("TuplGraphPerformanceMemoryTest#"
                + testName.getMethodName()).getConfiguration();
    }

}
