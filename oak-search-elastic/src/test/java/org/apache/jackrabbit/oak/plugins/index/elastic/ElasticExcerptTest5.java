/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.jackrabbit.oak.plugins.index.elastic;

import com.google.common.base.Joiner;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.InitialContentHelper;
import org.apache.jackrabbit.oak.api.*;
import org.apache.jackrabbit.oak.plugins.index.ExcerptTest5;
import org.apache.jackrabbit.oak.plugins.index.IndexOptions;
import org.apache.jackrabbit.oak.plugins.index.TestRepository;
import org.apache.jackrabbit.oak.plugins.index.TestUtil;
import org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants;
import org.apache.jackrabbit.oak.plugins.index.search.IndexFormatVersion;
import org.apache.jackrabbit.oak.plugins.memory.ArrayBasedBlob;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.query.AbstractQueryTest;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static com.google.common.collect.Lists.newArrayList;
import static org.apache.jackrabbit.oak.api.QueryEngine.NO_BINDINGS;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.*;
import static org.junit.Assert.*;

public class ElasticExcerptTest5 extends ExcerptTest5 {

    @ClassRule
    public static final ElasticConnectionRule elasticRule = new ElasticConnectionRule(
            ElasticTestUtils.ELASTIC_CONNECTION_STRING);

    public ElasticExcerptTest5() {
        indexOptions = new ElasticIndexOptions();
    }


    @Override
    protected ContentRepository createRepository() {
        ElasticTestRepositoryBuilder builder = new ElasticTestRepositoryBuilder(elasticRule);
        builder.setNodeStore(new MemoryNodeStore(InitialContentHelper.INITIAL_CONTENT));
        TestRepository repositoryOptionsUtil = builder.build();

        return repositoryOptionsUtil.getOak().createContentRepository();
    }
}