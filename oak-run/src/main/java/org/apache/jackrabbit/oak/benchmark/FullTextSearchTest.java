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

package org.apache.jackrabbit.oak.benchmark;

import java.io.File;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;

import javax.jcr.Node;
import javax.jcr.Repository;
import javax.jcr.Session;
import javax.jcr.query.Query;
import javax.jcr.query.QueryManager;
import javax.jcr.query.QueryResult;
import javax.jcr.query.RowIterator;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamReader;
import javax.xml.transform.stream.StreamSource;

import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.jackrabbit.oak.fixture.JcrCustomizer;
import org.apache.jackrabbit.oak.fixture.OakRepositoryFixture;
import org.apache.jackrabbit.oak.fixture.RepositoryFixture;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.LuceneInitializerHelper;
import org.apache.jackrabbit.util.Text;

import static com.google.common.base.Preconditions.checkArgument;

public class FullTextSearchTest extends AbstractTest {
    private int maxSampleSize = 100;
    private final File dump;
    private final boolean doReport;
    private List<String> sampleSet;
    private Random random;
    private Reader reader;

    public FullTextSearchTest(File dump, boolean doReport) {
        this.dump = dump;
        this.doReport = doReport;
    }

    @Override
    public void beforeSuite() throws Exception {
        random = new Random(42); //fixed seed
        Session importSession = loginWriter();
        sampleSet = importWikipedia(importSession);
        reader = new Reader();
    }

    @Override
    protected void runTest() throws Exception {
        reader.call();
    }

    private class Reader implements Callable<Boolean> {
        private final Session session = loginWriter();
        private final String word = Text.escapeIllegalJcrChars(sampleSet.get(random.nextInt(sampleSet.size())));

        @SuppressWarnings("deprecation")
        @Override
        public Boolean call() throws Exception {
            QueryManager qm = session.getWorkspace().getQueryManager();
            Query q = qm.createQuery("/jcr:root//*[jcr:contains(@text, '" + word + "')] order by jcr:score()", Query.XPATH);
            QueryResult r = q.execute();
            RowIterator it = r.getRows();
            checkArgument(it.hasNext(), "Not able to find entry with text [%s]", word);
            return it.hasNext();
        }
    }

    @Override
    protected Repository[] createRepository(RepositoryFixture fixture) throws Exception {
        if (fixture instanceof OakRepositoryFixture) {
            return ((OakRepositoryFixture) fixture).setUpCluster(1, new JcrCustomizer() {
                @Override
                public Jcr customize(Jcr jcr) {
                    jcr.with(new LuceneIndexProvider())
                            .with(new LuceneIndexEditorProvider())
                            .with(new LuceneInitializerHelper("luceneGlobal", null));
                    return jcr;
                }
            });
        }
        return super.createRepository(fixture);
    }

    private List<String> importWikipedia(Session session) throws Exception {
        long start = System.currentTimeMillis();
        int count = 0;
        Set<String> sampleWords = Sets.newHashSet();

        checkArgument(dump.exists(), "Dump file %s does not exist", dump.getAbsolutePath());
        if (doReport) {
            System.out.format("Importing %s...%n", dump);
        }
        Node wikipedia = session.getRootNode().addNode(
                "wikipedia", "nt:unstructured");

        String title = null;
        String text = null;
        XMLInputFactory factory = XMLInputFactory.newInstance();
        XMLStreamReader reader =
                factory.createXMLStreamReader(new StreamSource(dump));
        while (reader.hasNext()) {
            switch (reader.next()) {
                case XMLStreamConstants.START_ELEMENT:
                    if ("title".equals(reader.getLocalName())) {
                        title = reader.getElementText();
                    } else if ("text".equals(reader.getLocalName())) {
                        text = reader.getElementText();
                    }
                    break;
                case XMLStreamConstants.END_ELEMENT:
                    if ("page".equals(reader.getLocalName())) {
                        String name = Text.escapeIllegalJcrChars(title);
                        Node page = wikipedia.addNode(name);
                        page.setProperty("title", title);
                        page.setProperty("text", Text.escapeIllegalJcrChars(text));
                        count++;

                        if (count % 1000 == 0
                                && sampleWords.size() < maxSampleSize
                                && text != null) {
                            List<String> words = Splitter.on(CharMatcher.BREAKING_WHITESPACE)
                                    .trimResults().splitToList(text);
                            if (!words.isEmpty()) {
                                sampleWords.add(words.get(words.size() / 2));
                            }
                        }

                        if (doReport && count % 1000 == 0) {
                            long millis = System.currentTimeMillis() - start;
                            System.out.format(
                                    "Added %d pages in %d seconds (%.2fms/page)%n",
                                    count, millis / 1000, (double) millis / count);
                        }
                    }
                    break;
            }
        }

        session.save();

        if (doReport) {
            long millis = System.currentTimeMillis() - start;
            System.out.format(
                    "Imported %d pages in %d seconds (%.2fms/page)%n",
                    count, millis / 1000, (double) millis / count);
        }
        return Lists.newArrayList(sampleWords);
    }
}
