/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.solr.query;

import java.util.Collection;
import javax.annotation.CheckForNull;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.plugins.index.aggregate.NodeAggregator;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.OakSolrConfiguration;
import org.apache.jackrabbit.oak.query.QueryImpl;
import org.apache.jackrabbit.oak.query.fulltext.FullTextAnd;
import org.apache.jackrabbit.oak.query.fulltext.FullTextExpression;
import org.apache.jackrabbit.oak.query.fulltext.FullTextOr;
import org.apache.jackrabbit.oak.query.fulltext.FullTextTerm;
import org.apache.jackrabbit.oak.query.fulltext.FullTextVisitor;
import org.apache.jackrabbit.oak.spi.query.Cursor;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.IndexRow;
import org.apache.jackrabbit.oak.spi.query.PropertyValues;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.query.QueryIndex.FulltextQueryIndex;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.oak.commons.PathUtils.getName;

/**
 * A Solr based {@link QueryIndex}
 */
public class SolrQueryIndex implements FulltextQueryIndex {

    private static final String NATIVE_SOLR_QUERY = "native*solr";
    private static final String NATIVE_LUCENE_QUERY = "native*lucene";

    public static final String TYPE = "solr";

    private final Logger log = LoggerFactory.getLogger(SolrQueryIndex.class);

    private final String name;
    private final SolrServer solrServer;
    private final OakSolrConfiguration configuration;

    private final NodeAggregator aggregator;

    public SolrQueryIndex(String name, SolrServer solrServer, OakSolrConfiguration configuration) {
        this.name = name;
        this.solrServer = solrServer;
        this.configuration = configuration;
        // TODO this index should support aggregation in the same way as the Lucene index
        this.aggregator = null;
    }

    @Override
    public String getIndexName() {
        return name;
    }

    @Override
    public double getCost(Filter filter, NodeState root) {
        if (filter.getFullTextConstraint() == null && filter.getFulltextConditions() == null ||
                (filter.getPropertyRestrictions() != null && filter.getPropertyRestrictions().size() == 1 && filter.getPropertyRestriction(JcrConstants.JCR_UUID) != null)) {
            return Double.POSITIVE_INFINITY;
        }
        int cost = 10;
        Collection<Filter.PropertyRestriction> restrictions = filter.getPropertyRestrictions();
        if (restrictions != null) {
            cost /= 5;
        }
        if (filter.getPathRestriction() != null) {
            cost /= 2;
        }
        return cost;
    }

    @Override
    public String getPlan(Filter filter, NodeState nodeState) {
        return getQuery(filter).toString();
    }

    private SolrQuery getQuery(Filter filter) {

        SolrQuery solrQuery = new SolrQuery();
        setDefaults(solrQuery);

        StringBuilder queryBuilder = new StringBuilder();

        if (filter.getFullTextConstraint() != null) {
            queryBuilder.append(getFullTextQuery(filter.getFullTextConstraint()));
            queryBuilder.append(' ');
        } else if (filter.getFulltextConditions() != null) {
            Collection<String> fulltextConditions = filter.getFulltextConditions();
            for (String fulltextCondition : fulltextConditions) {
                queryBuilder.append(fulltextCondition).append(" ");
            }
        }

        Collection<Filter.PropertyRestriction> propertyRestrictions = filter.getPropertyRestrictions();
        if (propertyRestrictions != null && !propertyRestrictions.isEmpty()) {
            for (Filter.PropertyRestriction pr : propertyRestrictions) {
                // native query support
                if (NATIVE_SOLR_QUERY.equals(pr.propertyName) || NATIVE_LUCENE_QUERY.equals(pr.propertyName)) {
                    String nativeQueryString = String.valueOf(pr.first.getValue(pr.first.getType()));
                    if (isSupportedHttpRequest(nativeQueryString)) {
                        // pass through the native HTTP Solr request
                        String requestHandlerString = nativeQueryString.substring(0, nativeQueryString.indexOf('?'));
                        if (!"select".equals(requestHandlerString)) {
                            if (requestHandlerString.charAt(0) != '/') {
                                requestHandlerString = "/" + requestHandlerString;
                            }
                            solrQuery.setRequestHandler(requestHandlerString);
                        }
                        String parameterString = nativeQueryString.substring(nativeQueryString.indexOf('?') + 1);
                        for (String param : parameterString.split("&")) {
                            String[] kv = param.split("=");
                            if (kv.length != 2) {
                                throw new RuntimeException("Unparsable native HTTP Solr query");
                            } else {
                                if ("stream.body".equals(kv[0])) {
                                    kv[0] = "q";
                                    String mltFlString = "mlt.fl=";
                                    int mltFlIndex = parameterString.indexOf(mltFlString);
                                    if (mltFlIndex > -1) {
                                        int beginIndex = mltFlIndex + mltFlString.length();
                                        int endIndex = parameterString.indexOf('&', beginIndex);
                                        String fields;
                                        if (endIndex > beginIndex) {
                                            fields = parameterString.substring(beginIndex, endIndex);
                                        } else {
                                            fields = parameterString.substring(beginIndex);
                                        }
                                        kv[1] = "_query_:\"{!dismax qf=" + fields + " q.op=OR}" + kv[1] + "\"";
                                    }
                                }
                                solrQuery.setParam(kv[0], kv[1]);
                            }
                        }
                        return solrQuery; // every other restriction is not considered
                    } else {
                        queryBuilder.append(nativeQueryString);
                    }
                } else {
                    if (pr.propertyName.contains("/")) {
                        // cannot handle child-level property restrictions
                        continue;
                    }

                    if ("rep:excerpt".equals(pr.propertyName)) {
                        continue;
                    }

                    String first = null;
                    if (pr.first != null) {
                        first = partialEscape(String.valueOf(pr.first.getValue(pr.first.getType()))).toString();
                    }
                    String last = null;
                    if (pr.last != null) {
                        last = partialEscape(String.valueOf(pr.last.getValue(pr.last.getType()))).toString();
                    }

                    String prField = configuration.getFieldForPropertyRestriction(pr);
                    CharSequence fieldName = partialEscape(prField != null ?
                            prField : pr.propertyName);
                    if ("jcr\\:path".equals(fieldName.toString())) {
                        queryBuilder.append(configuration.getPathField());
                        queryBuilder.append(':');
                        queryBuilder.append(first);
                    } else {
                        if (pr.first != null && pr.last != null && pr.first.equals(pr.last)) {
                            queryBuilder.append(fieldName).append(':');
                            queryBuilder.append(first);
                        } else if (pr.first == null && pr.last == null) {
                            if (!queryBuilder.toString().contains(fieldName + ":")) {
                                queryBuilder.append(fieldName).append(':');
                                queryBuilder.append('*');
                            }
                        } else if ((pr.first != null && pr.last == null) || (pr.last != null && pr.first == null) || (!pr.first.equals(pr.last))) {
                            // TODO : need to check if this works for all field types (most likely not!)
                            queryBuilder.append(fieldName).append(':');
                            queryBuilder.append(createRangeQuery(first, last, pr.firstIncluding, pr.lastIncluding));
                        } else if (pr.isLike) {
                            // TODO : the current parameter substitution is not expected to work well
                            queryBuilder.append(fieldName).append(':');
                            queryBuilder.append(partialEscape(String.valueOf(pr.first.getValue(pr.first.getType())).replace('%', '*').replace('_', '?')));
                        } else {
                            throw new RuntimeException("[unexpected!] not handled case");
                        }
                    }
                }
                queryBuilder.append(" ");
            }
        }

        String[] pts = filter.getPrimaryTypes().toArray(new String[filter.getPrimaryTypes().size()]);
        for (int i = 0; i < pts.length; i++) {
            String pt = pts[i];
            if (i == 0) {
                queryBuilder.append("(");
            }
            if (i > 0 && i < pts.length) {
                queryBuilder.append("OR ");
            }
            queryBuilder.append("jcr\\:primaryType").append(':').append(partialEscape(pt)).append(" ");
            if (i == pts.length - 1) {
                queryBuilder.append(")");
                queryBuilder.append(' ');
            }
        }

        Filter.PathRestriction pathRestriction = filter.getPathRestriction();
        if (pathRestriction != null) {
            String path = purgePath(filter);
            String fieldName = configuration.getFieldForPathRestriction(pathRestriction);
            if (fieldName != null) {
                queryBuilder.append(fieldName);
                queryBuilder.append(':');
                queryBuilder.append(path);
            }
        }

        if (queryBuilder.length() == 0) {
            queryBuilder.append("*:*");
        }
        String escapedQuery = queryBuilder.toString();
        solrQuery.setQuery(escapedQuery);

        if (log.isDebugEnabled()) {
            log.debug("JCR query {} has been converted to Solr query {}",
                    filter.getQueryStatement(), solrQuery.toString());
        }

        return solrQuery;
    }

    private String getFullTextQuery(FullTextExpression ft) {
        final StringBuilder fullTextString = new StringBuilder();
        ft.accept(new FullTextVisitor() {

            @Override
            public boolean visit(FullTextOr or) {
                fullTextString.append('(');
                for (int i = 0; i < or.list.size(); i++) {
                    if (i > 0 && i < or.list.size()) {
                        fullTextString.append(" OR ");
                    }
                    FullTextExpression e = or.list.get(i);
                    String orTerm = getFullTextQuery(e);
                    fullTextString.append(orTerm);
                }
                fullTextString.append(')');
                fullTextString.append(' ');
                return true;
            }

            @Override
            public boolean visit(FullTextAnd and) {
                fullTextString.append('(');
                for (int i = 0; i < and.list.size(); i++) {
                    if (i > 0 && i < and.list.size()) {
                        fullTextString.append(" AND ");
                    }
                    FullTextExpression e = and.list.get(i);
                    String andTerm = getFullTextQuery(e);
                    fullTextString.append(andTerm);
                }
                fullTextString.append(')');
                fullTextString.append(' ');
                return true;
            }

            @Override
            public boolean visit(FullTextTerm term) {
                if (term.isNot()) {
                    fullTextString.append('-');
                }
                String p = term.getPropertyName();
                if (p != null && p.indexOf('/') >= 0) {
                    p = getName(p);
                }
                if (p == null) {
                    p = configuration.getCatchAllField();
                }
                fullTextString.append(partialEscape(p));
                fullTextString.append(':');
                String termText = term.getText();
                if (termText.indexOf(' ') > 0) {
                    fullTextString.append('"');
                }
                fullTextString.append(termText.replace("/", "\\/").replace(":", "\\:"));
                if (termText.indexOf(' ') > 0) {
                    fullTextString.append('"');
                }
                String boost = term.getBoost();
                if (boost != null) {
                    fullTextString.append('^');
                    fullTextString.append(boost);
                }
                fullTextString.append(' ');
                return true;
            }
        });
        return fullTextString.toString();
    }

    private boolean isSupportedHttpRequest(String nativeQueryString) {
        // the query string starts with ${supported-handler.selector}?
        return nativeQueryString.matches("(mlt|query|select|get)\\\\?.*");
    }

    private void setDefaults(SolrQuery solrQuery) {
        solrQuery.setParam("q.op", "AND");
        solrQuery.setParam("fl", "* score");
        String catchAllField = configuration.getCatchAllField();
        if (catchAllField != null && catchAllField.length() > 0) {
            solrQuery.setParam("df", catchAllField);
        }

        solrQuery.setParam("rows", "100000");
    }

    private static String createRangeQuery(String first, String last, boolean firstIncluding, boolean lastIncluding) {
        // TODO : handle inclusion / exclusion of bounds
        return "[" + (first != null ? first : "*") + " TO " + (last != null ? last : "*") + "]";
    }

    private static String purgePath(Filter filter) {
        return partialEscape(filter.getPath()).toString();
    }


    // partially borrowed from SolrPluginUtils#partialEscape
    private static CharSequence partialEscape(CharSequence s) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '\\' || c == '!' || c == '(' || c == ')' ||
                    c == ':' || c == '^' || c == '[' || c == ']' || c == '/' ||
                    c == '{' || c == '}' || c == '~' || c == '*' || c == '?' ||
                    c == '-' || c == ' ') {
                sb.append('\\');
            }
            sb.append(c);
        }
        return sb;
    }

    @Override
    public Cursor query(Filter filter, NodeState root) {
        if (log.isDebugEnabled()) {
            log.debug("converting filter {}", filter);
        }
        Cursor cursor;
        try {
            SolrQuery query = getQuery(filter);
            if (log.isDebugEnabled()) {
                log.debug("sending query {}", query);
            }
            QueryResponse queryResponse = solrServer.query(query);
            if (log.isDebugEnabled()) {
                log.debug("getting response {}", queryResponse);
            }
            cursor = new SolrCursor(queryResponse, query);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return cursor;
    }


    private class SolrCursor implements Cursor {

        private SolrDocumentList results;

        private SolrQuery query;

        private int counter;
        private int offset;

        public SolrCursor(QueryResponse queryResponse, SolrQuery query) {
            this.results = queryResponse.getResults();
            this.counter = 0;
            this.offset = 0;
            this.query = query;
        }

        @Override
        public boolean hasNext() {
            return results != null && offset + counter < results.getNumFound();
        }

        @Override
        public void remove() {
            results.remove(counter);
        }

        public IndexRow next() {
            if (counter < results.size() || updateResults()) {
                final SolrDocument doc = results.get(counter);
                counter++;
                return new IndexRow() {
                    @Override
                    public String getPath() {
                        return String.valueOf(doc.getFieldValue(
                                configuration.getPathField()));
                    }

                    @Override
                    public PropertyValue getValue(String columnName) {
                        if (QueryImpl.JCR_SCORE.equals(columnName)) {
                            float score = 0f;
                            Object scoreObj = doc.get("score");
                            if (scoreObj != null) {
                                score = (Float) scoreObj;
                            }
                            return PropertyValues.newDouble((double) score);
                        }
                        Object o = doc.getFieldValue(columnName);
                        return o == null ? null : PropertyValues.newString(o.toString());
                    }

                };
            } else {
                return null;
            }
        }

        private boolean updateResults() {
            int newOffset = offset + results.size();
            query.setParam("start", String.valueOf(newOffset));
            try {
                QueryResponse queryResponse = solrServer.query(query);
                SolrDocumentList localResults = queryResponse.getResults();
                boolean hasMoreResults = localResults.size() > 0;
                if (hasMoreResults) {
                    counter = 0;
                    offset = newOffset;
                    results = localResults;
                } else {
                    query.setParam("start", String.valueOf(offset));
                }
                return hasMoreResults;
            } catch (SolrServerException e) {
                throw new RuntimeException("error retrieving paged results", e);
            }
        }
    }


    @Override
    @CheckForNull
    public NodeAggregator getNodeAggregator() {
        return aggregator;
    }

}
