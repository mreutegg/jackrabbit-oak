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

package org.apache.jackrabbit.oak.plugins.index.lucene;

import javax.jcr.PropertyType;

import com.google.common.collect.Iterables;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.tree.ImmutableTree;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.lucene.codecs.Codec;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexDefinition.IndexingRule;
import org.junit.Test;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.ImmutableSet.of;
import static javax.jcr.PropertyType.TYPENAME_LONG;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.INCLUDE_PROPERTY_NAMES;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.INCLUDE_PROPERTY_TYPES;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.INDEX_RULES;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.PROP_NODE;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.plugins.memory.PropertyStates.createProperty;
import static org.apache.jackrabbit.oak.plugins.nodetype.write.InitialContent.INITIAL_CONTENT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class IndexDefinitionTest {
    private Codec oakCodec = new OakCodec();

    private NodeState root = INITIAL_CONTENT;

    private NodeBuilder builder = root.builder();

    @Test
    public void fullTextEnabled() throws Exception{
        IndexDefinition defn = new IndexDefinition(root, builder.getNodeState());
        assertTrue("By default fulltext is enabled", defn.isFullTextEnabled());
        assertTrue("By default everything is indexed", defn.includeProperty("foo"));
        assertFalse("Property types need to be defined", defn.includePropertyType(PropertyType.DATE));
        assertTrue("For fulltext storage is enabled", defn.isStored("foo"));

        assertFalse(defn.skipTokenization("foo"));
        assertTrue(defn.skipTokenization("jcr:uuid"));
    }

    @Test
    public void propertyTypes() throws Exception{
        builder.setProperty(createProperty(INCLUDE_PROPERTY_TYPES, of(TYPENAME_LONG), STRINGS));
        builder.setProperty(createProperty(INCLUDE_PROPERTY_NAMES, of("foo" , "bar"), STRINGS));
        builder.setProperty(LuceneIndexConstants.FULL_TEXT_ENABLED, false);
        IndexDefinition defn = new IndexDefinition(root, builder.getNodeState());

        assertFalse(defn.isFullTextEnabled());
        assertFalse("If fulltext disabled then nothing stored",defn.isStored("foo"));

        assertTrue(defn.includePropertyType(PropertyType.LONG));
        assertFalse(defn.includePropertyType(PropertyType.STRING));

        assertTrue(defn.includeProperty("foo"));
        assertTrue(defn.includeProperty("bar"));
        assertFalse(defn.includeProperty("baz"));

        assertTrue(defn.skipTokenization("foo"));
    }

    @Test
    public void propertyDefinition() throws Exception{
        builder.child(PROP_NODE).child("foo").setProperty(LuceneIndexConstants.PROP_TYPE, PropertyType.TYPENAME_DATE);
        builder.setProperty(createProperty(INCLUDE_PROPERTY_NAMES, of("foo" , "bar"), STRINGS));
        IndexDefinition defn = new IndexDefinition(root, builder.getNodeState());

        assertTrue(defn.hasPropertyDefinition("foo"));
        assertFalse(defn.hasPropertyDefinition("bar"));

        assertEquals(PropertyType.DATE, defn.getPropDefn("foo").getPropertyType());
    }

    @Test
    public void propertyDefinitionWithExcludes() throws Exception{
        builder.child(PROP_NODE).child("foo").setProperty(LuceneIndexConstants.PROP_TYPE, PropertyType.TYPENAME_DATE);
        IndexDefinition defn = new IndexDefinition(root, builder.getNodeState());

        assertTrue(defn.hasPropertyDefinition("foo"));
        assertFalse(defn.hasPropertyDefinition("bar"));

        assertEquals(PropertyType.DATE, defn.getPropDefn("foo").getPropertyType());
    }

    @Test
    public void codecConfig() throws Exception{
        IndexDefinition defn = new IndexDefinition(root, builder.getNodeState());
        assertNotNull(defn.getCodec());
        assertEquals(oakCodec.getName(), defn.getCodec().getName());

        builder.setProperty(LuceneIndexConstants.FULL_TEXT_ENABLED, false);
        defn = new IndexDefinition(root, builder.getNodeState());
        assertNull(defn.getCodec());

        Codec simple = Codec.getDefault();
        builder.setProperty(LuceneIndexConstants.CODEC_NAME, simple.getName());
        defn = new IndexDefinition(root, builder.getNodeState());
        assertNotNull(defn.getCodec());
        assertEquals(simple.getName(), defn.getCodec().getName());
    }

    @Test
    public void relativeProperty() throws Exception{
        builder.setProperty(createProperty(INCLUDE_PROPERTY_NAMES, of("foo" , "foo1/bar"), STRINGS));
        IndexDefinition defn = new IndexDefinition(root, builder.getNodeState());

        assertEquals(1, defn.getRelativeProps().size());
        assertEquals(new RelativeProperty("foo1/bar"), Iterables.getFirst(defn.getRelativeProps(), null));
        assertTrue(defn.hasRelativeProperty("bar"));
        assertFalse(defn.hasRelativeProperty("foo"));
    }

    @Test
    public void relativePropertyConfig() throws Exception{
        builder.child(PROP_NODE).child("foo1").child("bar").setProperty(LuceneIndexConstants.PROP_TYPE, PropertyType.TYPENAME_DATE);
        builder.child(PROP_NODE).child("foo2").child("bar2").child("baz").setProperty(LuceneIndexConstants.PROP_TYPE, PropertyType.TYPENAME_LONG);
        builder.setProperty(createProperty(INCLUDE_PROPERTY_NAMES, of("foo", "foo1/bar", "foo2/bar2/baz"), STRINGS));
        IndexDefinition defn = new IndexDefinition(root, builder.getNodeState());

        assertEquals(2, defn.getRelativeProps().size());
        assertNull(defn.getPropDefn("foo"));
        assertNotNull(defn.getPropDefn("foo1/bar"));
        assertEquals(PropertyType.DATE, defn.getPropDefn("foo1/bar").getPropertyType());
        assertEquals(PropertyType.LONG, defn.getPropDefn("foo2/bar2/baz").getPropertyType());
    }

    @Test
    public void indexRuleSanity() throws Exception{
        NodeBuilder rules = builder.child(INDEX_RULES);
        rules.child("nt:folder").setProperty(LuceneIndexConstants.FIELD_BOOST, 2.0);
        child(rules, "nt:folder/properties/prop1")
                .setProperty(LuceneIndexConstants.FIELD_BOOST, 3.0)
                .setProperty(LuceneIndexConstants.PROP_TYPE, PropertyType.TYPENAME_BOOLEAN);

        IndexDefinition defn = new IndexDefinition(root, builder.getNodeState());

        assertNull(defn.getApplicableIndexingRule(newTree(newNode("nt:base"))));

        IndexingRule rule1 = defn.getApplicableIndexingRule(newTree(newNode("nt:folder")));
        assertNotNull(rule1);
        assertEquals(2.0f, rule1.boost, 0);

        assertTrue(rule1.isIndexed("prop1"));
        assertFalse(rule1.isIndexed("prop2"));

        PropertyDefinition pd = rule1.getConfig("prop1");
        assertEquals(3.0f, pd.boost, 0);
        assertEquals(PropertyType.BOOLEAN, pd.propertyType);
    }

    @Test
    public void indexRuleInheritance() throws Exception{
        NodeBuilder rules = builder.child(INDEX_RULES);
        rules.child("nt:hierarchyNode").setProperty(LuceneIndexConstants.FIELD_BOOST, 2.0);

        IndexDefinition defn = new IndexDefinition(root, builder.getNodeState());

        assertNull(defn.getApplicableIndexingRule(newTree(newNode("nt:base"))));
        assertNotNull(defn.getApplicableIndexingRule(newTree(newNode("nt:hierarchyNode"))));
        assertNotNull(defn.getApplicableIndexingRule(newTree(newNode("nt:folder"))));

        //TODO Inheritance and mixin
    }

    @Test
    public void indexRuleWithPropertyRegEx() throws Exception{
        NodeBuilder rules = builder.child(INDEX_RULES);
        rules.child("nt:folder");
        child(rules, "nt:folder/properties/prop1")
                .setProperty(LuceneIndexConstants.FIELD_BOOST, 3.0);
        child(rules, "nt:folder/properties/prop2")
                .setProperty(LuceneIndexConstants.PROP_NAME, "foo.*")
                .setProperty(LuceneIndexConstants.PROP_IS_REGEX, true)
                .setProperty(LuceneIndexConstants.FIELD_BOOST, 4.0);

        IndexDefinition defn = new IndexDefinition(root, builder.getNodeState());

        IndexingRule rule1 = defn.getApplicableIndexingRule(newTree(newNode("nt:folder")));
        assertNotNull(rule1);

        assertTrue(rule1.isIndexed("prop1"));
        assertFalse(rule1.isIndexed("prop2"));
        assertTrue(rule1.isIndexed("fooProp"));

        PropertyDefinition pd = rule1.getConfig("fooProp2");
        assertEquals(4.0f, pd.boost, 0);
    }

    private static Tree newTree(NodeBuilder nb){
        return new ImmutableTree(nb.getNodeState());
    }

    private static NodeBuilder newNode(String typeName){
        NodeBuilder builder = EMPTY_NODE.builder();
        builder.setProperty(JcrConstants.JCR_PRIMARYTYPE, typeName);
        return builder;
    }

    private static NodeBuilder child(NodeBuilder nb, String path) {
        for (String name : PathUtils.elements(checkNotNull(path))) {
            nb = nb.child(name);
        }
        return nb;
    }
}
