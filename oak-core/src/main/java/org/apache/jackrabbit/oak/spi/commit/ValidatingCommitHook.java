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
package org.apache.jackrabbit.oak.spi.commit;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

import static org.apache.jackrabbit.oak.plugins.memory.MemoryNodeState.EMPTY_NODE;

/**
 * This commit hook implementation validates the changes to be committed
 * against all {@link Validator}s provided by the {@link ValidatorProvider}
 * passed to the class' constructor.
 */
public class ValidatingCommitHook implements CommitHook {

    private final ValidatorProvider validatorProvider;

    /**
     * Create a new commit hook which validates the commit against all
     * {@link Validator}s provided by {@code validatorProvider}.
     * @param validatorProvider  validator provider
     */
    public ValidatingCommitHook(ValidatorProvider validatorProvider) {
        this.validatorProvider = validatorProvider;
    }

    @Override
    public NodeState beforeCommit(
            NodeStore store, NodeState before, NodeState after)
            throws CommitFailedException {
        Validator validator = validatorProvider.getRootValidator(before, after);
        ValidatorDiff.validate(validator, store, before, after);
        return after;
    }

    @Override
    public void afterCommit(NodeStore store, NodeState before, NodeState after) {
        // nothing to do here
    }

    //------------------------------------------------------------< private >---

    private static class ValidatorDiff implements NodeStateDiff {

        private final Validator validator;

        private final NodeStore store;

        /**
         * Checked exceptions don't compose. So we need to hack around.
         * See http://markmail.org/message/ak67n5k7mr3vqylm and
         * http://markmail.org/message/bhocbruikljpuhu6
         */
        private CommitFailedException exception;

        /**
         * Validates the given subtree by diffing and recursing through it.
         *
         * @param validator validator for the root of the subtree
         * @param store store to where the subtree lives
         * @param before state of the original subtree
         * @param after state of the modified subtree
         * @throws CommitFailedException if validation failed
         */
        public static void validate(Validator validator, NodeStore store,
                NodeState before, NodeState after) throws CommitFailedException {
            new ValidatorDiff(validator, store).validate(before, after);
        }

        private ValidatorDiff(Validator validator, NodeStore store) {
            this.validator = validator;
            this.store = store;
        }

        private void validate(NodeState before, NodeState after)
                throws CommitFailedException {
            store.compare(before, after, this);
            if (exception != null) {
                throw exception;
            }
        }

        //-------------------------------------------------< NodeStateDiff >--

        @Override
        public void propertyAdded(PropertyState after) {
            if (exception == null) {
                try {
                    validator.propertyAdded(after);
                } catch (CommitFailedException e) {
                    exception = e;
                }
            }
        }

        @Override
        public void propertyChanged(PropertyState before, PropertyState after) {
            if (exception == null) {
                try {
                    validator.propertyChanged(before, after);
                } catch (CommitFailedException e) {
                    exception = e;
                }
            }
        }

        @Override
        public void propertyDeleted(PropertyState before) {
            if (exception == null) {
                try {
                    validator.propertyDeleted(before);
                } catch (CommitFailedException e) {
                    exception = e;
                }
            }
        }

        @Override
        public void childNodeAdded(String name, NodeState after) {
            if (exception == null) {
                try {
                    Validator v = validator.childNodeAdded(name, after);
                    if (v != null) {
                        validate(v, store, EMPTY_NODE, after);
                    }
                } catch (CommitFailedException e) {
                    exception = e;
                }
            }
        }

        @Override
        public void childNodeChanged(
                String name, NodeState before, NodeState after) {
            if (exception == null) {
                try {
                    Validator v =
                            validator.childNodeChanged(name, before, after);
                    if (v != null) {
                        validate(v, store, before, after);
                    }
                } catch (CommitFailedException e) {
                    exception = e;
                }
            }
        }

        @Override
        public void childNodeDeleted(String name, NodeState before) {
            if (exception == null) {
                try {
                    Validator v = validator.childNodeDeleted(name, before);
                    if (v != null) {
                        validate(v, store, before, EMPTY_NODE);
                    }
                } catch (CommitFailedException e) {
                    exception = e;
                }
            }
        }

    }

}
