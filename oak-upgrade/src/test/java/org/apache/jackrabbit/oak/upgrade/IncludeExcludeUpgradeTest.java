package org.apache.jackrabbit.oak.upgrade;

import org.apache.jackrabbit.commons.JcrUtils;
import org.apache.jackrabbit.core.RepositoryContext;
import org.apache.jackrabbit.core.config.RepositoryConfig;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Test;

import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import java.io.File;

public class IncludeExcludeUpgradeTest extends AbstractRepositoryUpgradeTest {

    @Override
    protected void createSourceContent(Repository repository) throws Exception {
        final Session session = repository.login(CREDENTIALS);
        JcrUtils.getOrCreateByPath("/content/foo/de", "nt:folder", session);
        JcrUtils.getOrCreateByPath("/content/foo/en", "nt:folder", session);
        JcrUtils.getOrCreateByPath("/content/foo/fr", "nt:folder", session);
        JcrUtils.getOrCreateByPath("/content/foo/it", "nt:folder", session);
        JcrUtils.getOrCreateByPath("/content/assets/foo", "nt:folder", session);
        JcrUtils.getOrCreateByPath("/content/assets/foo/2015", "nt:folder", session);
        JcrUtils.getOrCreateByPath("/content/assets/foo/2015/02", "nt:folder", session);
        JcrUtils.getOrCreateByPath("/content/assets/foo/2015/01", "nt:folder", session);
        JcrUtils.getOrCreateByPath("/content/assets/foo/2014", "nt:folder", session);
        JcrUtils.getOrCreateByPath("/content/assets/foo/2013", "nt:folder", session);
        JcrUtils.getOrCreateByPath("/content/assets/foo/2012", "nt:folder", session);
        JcrUtils.getOrCreateByPath("/content/assets/foo/2011", "nt:folder", session);
        JcrUtils.getOrCreateByPath("/content/assets/foo/2010", "nt:folder", session);
        JcrUtils.getOrCreateByPath("/content/assets/foo/2010/12", "nt:folder", session);
        JcrUtils.getOrCreateByPath("/content/assets/foo/2010/11", "nt:folder", session);
        session.save();
    }

    @Override
    protected void doUpgradeRepository(File source, NodeStore target) throws RepositoryException {
        final RepositoryConfig config = RepositoryConfig.create(source);
        final RepositoryContext context = RepositoryContext.create(config);
        try {
            final RepositoryUpgrade upgrade = new RepositoryUpgrade(context, target);
            upgrade.setIncludes(
                    "/content/foo/en",
                    "/content/assets/foo"
            );
            upgrade.setExcludes(
                    "/content/assets/foo/2013",
                    "/content/assets/foo/2012",
                    "/content/assets/foo/2011",
                    "/content/assets/foo/2010"
            );
            upgrade.copy(null);
        } finally {
            context.getRepository().shutdown();
        }
    }

    @Test
    public void shouldHaveIncludedPaths() throws RepositoryException {
        assertExisting(
                "/content/foo/en",
                "/content/assets/foo/2015/02",
                "/content/assets/foo/2015/01",
                "/content/assets/foo/2014"
        );
    }

    @Test
    public void shouldLackPathsThatWereNotIncluded() throws RepositoryException {
        assertMissing(
                "/content/foo/de",
                "/content/foo/fr",
                "/content/foo/it"
        );
    }

    @Test
    public void shouldLackExcludedPaths() throws RepositoryException {
        assertMissing(
                "/content/assets/foo/2013",
                "/content/assets/foo/2012",
                "/content/assets/foo/2011",
                "/content/assets/foo/2010"
        );
    }
}
