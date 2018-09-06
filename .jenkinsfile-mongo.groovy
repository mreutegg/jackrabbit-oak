/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

properties([parameters([
    string(name: 'GIT_REPOSITORY', defaultValue: 'https://github.com/apache/jackrabbit-oak.git/', description: '', trim: true), 
    string(name: 'GIT_BRANCH', defaultValue: 'trunk', description: '', trim: true),
    string(name: 'OAK_MODULES', defaultValue: 'oak-jcr:ut,oak-jcr:it,oak-store-document,oak-lucene,oak-it,oak-run,oak-upgrade,oak-pojosr,oak-it-osgi', description: '', trim: true),
    string(name: 'MONGO_VERSION', defaultValue: '3.6', description: '', trim: true),
    string(name: 'JENKINS_NODE_LABEL', defaultValue: 'ubuntu', description: '', trim: true),
    string(name: 'MAVEN_VERSION_NAME', defaultValue: 'Maven 3 (latest)', description: '', trim: true)
]), buildDiscarder(logRotator(artifactDaysToKeepStr: '', artifactNumToKeepStr: '', daysToKeepStr: '', numToKeepStr: '20'))])

def getLocalRepositoryPath() {
    withMaven(maven: "${params.MAVEN_VERSION_NAME}") {
        return sh(script: "mvn help:evaluate -Dexpression=settings.localRepository | grep -E '^([a-zA-Z]:|/)'", returnStdout: true).trim()
    }
}

def buildModuleInsideContainer(moduleName, testOptions) {
    // checkout from git repo
    git url: "${params.GIT_REPOSITORY}", branch: "${params.GIT_BRANCH}"
    def mvnCmd = "mvn --batch-mode -Dmaven.repo.local=/var/maven/repository -s /var/maven/settings/settings.xml"
    // build modules required by this module
    sh "${mvnCmd} -Dbaseline.skip=true -T 1C clean install -DskipTests -pl :${moduleName} -am"
    try {
        // run tests
        sh "${mvnCmd} ${testOptions} -Dnsfixtures=DOCUMENT_NS -Dmongo.host=db verify -pl :${moduleName}"
    } finally {
        archiveArtifacts '*/target/unit-tests.log'
        junit '*/target/surefire-reports/*.xml,*/target/failsafe-reports/*.xml'
    }
}

def testOptionsFrom(moduleSpec) {
    def testOptions = '-PintegrationTesting'
    def idx = moduleSpec.indexOf(':')
    if (idx != -1) {
        flags = moduleSpec.substring(idx + 1)
        if (flags == 'ut') {
            // unit tests only
            testOptions = ''
        } else if (flags == 'it') {
            // integration tests only
            testOptions = '-PintegrationTesting -Dsurefire.skip.ut=true'
        }
    }
    return testOptions
}

static def moduleNameFrom(moduleSpec) {
    def idx = moduleSpec.indexOf(':')
    if (idx == -1) {
        return moduleSpec
    } else {
        return moduleSpec.substring(0, idx)
    }
}

def buildModule(moduleSpec) {
    def moduleName = moduleNameFrom(moduleSpec)
    def testOptions = testOptionsFrom(moduleSpec)
    stage('build ' + moduleSpec) {
        node(label: "${JENKINS_NODE_LABEL}") {
            timeout(60) {
                docker.image("mongo:${MONGO_VERSION}").withRun() { c ->
                    def localMavenRepoPath = getLocalRepositoryPath()
                    docker.image('maven:3.5-jdk-8').inside("--link ${c.id}:db -v ${localMavenRepoPath}:/var/maven/repository -v $HOME/.m2:/var/maven/settings:ro") {
                        buildModuleInsideContainer(moduleName, testOptions)
                    }
                }
            }
        }
    }
}

def stagesFor(modules) {
    def stages = [:]
    for (m in modules.tokenize(',')) {
        def module = m.trim()
        stages[module] = { buildModule(module) }
    }
    return stages
}

parallel stagesFor("${params.OAK_MODULES}")
