#!/usr/bin/env groovy
/**
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

def OAK_MODULES = 'oak-pojosr,oak-it-osgi'

properties([buildDiscarder(logRotator(artifactDaysToKeepStr: '', artifactNumToKeepStr: '', daysToKeepStr: '', numToKeepStr: '20'))])

def buildModule(moduleSpec) {
    def moduleName = ''
    def testOptions = '-PintegrationTesting'
    def idx = moduleSpec.indexOf(':') 
    if (idx == -1) {
        moduleName = moduleSpec
    } else {
        moduleName = moduleSpec.substring(0, idx)
        flags = moduleSpec.substring(idx + 1)
        if (flags == 'ut') {
            // unit tests only
            testOptions = '' 
        } else if (flags == 'it') {
            // integration tests only
            testOptions = '-PintegrationTesting -Dsurefire.skip.ut=true' 
        }
    }
    stage('build ' + moduleSpec) {
        node(label: 'ubuntu') {
            def JAVA_JDK_11=tool name: 'jdk_11_latest', type: 'hudson.model.JDK'
            def MAVEN_3_LATEST=tool name: 'maven_3_latest', type: 'hudson.tasks.Maven$MavenInstallation'
            timeout(60) {
                checkout scm
                withEnv(["Path+JDK=$JAVA_JDK_11/bin","Path+MAVEN=$MAVEN_3_LATEST/bin","JAVA_HOME=$JAVA_JDK_11"]) {
                    sh "mvn --batch-mode -Dbaseline.skip=true -T 1C clean install -DskipTests -pl :${moduleName} -am"
                    try {
                        sh "mvn --batch-mode ${testOptions} -DtrimStackTrace=false -Dnsfixtures=SEGMENT_TAR,DOCUMENT_NS verify -pl :${moduleName}"
                    } finally {
                        archiveArtifacts(artifacts: '*/target/unit-tests.log', allowEmptyArchive: true)
                        junit '*/target/surefire-reports/*.xml,*/target/failsafe-reports/*.xml'
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

parallel stagesFor("${OAK_MODULES}")
