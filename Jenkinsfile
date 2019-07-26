pipeline {
    agent none
    tools {
        jdk 'jdk11'
    }
    stages {
        stage("Review") {
            when {
                expression { env.CHANGE_ID != null }
            }
            parallel {
                stage("StaticAnalysis") {
                    steps {
                        node("slave-sbt") {
                            checkout scm
                            sh 'sbt clean scalafmtCheck test:scalafmtCheck scalafmtSbtCheck scapegoat'
                        }
                    }
                }
                stage("Tests/Coverage") {
                    steps {
                        node("slave-sbt") {
                            checkout scm
                            sh "sbt clean coverage test coverageReport coverageAggregate"
                            sh "curl -s https://codecov.io/bash >> ./coverage.sh"
                            sh "bash ./coverage.sh -t `oc get secrets codecov-secret --template='{{.data.nexus_service}}' | base64 -d`"
                        }
                    }
                }
            }
        }
        stage("Release") {
            when {
                expression { env.CHANGE_ID == null }
            }
            steps {
                node("slave-sbt") {
                    checkout scm
                    sh 'sbt releaseEarly'
                }
            }
        }
        stage("Report Coverage") {
            when {
                expression { env.CHANGE_ID == null }
            }
            steps {
                node("slave-sbt") {
                    checkout scm
                    sh "sbt clean coverage test coverageReport coverageAggregate"
                    sh "curl -s https://codecov.io/bash >> ./coverage.sh"
                    sh "bash ./coverage.sh -t `oc get secrets codecov-secret --template='{{.data.nexus_service}}' | base64 -d`"
                }
            }
        }
    }
}
