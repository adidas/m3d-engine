pipeline {

  options {
    ansiColor('xterm')
    disableConcurrentBuilds()
    skipDefaultCheckout(true)
    skipStagesAfterUnstable()
    timeout(time: 1, unit: 'HOURS')
    buildDiscarder(logRotator(daysToKeepStr: '32', numToKeepStr: '16'))
  }

  stages {
    stage('cleanup workspace') {
      steps {
        cleanWs()
      }
    }

    stage('checkout repositories') {
      steps {
        retry(3) {
          checkout scm
        }
      }
    }

    stage('build container image') {
      steps {
        sh "./dev-env.sh image-build"
      }
    }

    stage('run container') {
      steps {
        sh "./dev-env.sh container-run -w ${workspace}"
      }
    }

    stage('lint code') {
          steps {
            sh "./dev-env.sh project-lint -w ${workspace}"
          }
        }

    stage('run tests') {
      steps {
        sh "./dev-env.sh project-test -w ${workspace}"
      }
    }
  }

  post {
    always {
      sh "./dev-env.sh container-stop -w ${workspace}"
      sh "./dev-env.sh container-delete -w ${workspace}"
      cleanWs()
    }

    unstable {
      emailextrecipients([
              [$class: 'CulpritsRecipientProvider'],
              [$class: 'UpstreamComitterRecipientProvider'],
              [$class: 'DevelopersRecipientProvider'],
              [$class: 'RequesterRecipientProvider'],
              [$class: 'FailingTestSuspectsRecipientProvider'],
              [$class: 'FirstFailingBuildSuspectsRecipientProvider']
      ])
    }

    failure {
      emailextrecipients([
              [$class: 'CulpritsRecipientProvider'],
              [$class: 'UpstreamComitterRecipientProvider'],
              [$class: 'DevelopersRecipientProvider'],
              [$class: 'RequesterRecipientProvider'],
              [$class: 'FailingTestSuspectsRecipientProvider'],
              [$class: 'FirstFailingBuildSuspectsRecipientProvider']
      ])
    }
  }
}