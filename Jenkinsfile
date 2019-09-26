pipeline {
  agent { label 'm3d' }

  options {
    ansiColor('xterm')
    disableConcurrentBuilds()
    skipDefaultCheckout(true)
    skipStagesAfterUnstable()
    timeout(time: 1, unit: 'HOURS')
    buildDiscarder(logRotator(daysToKeepStr: '32', numToKeepStr: '16'))
  }

  environment {
    GIT_CREDENTIALS = "9654c627-4650-4079-be03-2d0336fe724f"
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