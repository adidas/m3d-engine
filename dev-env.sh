#!/bin/bash

set -e

SCRIPT_NAME="dev-env.sh"
PROJECT_NAME="m3d-engine"
CONTAINER_IMAGE_NAME="$PROJECT_NAME"

PARAM_WORKSPACE=(    "workspace"   "w" "m3d-engine code directory (must be the same within the container life-cycle)")
PARAM_TEST_FILTER=(  "test-filter" "f" "filter string for selecting specific tests when testing the code with sbt")
OPTION_HELP=(        "help"        "h" "show help message for the command")
OPTION_DEBUG=(       "debug"       "d" "start containers or run tests in debug mode (must be the same within the container life-cycle)")
OPTION_INTERACTIVE=( "interactive" "i" "use interactive mode and allocate pseudo-TTY when executing a command inside the container")

ARG_ACTION_IMAGE_BUILD=(       "image-build"       "build the docker image")
ARG_ACTION_CONTAINER_RUN=(     "container-run"     "run a container from the docker image")
ARG_ACTION_CONTAINER_EXECUTE=( "container-execute" "execute an external command within the container")
ARG_ACTION_CONTAINER_STOP=(    "container-stop"    "stop the container")
ARG_ACTION_CONTAINER_DELETE=(  "container-delete"  "delete the container")
ARG_ACTION_PROJECT_ASSEMBLE=(  "project-assemble"  "build the code and create a jar-file")
ARG_ACTION_PROJECT_TEST=(      "project-test"      "run tests within the container")
ARG_ACTION_PROJECT_CLEAN=(     "project-clean"     "clean pyc-files in the project directory")
ARG_COMMAND=(                  "command"           "command to execute within the container")

AVAILABLE_ACTIONS=(\
    "$ARG_ACTION_IMAGE_BUILD" \
    "$ARG_ACTION_CONTAINER_RUN" \
    "$ARG_ACTION_CONTAINER_EXECUTE" \
    "$ARG_ACTION_CONTAINER_STOP" \
    "$ARG_ACTION_CONTAINER_DELETE" \
    "$ARG_ACTION_PROJECT_ASSEMBLE" \
    "$ARG_ACTION_PROJECT_TEST" \
    "$ARG_ACTION_PROJECT_CLEAN" \
)

source "./common.sh"

###   PROJECT-SPECIFIC FUNCTIONS   ###

function create_actions_help_string() {
  echo "Usage: $SCRIPT_NAME action [-h] [-d] [-w workspace]"
  print_help_lines "Supported actions:" \
    "ARG_ACTION_IMAGE_BUILD" \
    "ARG_ACTION_CONTAINER_RUN" \
    "ARG_ACTION_CONTAINER_EXECUTE" \
    "ARG_ACTION_CONTAINER_STOP" \
    "ARG_ACTION_CONTAINER_DELETE" \
    "ARG_ACTION_PROJECT_ASSEMBLE" \
    "ARG_ACTION_PROJECT_TEST" \
    "ARG_ACTION_PROJECT_CLEAN"
  print_help_lines "Parameters:" "PARAM_WORKSPACE"
  print_help_lines "Options:" "OPTION_DEBUG" "OPTION_HELP"
}

function create_action_default_help_string() {
  local LOCAL_ACTION=$1
  echo "Usage: $SCRIPT_NAME $LOCAL_ACTION [-h] [-d] [-w workspace]"
  print_help_lines "Parameters:" "PARAM_WORKSPACE"
  print_help_lines "Options:" "OPTION_DEBUG" "OPTION_HELP"
}

function create_action_execute_help_string() {
  echo "Usage: $SCRIPT_NAME $ARG_ACTION_CONTAINER_EXECUTE [-h] [-d] [-i] [-w workspace] command"
  print_help_lines "Arguments:" "ARG_COMMAND"
  print_help_lines "Parameters:" "PARAM_WORKSPACE"
  print_help_lines "Options:" "OPTION_INTERACTIVE" "OPTION_DEBUG" "OPTION_HELP"
}

function create_action_assembly_help_string() {
  echo "Usage: $SCRIPT_NAME $ARG_ACTION_PROJECT_ASSEMBLE [-h] [-d] [-i] [-w workspace]"
  print_help_lines "Parameters:" "PARAM_WORKSPACE"
  print_help_lines "Options:" "OPTION_INTERACTIVE" "OPTION_DEBUG" "OPTION_HELP"
}

function create_action_run_tests_help_string() {
  echo "Usage: $SCRIPT_NAME $ARG_ACTION_PROJECT_TEST [-h] [-d] [-i] [-w workspace] [-f test_filter]"
  print_help_lines "Parameters:" "PARAM_WORKSPACE" "PARAM_TEST_FILTER"
  print_help_lines "Options:" "OPTION_INTERACTIVE" "OPTION_DEBUG" "OPTION_HELP"
}

function create_action_build_help_string() {
  echo "Usage: $SCRIPT_NAME build [-h]"
  print_help_lines "Options:" "OPTION_HELP"
}

###   ARGUMENT PARSING   ###

ACTION=$(array_contains "$1" "${AVAILABLE_ACTIONS[@]}")
if [[ -z "$ACTION" ]]; then
  HELP_STRING=$(create_actions_help_string)
  exit_with_messages "ERROR: wrong action specified" "$HELP_STRING"
else
  shift
  if [[ "$ACTION" == "$ARG_ACTION_IMAGE_BUILD" ]]; then
    HELP_STRING=$(create_action_build_help_string)
    ACTION_AVAILABLE_ARGS=("$OPTION_HELP")
  elif [[ "$ACTION" == "$ARG_ACTION_CONTAINER_EXECUTE" ]]; then
    HELP_STRING=$(create_action_execute_help_string)
    ACTION_AVAILABLE_ARGS=("$PARAM_WORKSPACE" "$OPTION_INTERACTIVE" "$OPTION_DEBUG" "$OPTION_HELP")
  elif [[ "$ACTION" == "$ARG_ACTION_PROJECT_ASSEMBLE" ]]; then
    HELP_STRING=$(create_action_assembly_help_string)
    ACTION_AVAILABLE_ARGS=("$PARAM_WORKSPACE" "$OPTION_INTERACTIVE" "$OPTION_DEBUG" "$OPTION_HELP")
  elif [[ "$ACTION" == "$ARG_ACTION_PROJECT_TEST" ]]; then
    HELP_STRING=$(create_action_run_tests_help_string)
    ACTION_AVAILABLE_ARGS=("$PARAM_WORKSPACE" "$PARAM_TEST_FILTER" "$OPTION_DEBUG" "$OPTION_HELP")
  else
    HELP_STRING=$(create_action_default_help_string "$ACTION")
    ACTION_AVAILABLE_ARGS=("$PARAM_WORKSPACE" "$OPTION_DEBUG" "$OPTION_HELP")
  fi
fi

OTHER_ARGS=()
while [[ $# -gt 0 ]]; do
  case $1 in
    -w|--workspace)
      shift
      validate_args_non_empty "$HELP_STRING" "$@"
      WORKSPACE="$1";;
    -f|--test-filter)
      shift
      validate_args_non_empty "$HELP_STRING" "$@"
      validate_possible_values "$HELP_STRING" "$PARAM_TEST_FILTER" "${ACTION_AVAILABLE_ARGS[@]}"
      TEST_FILTER="$1";;
    -i|--interactive)
      validate_possible_values "$HELP_STRING" "$OPTION_INTERACTIVE" "${ACTION_AVAILABLE_ARGS[@]}"
      INTERACTIVE=1;;
    -d|--debug)
      DEBUG=1;;
    -h|--help)
      exit_with_messages "$HELP_STRING";;
    *)
      OTHER_ARGS+=("$1")
  esac
  shift
done

if [[ -z "$WORKSPACE" ]]; then
  WORKSPACE=$(pwd)
fi

if [[ -z "$DEBUG" ]]; then
  CONTAINER_INSTANCE_NAME=$(create_container_instance_name "$CONTAINER_IMAGE_NAME" "$WORKSPACE")
else
  CONTAINER_INSTANCE_NAME=$(create_container_instance_name "${CONTAINER_IMAGE_NAME}-debug" "$WORKSPACE")
fi

###   ACTION HANDLERS   ###

# build the docker image
if [[ "$ACTION" == "$ARG_ACTION_IMAGE_BUILD" ]]; then
  echo "Creating a docker image $CONTAINER_IMAGE_NAME ..."
  validate_args_are_empty "$HELP_STRING" "${OTHER_ARGS[@]}"

  docker build -t "$CONTAINER_IMAGE_NAME" .

# run a container from the docker image
elif [[ "$ACTION" == "$ARG_ACTION_CONTAINER_RUN" ]]; then
  echo "Running the container $CONTAINER_INSTANCE_NAME ..."
  validate_args_are_empty "$HELP_STRING" "${OTHER_ARGS[@]}"

  if [[ -z "$DEBUG" ]]; then
    docker run -t -d --name "$CONTAINER_INSTANCE_NAME" -v "${WORKSPACE}:/root/workspace/${PROJECT_NAME}" "$CONTAINER_IMAGE_NAME"
  else
    echo "Debugging is enabled"
    docker run -t -d --name "$CONTAINER_INSTANCE_NAME" -v "${WORKSPACE}:/root/workspace/${PROJECT_NAME}" -p 5005:5005 "$CONTAINER_IMAGE_NAME"
  fi

# cleanup files generated by SBT
elif [[ "$ACTION" == "$ARG_ACTION_PROJECT_CLEAN" ]]; then
  echo "Deleting files generated by SBT ..."
  validate_args_are_empty "$HELP_STRING" "${OTHER_ARGS[@]}"
  exec_command_within_container "$CONTAINER_INSTANCE_NAME" "$PROJECT_NAME" "rm -rf ./target ./project/target ./project/project ./derby.log"

# execute a command in the container
elif [[ "$ACTION" == "$ARG_ACTION_CONTAINER_EXECUTE" ]]; then
  echo "Executing command ..."
  if [[ "${#OTHER_ARGS[@]}" != 1 ]]; then
    exit_with_messages "ERROR: too many arguments" "$HELP_STRING"
  fi

  EXTERNAL_CMD="${OTHER_ARGS[0]}"
  exec_command_within_container "$CONTAINER_INSTANCE_NAME" "$PROJECT_NAME" "$EXTERNAL_CMD" "$INTERACTIVE"

# build the code and assembly a jar-file
elif [[ "$ACTION" == "$ARG_ACTION_PROJECT_ASSEMBLE" ]]; then
  echo "Creating a jar-file ..."
  validate_args_are_empty "$HELP_STRING" "${OTHER_ARGS[@]}"

  SBT_CMD="sbt clean assembly"
  exec_command_within_container "$CONTAINER_INSTANCE_NAME" "$PROJECT_NAME" "$SBT_CMD" "$INTERACTIVE"

# execute tests in the container
elif [[ "$ACTION" == "$ARG_ACTION_PROJECT_TEST" ]]; then
  echo "Executing tests ..."
  validate_args_are_empty "$HELP_STRING" "${OTHER_ARGS[@]}"

  if [[ -z "$DEBUG" ]]; then
    SBT_OPTS="-Xms512M -Xmx512M"
    SBT_CMD="SBT_OPTS=\"${SBT_OPTS}\" sbt \"test\""
    exec_command_within_container "$CONTAINER_INSTANCE_NAME" "$PROJECT_NAME" "$SBT_CMD" "$INTERACTIVE"
  else
    echo "Debugging is enabled"
    SBT_OPTS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005 -Xms512M -Xmx512M"
    SBT_CMD="SBT_OPTS=\"${SBT_OPTS}\" sbt \";set Test / fork := false; test:testOnly ${TEST_FILTER}\""
    exec_command_within_container "$CONTAINER_INSTANCE_NAME" "$PROJECT_NAME" "$SBT_CMD" 1
  fi

# stop the container
elif [[ "$ACTION" == "$ARG_ACTION_CONTAINER_STOP" ]]; then
  echo "Terminating dev environment ..."
  validate_args_are_empty "$HELP_STRING" "${OTHER_ARGS[@]}"

  echo "Stopping the container ..."
  (docker stop "$CONTAINER_INSTANCE_NAME" 1>/dev/null && echo "The container was stopped")

# delete the container
elif [[ "$ACTION" == "$ARG_ACTION_CONTAINER_DELETE" ]]; then
  echo "Terminating dev environment ..."
  validate_args_are_empty "$HELP_STRING" "${OTHER_ARGS[@]}"

  echo "Deleting the container ..."
  (docker rm "$CONTAINER_INSTANCE_NAME" 1>/dev/null && echo "The container was deleted")

# wrong action is specified
else
  exit_with_messages "ERROR: handler for the \"${ACTION}\" action is not defined"
fi
