#!/bin/bash

##################################################################
##################################################################
# CONSTANTS

declare -r CONST_DIR_LOG=${SYS_LOG}/

declare -ir CONST_LOG_LEVEL_TRACE=0
declare -ir CONST_LOG_LEVEL_DEBUG=1
declare -ir CONST_LOG_LEVEL_INFO=2
declare -ir CONST_LOG_LEVEL_WARN=3
declare -ir CONST_LOG_LEVEL_ERROR=4
declare -ir CONST_LOG_LEVEL_FATAL=5
declare -ir CONST_LOG_LEVEL_NONE=999

declare -r CONST_COLOUR_BLACK=$(tput setaf 0)
declare -r CONST_COLOUR_RED=$(tput setaf 1)
declare -r CONST_COLOUR_GREEN=$(tput setaf 2)
declare -r CONST_COLOUR_YELLOW=$(tput setaf 3)
declare -r CONST_COLOUR_LIME_YELLOW=$(tput setaf 190)
declare -r CONST_COLOUR_POWDER_BLUE=$(tput setaf 153)
declare -r CONST_COLOUR_BLUE=$(tput setaf 4)
declare -r CONST_COLOUR_MAGENTA=$(tput setaf 5)
declare -r CONST_COLOUR_CYAN=$(tput setaf 6)
declare -r CONST_COLOUR_WHITE=$(tput setaf 7)
declare -r CONST_COLOUR_BRIGHT=$(tput bold)
declare -r CONST_COLOUR_NORMAL=$(tput sgr0)
declare -r CONST_COLOUR_BLINK=$(tput blink)
declare -r CONST_COLOUR_REVERSE=$(tput smso)
declare -r CONST_COLOUR_UNDERLINE=$(tput smul)

#----------------------------------------------------------------------------------------------------
# FUNCTIONS

# Write log with a common header
function log_write() {
	local log_level=$1
	local log_message=$2
	local log_level_string="NONE"

	case $log_level in
	0)
		log_level_string="${CONST_COLOUR_CYAN}TRACE${CONST_COLOUR_NORMAL}"
		;;
	1)
		log_level_string="${CONST_COLOUR_BLUE}DEBUG${CONST_COLOUR_NORMAL}"
		;;
	2)
		log_level_string="${CONST_COLOUR_GREEN}INFO${CONST_COLOUR_NORMAL}"
		;;
	3)
		log_level_string="${CONST_COLOUR_BLINK}${CONST_COLOUR_YELLOW}WARN${CONST_COLOUR_NORMAL}"
		log_message="${CONST_COLOUR_YELLOW}${log_message}${CONST_COLOUR_NORMAL}"
		;;
	4)
		log_level_string="${CONST_COLOUR_BLINK}${CONST_COLOUR_RED}ERROR${CONST_COLOUR_NORMAL}"
		log_message="${CONST_COLOUR_RED}${log_message}${CONST_COLOUR_NORMAL}"
		;;
	5)
		log_level_string="${CONST_COLOUR_BLINK}${CONST_COLOUR_MAGENTA}FATAL${CONST_COLOUR_NORMAL}"
		log_message="${CONST_COLOUR_MAGENTA}${log_message}${CONST_COLOUR_NORMAL}"
		;;
	*)
		log_level_string="${CONST_COLOUR_WHITE}NONE${CONST_COLOUR_NORMAL}"
		;;
	esac

	printf "[%s] [%s] [%s] %s\n" \
		"$log_level_string" \
		"${CONST_COLOUR_WHITE}$(date "+%Y-%m-%d %T")${CONST_COLOUR_NORMAL}" \
		"${CONST_COLOUR_YELLOW}${LOG_NAME}${CONST_COLOUR_NORMAL}" \
		"$log_message"
}

#----------------------------------------------------------------------------------------------------
# Initialise logging
function log_init() {

	local v_job_name=$(basename $0 '.sh')
	local v_now=$(date +"%Y%m%d%H%M%S")

	# Set arg
	if [ -z $1 ]; then
		printf "[$0] No argument supplied for logging level [trace|debug|info|warn|error|fatal]. Setting default level to info\n"
		local p_log_level="info"
	else
		local p_log_level=$1
	fi

	# Set arg
	if [ -z $2 ]; then
		printf "[$0] No argument supplied for log file suffix. Setting default suffix to empty string\n"
		local p_log_file_suffix=""
	else
		local p_log_file_suffix="_$2"
	fi

	case $p_log_level in

	"trace")
		v_logging_level_val=$CONST_LOG_LEVEL_TRACE
		;;

	"debug")
		v_logging_level_val=$CONST_LOG_LEVEL_DEBUG
		;;

	"info")
		v_logging_level_val=$CONST_LOG_LEVEL_INFO
		;;

	"warn")
		v_logging_level_val=$CONST_LOG_LEVEL_WARN
		;;

	"error")
		v_logging_level_val=$CONST_LOG_LEVEL_ERROR
		;;

	"fatal")
		v_logging_level_val=$CONST_LOG_LEVEL_FATAL
		;;

	*)
		#v_logging_level_val=$CONST_LOG_LEVEL_NONE
		v_logging_level_val=$CONST_LOG_LEVEL_DEBUG
		;;

	esac

    if [[ -z "$(logname 2> /dev/null)" ]]; then
        export LOG_NAME=$(whoami)
    else
        export LOG_NAME=$(logname)
    fi

    declare -r -g CONST_LOG_LEVEL_DEFAULT=$v_logging_level_val


	if [[ ! -d $CONST_DIR_LOG ]]; then
		mkdir -p $CONST_DIR_LOG
	fi

    if [[ -z "${LOGFILE}" ]]; then
        declare -g LOGFILE="${CONST_DIR_LOG}/${v_job_name}_${v_now}.log"
    fi

    if [[ -e ${LOGFILE} ]]; then
        BACKUP_LOG="$(echo ${LOGFILE}| cut -d"." -f-1)$(date +"%H%M%S").failed.log"
        mv ${LOGFILE} ${BACKUP_LOG}
        printf "RENAMING EXISTING ${LOGFILE} to ${BACKUP_LOG}\n" | tee -a ${LOGFILE}
    fi

	exec >>>(tee -a >(sed -r 's/\x1B\[([0-9]{1,2}(;[0-9]{1,2})?)?[m|K]//g'  >> ${LOGFILE}))

	log_default "[$0] Log file location [${LOGFILE}]"
	log_default "[$0] Logging level set to [$CONST_LOG_LEVEL_DEFAULT]"
}

#----------------------------------------------------------------------------------------------------
# Log TRACE
function log_trace() {
	if ((CONST_LOG_LEVEL_DEFAULT <= CONST_LOG_LEVEL_TRACE)); then
		log_write $CONST_LOG_LEVEL_TRACE "$1"
	fi
}

#----------------------------------------------------------------------------------------------------
# Log DEBUG
function log_debug() {
	if ((CONST_LOG_LEVEL_DEFAULT <= CONST_LOG_LEVEL_DEBUG)); then
		log_write $CONST_LOG_LEVEL_DEBUG "$1"
	fi
}

#----------------------------------------------------------------------------------------------------
# Log INFO
function log_info() {
	if ((CONST_LOG_LEVEL_DEFAULT <= CONST_LOG_LEVEL_INFO)); then
		log_write $CONST_LOG_LEVEL_INFO "$1"
	fi
}

#----------------------------------------------------------------------------------------------------
# Log WARN
function log_warn() {
	if ((CONST_LOG_LEVEL_DEFAULT <= CONST_LOG_LEVEL_WARN)); then
		log_write $CONST_LOG_LEVEL_WARN "$1"
	fi
}

#----------------------------------------------------------------------------------------------------
# Log ERROR
function log_error() {
	if ((CONST_LOG_LEVEL_DEFAULT <= CONST_LOG_LEVEL_ERROR)); then
		log_write $CONST_LOG_LEVEL_ERROR "$1"
	fi
}

#----------------------------------------------------------------------------------------------------
# Log FATAL
function log_fatal() {
	if ((CONST_LOG_LEVEL_DEFAULT <= CONST_LOG_LEVEL_FATAL)); then
		log_write $CONST_LOG_LEVEL_FATAL "$1"
	fi
}

#----------------------------------------------------------------------------------------------------
# Log NONE
function log_default() {
	log_write $CONST_LOG_LEVEL_NONE "$1"
}

#----------------------------------------------------------------------------------------------------
# Main
if [[ -z $CONST_LOG_LEVEL_DEFAULT ]]; then
	export CONST_LOG_LEVEL_DEFAULT=$CONST_LOG_LEVEL_INFO
fi
