#!/bin/sh
#
# run script to start docker containers for postgres and mongo backends.

help(){
  echo " Usage:"
  echo "   \$ provisioning.sh -t read_write_loc_on_mac [-lt mnt_on_linux_vm] start|stop|restart [mongo|posgres]"
  echo "    If ACTION is not given [start|stop|restart], then start all backends"
  echo
  echo "  To stop all containers: "
  echo "   \$ provisioning.sh stop"
  echo
  echo "  start/restart requires -t option: "
  echo "   \$ provisioning.sh -t ~/tmp"
  echo 
  echo "  restart mongo: "
  echo "   \$ provisioning.sh -t ~/tmp restart mongo"
  echo 
  exit
}

# default action applies to all containers
CONTAINER=all

# process command line arguments
while [[ $# > 0 ]]
do
  key=$1
  case $key in
    -h|--help)
      help
      ;;

    -lt|--linux_tmp)
      LINUX_TMP="$2";
      shift
      ;;

    -t|--tmp)
      # TMP_DIR is required if start/restart and postgres.
      # Easiest to use it w/ flag for now
      TMP_DIR="$2"; 
      shift;;

    start|stop|restart)
      ACTION="$1"

      # if next argument is null, then apply action to all backends
      if [[ -n "$2" ]]
      then
        CONTAINER="$2"
        shift
      fi
      ;;
    *) echo "unknown argument .. exiting"; exit;;
  esac
  shift
done


if [[ -z "$ACTION" ]]
then
    ACTION=start
    echo "no ACTION specified, start all backends"
fi


MONGO_CMD="docker run -d -p 27017:27017 mongo"

if [[ "$ACTION" == "start" || "$ACTION" == "restart" ]]
then
  # postgres is started or restarted
  if [[ "$CONTAINER" == "postgres" || -z "$CONTAINER" ]]
  then
    # ensure TMP_DIR is defined
    if [[ -z "$TMP_DIR" ]] 
    then
      echo "$ACTION requires a read/write location via -t"
      echo "optionally you can define mount point for for Linux VM or Windows WM"
      exit
    fi

    # WOULD be nice to exit if TMP_DIR is not world read/write but this is not working
    ##PERM=`stat -f "%OLp" $TMP_DIR`
    #echo "perm: $PERM"
    #if [[ $PERM!="666" && $PERM!="777" ]]
    #then
    #  echo "$TMP_DIR needs world read/write permissions"
    #  exit
    #fi

    # update postgres command
    POSTGRES_PRE="docker run -d -v $TMP_DIR:$TMP_DIR"
    POSTGRES_POST="-p 5432:5432 -e POSTGRES_DB=test -e POSTGRES_PASSWORD= postgres:9.3"
    if [[ -n "$LINUX_TMP" ]]
    then
      POSTGRES_PRE="$POSTGRES_PRE -v $TMP_DIR:$LINUX_TMP"
    fi
    POSTGRES_CMD="$POSTGRES_PRE $POSTGRES_POST"
  fi
fi

# now create backends and commands
case "$CONTAINER" in
  mongo)
    BACKENDS=("mongo")
    DCMD=("$MONGO_CMD")
    ;;
  postgres)
    BACKENDS=("postgres")
    DCMD=("$POSTGRES_CMD")
    ;;
  all)
    BACKENDS=("mongo" "postgres")
    DCMD=("$MONGO_CMD" "$POSTGRES_CMD")
    ;;
esac


# function starts all backends
start () {
  # start backends if they are not running
  DOCKER_PS=`docker ps| awk '{print $2}'`
  bLen=${#BACKENDS[@]}
  for (( i=0; i<${bLen}; i++ ))
  do
      b=${BACKENDS[i]}
      if [[ $DOCKER_PS == *$b* ]]
      then 
          echo "$b already running"
      else
          echo "$b not running"
          DCID=`${DCMD[i]}`
          echo "started $b. Docker CID: $DCID"
      fi
  done
}


# function to stop all backends
stop () {
  L_DOCKER_PS=($(docker ps| awk '{print $2}'))
  L_DOCKER_CID=($(docker ps| awk '{print $1}'))
  dLen=${#L_DOCKER_CID[@]}

  for (( b=0; b<${#BACKENDS[@]}; b++ ))
  do
    # Look for {BACKENDS[b]} in list of docker processes
    for (( i=1; i<${dLen}; i++ ))
    do
      if [[ "${L_DOCKER_PS[i]}" == *"${BACKENDS[b]}"* ]] 
      then
        k_cid=`docker kill ${L_DOCKER_CID[i]}`
        echo "stopped ${BACKENDS[b]} - killed $k_cid"
      fi
    done
  done
}

# write docker config info to file
INFO_FILE=$TMP_DIR/docker_info.txt

info_to_file () {
  # write info about tmp_dir and ip address to file at TMP_DIR
  echo "# For now just copy past following to VM before running tests" > $INFO_FILE
  echo "export POSTGRES_IP=`docker-machine ip default`" >> $INFO_FILE
  echo "export MONGO_IP=`docker-machine ip default`" >> $INFO_FILE
  echo "export POSTGRES_TMP_DIR=$TMP_DIR" >> $INFO_FILE
  echo "export POSTGRES_TMP_DIR=$LINUX_TMP" >> $INFO_FILE

  echo "\nSee $INFO_FILE to set env variables prior to running tests\n"
}


# start/restart backends
case "$ACTION" in
  start)
    start
    info_to_file
    ;;
  stop)
    stop
    ;;
  restart)
    stop
    start
    info_to_file
    ;;
esac
