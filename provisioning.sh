#!/bin/sh
#
# run script to start docker containers for postgres and mongo backends.

help(){
  echo "Usage:"
  echo "   \$ provisioning.sh [-h] -t read_write_loc_on_mac [-lt mnt_on_linux_vm][-m additiona_mount_point] start|stop|restart [mongo|postgres]"
  echo
  echo "Synopsis:"
  echo "    Use this to manage docker containers for various backends used to test blaze eco-system. "
  echo
  echo "start|stop|restart"
  echo "            Perform one of these actions for all backend docker containers. "
  echo "            start|restart require [-t] be given. If this is not provided, then start all containers."
  echo "            This is read/write location primarily for PostgresQL container; it is also used to write "
  echo "            docker_info.txt which provides environment variables that must be set prior to running tests."
  echo
  echo "Available options"
  echo
  echo "-h|--help   Print this usage help"
  echo
  echo "-t|--tmp    Absolute path to read/write location. This must be world r+w. It is required for start|restart "
  echo "            options. Postgres backend needs this for tests that write data to disc. Mongo tests do not "
  echo "            use this; however, the script output a text file with config info when any backend is started. "
  echo "            It is therefore required for all backends."
  echo
  echo "-lt|--linux_tmp"
  echo "            When testing odo on Linux VM; the docker container needs map the TMP_DIR given by "
  echo "            -t flag above to another mount point given by -lt. When running the tests on Linux VM, the "
  echo "            docker containers will get this as the location to write data so docker must also have this "
  echo "            as a mount point. The Linux VM should mount the host's TMP_DIR to this location so both docker "
  echo "            and the VM point to the same location on the host."
  echo
  echo "-m|--mnt"   Additional mount point for docker. Primarily used to find data for loading into DB. For
  echo "            instance, if blaze project is in HOME_DIR, then adding -m ~/blaze/blaze/examples/data allows "
  echo "            the postgres to read data running tests that load data into DB from here. "
  echo
  echo "mongo|postgres"
  echo "            Specifies the docker container to which the action is to be applied. If this is not provided, "
  echo "            the action applies to all backend containers."
  echo
  echo "Examples:"
  echo "stop all containers: "
  echo "   \$ provisioning.sh stop"
  echo
  echo "start/restart requires -t option: "
  echo "   \$ provisioning.sh -t /Users/jsandhu/tmp"
  echo 
  echo "restart mongo: "
  echo "   \$ provisioning.sh -t /Users/jsandhu/tmp restart mongo"
  echo 
  exit
}

# default action applies to all containers
CONTAINER=all
MNT_DIR=()

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

    -m|--mnt)
      # additional read only mount points primarily used for finding data to load into DB - only postgres right now
      MNT_DIR+=("$2");
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

  # postgres is started or restarted
  if [[ "$CONTAINER" == "postgres" || "$CONTAINER" == "all" ]]
  then
    # update postgres command
    POSTGRES_PRE="docker run -d -v $TMP_DIR:$TMP_DIR"
    POSTGRES_POST="-p 5432:5432 -e POSTGRES_DB=test -e POSTGRES_PASSWORD= postgres:9.3"
    if [[ -n "$LINUX_TMP" ]]
    then
      POSTGRES_PRE="$POSTGRES_PRE -v $TMP_DIR:$LINUX_TMP"
    fi
    # add additional mount points
    for m in "${MNT_DIR[@]}"
    do
      POSTGRES_PRE="$POSTGRES_PRE -v $m:$m"
    done
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
  echo "# copy following to define env vars in windows cmd line" >> $INFO_FILE
  echo "set POSTGRES_IP=`docker-machine ip default`" >> $INFO_FILE
  echo "set MONGO_IP=`docker-machine ip default`" >> $INFO_FILE
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
