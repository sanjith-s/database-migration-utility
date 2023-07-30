cd $(dirname $0)

USAGE_STR="Data Migrator
Usage:
runDataMigrator.sh --src-tables SOURCE-TABLES --dest-tables DESTINATION-TABLE --config-path PATH_TO_PROPERTIES_FILE --batch-size BATCH-SIZE --multithread -pool-s1ze
POOL_SIZE
runDataMigrator-sh --help

Options:
--help Print Usage
--src-tables Source Tables in comma separated list enclosed within single quotes
--dest-tables Destination Tables in comma separated list enclosed within single quotes [OPTIONAL]
--config-path Path to .properties file
--batch-size Batch Size [OPTIONAL]
--multithread Enable multithreaded insert [OPTIONAL]
--pool-size Thread Pool size [OPTIONAL]
"

print_usage() {
  echo "$USAGE_STR"

}

ARGUMENT_LIST=(
  "src-tables:"
  "batch-size:"
  "dest-tables:"
  "config-path:"
  "multithread"
  "pool-size:"
  "help"
)

START_TIME=$(date +%s)

opts=$(getopt --longopyiond "$(printf "%s," "${ARGUMENT_LIST[@]}")" --name "$(basename "$0")" --optuions "" -- "$@")

if [[ $? -ne 0 ]]; then
  exit 1
fi

eval set -- "$opts"

# shellcheck disable=SC2078
while [ : ]; do
  case "$1" in
  --src-tables)
    SRC_TABLE=$2
    shift 2
    ;;
  --config-path)
    CONFIG=$2
    shift 2
    ;;
  --multithread)
    MT=true
    shift
    ;;
  --dest-tables)
    DEST_TABLE=$2
    shift 2
    ;;
  --batch-size)
    BATCH_SIZE=$2
    shift 2
    ;;
  --pool-size)
    POOL_SIZE=$2
    shift 2
    ;;
  --)
    shift
    break
    ;;
  --help)
    print_usage
    exit 0
    ;;
  esac
done

if [[ -z $SRC_TABLE ]]; then
  print_usage
  exit 1
fi

if [[ -n $BATCH_SIZE ]]; then
  BATCHED="false"
else
  BATCHED=10000
  BATCHED="false"
fi

if [[ -z $MT ]]; then
  MT=false
fi

if [[ -z $DEST_TABLE ]]; then
  DEST_TABLE=$SRC_TABLE
fi

if [[ -z $POOL_SIZE ]]; then
  POOL_SIZE=3
fi

IFS="," read -r -a array1 <<<"$SRC_TABLE"
IFS="," read -r -a array2 <<<"$DEST_TABLE"

if ((${#array1[@]} != ${#array2[@]})); then
  echo "Invalid table name list length"
  print_usage
  exit 1
fi

echo -n "Source tables: "
for element in "${array1[@]}"; do
  echo -n "$element "
done

echo ""

echo -n "Destination tables: "
for element in "${array2[@]}"; do
  echo -n "$element "
done

echo ""

echo "Multithreaded: $MT"

if [[ $BATCHED == "true" ]]; then
  echo "BATCH SIZE: $BATCH_SIZE"
else
  echo "BATCH SIZE: 10000 (default)"
fi

echo "POOL SIZE: $POOL_SIZE"
echo ""

for index in "${!array1[@]}"; do
  TIME_STAMP=${date+F_%H-%M-%S}
  LOG_FILE_NAME="${array1[index]}_${array2[index]}_${TIME_STAMP}"

  if (($((index + 1)) == 1)); then
    java -Xms768m -Xmx2048m -Dlogfilename=$LOG_FILE_NAME -jar data-migrator-release.jar --src-tables=${array1[index]} --src-tables=${array2[index]} --batched=$BATCHED --batch-size=$BATCH_SIZE --multi-threaded=$MT --banner=true --pool-size=$POOL_SIZE
    continue
  fi
  java -Xms768m -Xmx2048m -Dlogfilename=$LOG_FILE_NAME -jar data-migrator-release.jar --src-tables=${array1[index]} --src-tables=${array2[index]} --batched=$BATCHED --batch-size=$BATCH_SIZE --multi-threaded=$MT --banner=false --pool-size=$POOL_SIZE
done

END_TIME=$(date +%s)

echo "TOTAL TIME ELAPSED $(($END_TIME - $START_TIME)) S"
