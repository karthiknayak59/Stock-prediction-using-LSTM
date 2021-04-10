# MADE BY:
# Ammunje Karthik Nayak, Himanshu Jain, Nicholas Young Chung
#
#read command line inputs
YEAR_FORMAT=$1
MONTH_1=$2
MONTH_2=$3
write_directory=$4
fundamentalDataFile="fundamentaldata"

case `hostname` in
tristram*) ;; # do nothing
*) echo "ERROR: must be run on tristram" >&2; exit 1 ;;
esac
#read fundamental data and create output CSV at write-directory.
python scrapy.py $YEAR_FORMAT $MONTH_1 $write_directory $fundamentalDataFile > /dev/null

#DIRECTORY="${write_directory}/model"
#if [ ! -d "$DIRECTORY" ]; then
#	unzip -d $write_directory model.zip
#fi

outputFilename="mytrades.txt"
#read streaming data to make trades.
python trader_final.py $MONTH_1 $MONTH_2 "${fundamentalDataFile}.csv" $write_directory $outputFilename > /dev/null

cat "${write_directory}/${outputFilename}"
