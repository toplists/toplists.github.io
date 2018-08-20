#!/bin/bash

cd "$( dirname "${BASH_SOURCE[0]}" )"

now=`date -u +%Y-%m-%d_%H%M_UTC`
echo $now
wget -nv --no-check-certificate -t 20 -O public/archive/alexa/alexa-top1m-$now.csv.zip http://s3.amazonaws.com/alexa-static/top-1m.csv.zip &
wget -nv --no-check-certificate -t 20 -O public/archive/majestic/majestic_million_$now.csv http://downloads.majestic.com/majestic_million.csv &
wget -nv --no-check-certificate -t 20 -O public/archive/umbrella/cisco-umbrella-top1m-$now.csv.zip http://s3-us-west-1.amazonaws.com/umbrella-static/top-1m.csv.zip
wget -nv --no-check-certificate -t 20 -O public/archive/quantcast/quantcast-top-sites-$now.txt.zip https://ak.quantcast.com/quantcast-top-sites.zip &
# wget -nv --no-check-certificate -t 20 -O public/statvoo/statvoo-top1m-$now.csv.zip https://siteinfo.statvoo.com/dl/top-1million-sites.csv.zip &
wait 
xz public/archive/majestic/majestic_million_$now.csv
# sha512sum <(cat find public/majestic/majestic_million/*csv | sort | tail -n 1) public/majestic/majestic_million_$now.csv 

# we can reduce storage from 11M to about 6M per list using:
function compress {
	# $1 original zip file
	DIR=$(dirname $1)
	BN=$(basename -s .zip $1)
	XZ=$DIR/$BN.xz
	unzip -p $1 | xz > $XZ
	diff <(unzip -p $1) <(xzcat $XZ)
	if [[ $? != 0 ]]
	then
		echo "Files differ: $1 $XZ"
	else
		rm $1
	fi
}
compress public/archive/alexa/alexa-top1m-$now.csv.zip
compress public/archive/umbrella/cisco-umbrella-top1m-$now.csv.zip
compress public/archive/quantcast/quantcast-top-sites-$now.txt.zip
#unzip -p public/archive/alexa/alexa-top1m-$now.csv.zip |xz > public/archive/alexa/alexa-top1m-$now.csv.xz
#unzip -p public/archive/umbrella/cisco-umbrella-top1m-$now.csv.zip | xz  > public/archive/umbrella/cisco-umbrella-top1m-$now.csv.xz
#unzip -p public/archive/quantcast/quantcast-top-sites-$now.txt.zip | xz > public/archive/quantcast/quantcast-top-sites-$now.txt.xz
./toplists_correlate.py
./toplists_daytoday.py
/srv/website/update_website.sh
