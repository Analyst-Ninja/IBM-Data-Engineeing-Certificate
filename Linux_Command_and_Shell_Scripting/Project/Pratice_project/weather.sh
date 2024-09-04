#! /bin/bash


today=$(date +%Y%m%d)
weather_report=raw_data_$today

city=Casablanca
curl -s wttr.in/$city --output $weather_report

grep °C $weather_report > temperatures.txt

obs_temp=$(head -1 temperatures.txt | tr -s " " | xargs | rev | cut -d " " -f2 | rev)

fcs_temp=$(head -3 temperatures.txt | tail -1 | tr -s " " | xargs | rev | cut -d "C" -f2 | cut -d " " -f2 | rev)

hour=$(TZ='Morocco/Casablanca' date -u +%H) 
day=$(TZ='Morocco/Casablanca' date -u +%d) 
month=$(TZ='Morocco/Casablanca' date +%m)
year=$(TZ='Morocco/Casablanca' date +%Y)

record=$(echo -e "$year | $month | $day | $hour  |  $obs_temp | $fcs_temp")
echo $record>>rx_poc.log

