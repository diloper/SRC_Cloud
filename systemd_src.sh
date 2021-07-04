#!/bin/bash
search_dir='/home/testforta01/SRC_D'
#search_dir=$(eval echo ~$USER)'/SRC_D'

#search_dir=$(pwd)'/SRC_D'
search_dir=$(pwd)
echo $search_dir
for entry in "$search_dir"/*;
do
	#basename= "$entry"
	A=${entry##*/}
	#echo $A
	if [[ $A == "tmp"* ]];
	then
		sudo rm $entry
		echo $A	
	fi
done

#delect pyc file
find . -name "*tmp*.py" -exec rm -f {} \;
find . -name "*.pyc" -exec rm -f {} \;
echo '####################################################'
echo 'Stopping running containers (if available)...'
echo '####################################################'
docker container stop $(docker ps -aq)
#docker container rm $(docker ps -aq)
docker run  --rm --mount type=bind,source="$search_dir",target=/tmp --name SRC_Server diloper/tensorflow_evn:latest python demo.py
sys_arg=$(hostname)
#docker run  -d --rm --mount type=bind,source="$search_dir",target=/tmp --name SRC_Server diloper/tensorflow_evn:latest python demo.py $sys_arg
#sudo shutdown -h now
#while [[ -d /proc/$(pgrep python) ]]; do sleep 1; done; poweroff
counter=0
#while [[ -d /proc/$(pgrep python) ]]; 

while true; 
do sleep 20;
A=$(sudo docker ps | grep 'python demo.py')
#echo $A
if [ -z "$A" ];
then
	echo "empty";	
	counter=$((counter+1))
	echo "$counter"
	if [ $counter -gt 3 ];
	then
		echo "$counter"
		sudo shutdown -h now	
	fi

	
else
	echo $A;	
fi	
done; 
