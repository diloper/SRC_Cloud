sys_arg="$USER"
echo $sys_arg
sed -i 's/testforota02/'$sys_arg'/' docker-dokuwiki.service
