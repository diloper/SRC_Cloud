sys_arg="$USER"
echo $sys_arg
sed -i 's/testforota02/'$sys_arg'/' docker-dokuwiki.service
sudo cp docker-dokuwiki.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable docker-dokuwiki.service

sudo systemctl --all | grep docker-dokuwiki.service

sudo systemctl status docker-dokuwiki.service
