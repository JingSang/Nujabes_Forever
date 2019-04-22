#!/bin/bash
#从sentry用户中回收权限
#要4个参数分别为 kerberos认证用户名 sentry授权用户名 topic名 权限列表--一个字符串
#例如./grant_acl_to_role.sh songlei kafka_role sentry-test "read write create"



has_user=0

input_name=$1
role_name=$2
topic=$3
acl_list=$(echo $4|cut -f 1- -d " ")

user_names=$(cat /etc/passwd|cut -f 1 -d ":"|grep $input_name)

if [ $# -ge 4 ]
then
	echo '参数正常'
	for user_name in $user_names
	do
		if [ $user_name = $input_name ]
		then
			has_user=1
			echo "有该用户"
			break
		fi
			echo "没有该用户"

	done
	
	if [ $has_user -eq 0 ]
	then
		exit
	fi

	echo '走到这里了'
	echo $role_name
	echo $topic
	echo $acl_list
	
	for acl in $acl_list
	do 
		echo "回收权限$acl"
		sudo -u kafka kafka-sentry  -rpr -r $role_name -p "Topic=$topic->action=$acl"
	done
	
	
else
	echo '参数太少了'
fi
